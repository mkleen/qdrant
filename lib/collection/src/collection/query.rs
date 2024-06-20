use std::sync::Arc;
use std::time::Duration;

use futures::{future, TryFutureExt};
use itertools::{Either, Itertools};
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::types::{Order, ScoredPoint};
use segment::utils::scored_point_ties::ScoredPointTies;
use tokio::time::Instant;

use super::Collection;
use crate::common::batching::batch_requests;
use crate::common::fetch_vectors::resolve_referenced_vectors_batch;
use crate::common::transpose_iterator::transposed_iter;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::universal_query::collection_query::CollectionQueryRequest;
use crate::operations::universal_query::shard_query::{
    Fusion, ScoringQuery, ShardQueryRequest, ShardQueryResponse,
};

impl Collection {
    /// Returns a shape of [shard_id, batch_id, intermediate_response, points]
    async fn batch_query_shards_concurrently(
        &self,
        batch_request: Arc<Vec<ShardQueryRequest>>,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<Vec<(Vec<ScoredPoint>, Order)>>>> {
        // query all shards concurrently
        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(shard_selection)?;
        let all_searches = target_shards.iter().map(|(shard, shard_key)| {
            let shard_key = shard_key.cloned();
            shard
                .query_batch(
                    Arc::clone(&batch_request),
                    read_consistency,
                    shard_selection.is_shard_id(),
                    timeout,
                )
                .and_then(move |mut shard_responses| async move {
                    if shard_key.is_none() {
                        return Ok(shard_responses);
                    }
                    shard_responses
                        .iter_mut()
                        .flatten()
                        .flat_map(|(points, _)| points)
                        .for_each(|point| point.shard_key.clone_from(&shard_key));

                    Ok(shard_responses)
                })
        });
        future::try_join_all(all_searches).await
    }

    /// This function is used to query the collection. It will return a list of scored points.
    async fn do_query_batch(
        &self,
        requests_batch: Vec<ShardQueryRequest>,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let instant = Instant::now();

        let requests_batch = Arc::new(requests_batch);

        let all_shards_results = self
            .batch_query_shards_concurrently(
                requests_batch.clone(),
                read_consistency,
                &shard_selection,
                timeout,
            )
            .await?;

        let results_f = transposed_iter(all_shards_results)
            .zip(requests_batch.iter())
            .map(|(shards_results, request)| async {
                // shards_results shape: [num_shards, num_intermediate_results, num_points]
                let mut merged_intermediates = self
                    .merge_intermediate_results_from_shards(request, shards_results)
                    .await?;

                let result = if let Some(ScoringQuery::Fusion(fusion)) = &request.query {
                    // If the root query is a Fusion, the returned results correspond to each the prefetches.
                    match fusion {
                        Fusion::Rrf => rrf_scoring(merged_intermediates),
                    }
                } else {
                    // Otherwise, it will be a list with a single list of scored points.
                    debug_assert_eq!(merged_intermediates.len(), 1);
                    merged_intermediates.pop().ok_or_else(|| {
                        CollectionError::service_error(
                            "Query response was expected to have one list of results.",
                        )
                    })?
                };

                let result: Vec<ScoredPoint> = result
                    .into_iter()
                    .skip(request.offset)
                    .take(request.limit)
                    .collect();

                let filter_refs = request.filter_refs();
                self.post_process_if_slow_request(instant.elapsed(), filter_refs);

                Ok::<_, CollectionError>(result)
            });
        let results = future::try_join_all(results_f).await?;

        Ok(results)
    }

    /// To be called on the user-responding instance. Resolves ids into vectors, and merges the results from local and remote shards.
    ///
    /// This function is used to query the collection. It will return a list of scored points.
    pub async fn query_batch(
        &self,
        requests_batch: Vec<(CollectionQueryRequest, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // Turn ids into vectors, if necessary
        let ids_to_vectors = resolve_referenced_vectors_batch(
            &requests_batch,
            self,
            |_| async { unimplemented!("lookup_from is not implemented yet") },
            read_consistency,
        )
        .await?;

        let futures = batch_requests::<
            (CollectionQueryRequest, ShardSelectorInternal),
            ShardSelectorInternal,
            Vec<ShardQueryRequest>,
            Vec<_>,
        >(
            requests_batch,
            |(_req, shard)| shard,
            |(req, _), acc| {
                req.try_into_shard_request(&ids_to_vectors)
                    .map(|shard_req| {
                        acc.push(shard_req);
                    })
            },
            |shard_selection, shard_requests, futures| {
                if shard_requests.is_empty() {
                    return Ok(());
                }

                futures.push(self.do_query_batch(
                    shard_requests,
                    read_consistency,
                    shard_selection,
                    timeout,
                ));

                Ok(())
            },
        )?;

        let results = future::try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok(results)
    }

    /// To be called on the remote instance. Only used for the internal service.
    ///
    /// If the root query is a Fusion, the returned results correspond to each the prefetches.
    /// Otherwise, it will be a list with a single list of scored points.
    pub async fn query_batch_internal(
        &self,
        requests: Vec<ShardQueryRequest>,
        shard_selection: &ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let requests_arc = Arc::new(requests);

        // Results from all shards
        // Shape: [num_shards, batch_size, num_intermediate_results, num_points]
        let all_shards_results = self
            .batch_query_shards_concurrently(
                Arc::clone(&requests_arc),
                None,
                shard_selection,
                timeout,
            )
            .await?;

        let merged_f = transposed_iter(all_shards_results)
            .zip(requests_arc.iter())
            .map(|(shards_results, request)| async {
                // shards_results shape: [num_shards, num_intermediate_results, num_points]
                self.merge_intermediate_results_from_shards(request, shards_results)
                    .await
            });
        let merged = futures::future::try_join_all(merged_f).await?;
        Ok(merged)
    }

    /// Merges the results in each shard for each intermediate query.
    /// ```text
    /// [ [shard1_result1, shard1_result2],
    ///          ↓               ↓
    ///   [shard2_result1, shard2_result2] ]
    ///
    /// = [merged_result1, merged_result2]
    /// ```
    async fn merge_intermediate_results_from_shards(
        &self,
        request: &ShardQueryRequest,
        all_shards_results: Vec<Vec<(Vec<ScoredPoint>, Order)>>,
    ) -> CollectionResult<ShardQueryResponse> {
        let intermediate_limits =
            request.intermediate_response_info(|r| r.offset + r.limit, |p| p.limit);

        let results_len = intermediate_limits.len();
        let mut results = ShardQueryResponse::with_capacity(results_len);
        debug_assert!(all_shards_results
            .iter()
            .all(|shard_results| shard_results.len() == results_len));

        // Shape: [num_internal_queries, num_shards, num_scored_points]
        let all_shards_result_by_transposed = transposed_iter(all_shards_results);

        for (shards_results, take) in all_shards_result_by_transposed.zip(intermediate_limits) {
            // `shards_results` shape: [num_shards, num_scored_points]

            debug_assert!(shards_results.iter().map(|(_, order)| order).all_equal());
            let order = shards_results
                .first()
                .map(|(_, order)| *order)
                .unwrap_or(Order::SmallBetter);

            let shards_results = shards_results
                .into_iter()
                .map(|(scored_points, _)| scored_points);

            // Equivalent to:
            //
            // shards_results
            //     .into_iter()
            //     .kmerge_by(match order {
            //         Order::LargeBetter => |a, b| ScoredPointTies(a) > ScoredPointTies(b),
            //         Order::SmallBetter => |a, b| ScoredPointTies(a) < ScoredPointTies(b),
            //     })
            //
            // if the `kmerge_by` function were able to work with reference predicates.
            // Either::Left and Either::Right are used to allow type inference to work.
            //
            let intermediate_result = match order {
                Order::LargeBetter => Either::Left(
                    shards_results.kmerge_by(|a, b| ScoredPointTies(a) > ScoredPointTies(b)),
                ),
                Order::SmallBetter => Either::Right(
                    shards_results.kmerge_by(|a, b| ScoredPointTies(a) < ScoredPointTies(b)),
                ),
            }
            .unique_by(|point| point.id)
            .take(take)
            .collect();

            results.push(intermediate_result);
        }

        Ok(results)
    }
}
