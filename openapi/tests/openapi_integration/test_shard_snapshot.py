from time import sleep
import hashlib
import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_snapshot'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_shard_snapshot_operations(http_server):
    (srv_dir, srv_url) = http_server

    # no snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="POST",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        query_params={'wait': 'true'},
    )
    assert response.ok
    snapshot_name = response.json()['result']['name']
    snapshot_checksum = response.json()['result']['checksum']

    # validate it exists
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 1
    assert response.json()['result'][0]['name'] == snapshot_name
    assert response.json()['result'][0]['checksum'] == snapshot_checksum

    # download it, save, and validate checksum
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
    )
    assert response.ok
    with open(srv_dir / "snapshot.tar", 'wb') as f:
        f.write(response.content)
    assert snapshot_checksum == hashlib.sha256(response.content).hexdigest()

    # delete it
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}',
        method="DELETE",
        path_params={'shard_id': 0, 'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
        query_params={'wait': 'true'},
    )
    assert response.ok

    # validate it is gone
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # try to recover shard from snapshot with wrong checksum
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": f"{srv_url}/snapshot.tar",
            "checksum": "3" * len(snapshot_checksum),
            "wait": "true",
        },
    )
    assert response.status_code == 400

    # recover shard from snapshot with correct checksum
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": f"{srv_url}/snapshot.tar",
            "checksum": snapshot_checksum,
            "wait": "true",
        },
    )
    assert response.ok


@pytest.mark.timeout(20)
def test_shard_snapshot_operations_non_wait():
    # there no snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="POST",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    # validate it exists
    snapshot_name = None
    while True:
        try:
            response = request_with_validation(
                api='/collections/{collection_name}/shards/{shard_id}/snapshots',
                method="GET",
                path_params={'shard_id': 0, 'collection_name': collection_name},
            )
            assert response.ok
            assert len(response.json()['result']) == 1
            snapshot_name = response.json()['result'][0]['name']
            break
        except AssertionError:
            # wait for snapshot to be created
            sleep(0.1)
            continue

    # delete it
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}',
        method="DELETE",
        path_params={'shard_id': 0, 'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    # validate it is gone
    while True:
        try:
            response = request_with_validation(
                api='/collections/{collection_name}/shards/{shard_id}/snapshots',
                method="GET",
                path_params={'shard_id': 0, 'collection_name': collection_name},
            )
            assert response.ok
            assert len(response.json()['result']) == 0
            break
        except AssertionError:
            # wait for snapshot to be deleted
            sleep(0.1)
            continue


def test_shard_snapshot_recovery_errors():

    # Invalid collection name
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': "somethingthatdoesnotexist"},
        body={
            "location": "whatever",
        }
    )
    assert response.status_code == 404

    # Invalid file url
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": "file:///whatever.snapshot",
        }
    )
    assert response.status_code == 400

    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": "http://localhost:6333/snapshots/whatever.snapshot",
        }
    )
    assert response.status_code == 400
