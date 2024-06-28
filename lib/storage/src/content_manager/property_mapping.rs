use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use collection::shards::CollectionId;
use io::file_operations::{atomic_save_json, init_file, read_json};
use serde::{Deserialize, Serialize};

use crate::content_manager::errors::StorageError;

pub const COLLECTION_PROPERTY_MAPPING: &str = "properties.json";

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub struct CollectionPropertyMapping(HashMap<CollectionId, HashMap<String, String>>);

impl CollectionPropertyMapping {
    pub fn load(path: &Path) -> Result<Self, StorageError> {
        Ok(read_json(path)?)
    }

    pub fn save(&self, path: &Path) -> Result<(), StorageError> {
        Ok(atomic_save_json(path, self)?)
    }
}

#[derive(Debug)]
pub struct CollectionPropertyPersistence {
    data_path: PathBuf,
    mapping: CollectionPropertyMapping,
}

impl CollectionPropertyPersistence {
    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(COLLECTION_PROPERTY_MAPPING)
    }

    pub fn open(dir_path: PathBuf) -> Result<Self, StorageError> {
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }
        let config_path = Self::get_config_path(&dir_path);
        let data_path = init_file(config_path)?;
        let mapping = CollectionPropertyMapping::load(&data_path)?;
        Ok(CollectionPropertyPersistence { data_path, mapping })
    }

    pub fn delete(
        &mut self,
        collection_name: String,
        property_name: String,
    ) -> Result<(), StorageError> {
        if let Some(result) = self.mapping.0.get(&collection_name) {
            let mut values = result.clone();
            values.remove(&property_name);
            if values.is_empty() {
                self.mapping.0.remove(&collection_name);
            } else {
                self.mapping.0.insert(collection_name, values);
            }
            self.mapping.save(&self.data_path)?;
        }
        Ok(())
    }

    pub fn insert(
        &mut self,
        data: (String, String),
        collection_name: String,
    ) -> Result<(), StorageError> {
        let (key, value) = data;
        if let Some(result) = self.mapping.0.get(&collection_name) {
            let mut values = result.clone();
            values.insert(key, value);
            self.mapping.0.insert(collection_name, values);
        } else {
            let mut values = HashMap::new();
            values.insert(key, value);
            self.mapping.0.insert(collection_name, values);
        }
        self.mapping.save(&self.data_path)?;
        Ok(())
    }

    pub fn get(&self, collection_name: &str) -> Option<HashMap<String, String>> {
        self.mapping.0.get(collection_name).cloned()
    }
}
