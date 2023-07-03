// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{CacheStorageConfig, StorageBackend};
use quickwit_proto::cache_storage::SplitsChangeNotification;
use tokio::sync::RwLock;

use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, SendableAsync, Storage, StorageFactory,
    StorageResolver, StorageResolverError, StorageResult,
};

/// Storage that wraps two storages using one of them as a cache for another
#[derive(Clone)]
pub struct CacheStorage {
    uri: Uri,
    storage: Arc<dyn Storage>,
    cache: Arc<dyn Storage>,
    cache_splits: Arc<RwLock<HashMap<String, (SplitState, Uri)>>>,
}

impl fmt::Debug for CacheStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("CacheStorage")
            .field("storage", &self.storage)
            .field("cache", &self.cache)
            .finish()
    }
}

impl CacheStorage {
    /// Create a resolver that can uses ram storage for both cache and the upstream storage
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> CacheStorage {
        use crate::RamStorage;

        CacheStorage {
            uri: Uri::for_test("cache:///"),
            storage: Arc::new(RamStorage::default()),
            cache: Arc::new(RamStorage::default()),
            cache_splits: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Storage for CacheStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.cache.check_connectivity().await?;
        self.storage.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.storage.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.storage.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let guard = self.cache_splits.read().await;
        let split_id = path.to_str().unwrap();
        let split_id = &split_id[..split_id.len() - ".split".len()];
        if let Some(split) = guard.get(split_id) {
            match split.0 {
                SplitState::Loading => {
                    // TODO: Slice is not ready
                }
                SplitState::Ready => return self.cache.get_slice(path, range).await,
                SplitState::Deleted => {
                    // TODO: Slice is deleted
                }
            }
        }
        self.storage.get_slice(path, range).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.storage.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.storage.bulk_delete(paths).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        // TODO: Add caching logic
        self.storage.get_all(path).await
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }
}

impl CacheStorageFactory {
    pub fn new(storage_config: CacheStorageConfig) -> Self {
        Self {
            storage_config,
            splits: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn update_split_cache(
        &self,
        storage_resolver: &StorageResolver,
        splits: Vec<SplitsChangeNotification>,
    ) -> StorageResult<()> {
        let mut splits_guard = self.splits.write().await;
        let new_splits: HashSet<String> = splits.iter().map(|rec| rec.split_id.clone()).collect();
        let old_splits: HashSet<String> = splits_guard.keys().into_iter().cloned().collect();
        let deleted = old_splits.difference(&new_splits);
        for split in splits {
            if !splits_guard.contains_key(&split.split_id) {
                let uri: Uri = split.storage_uri.parse().expect("Shouldn't happen");
                self.load(storage_resolver, &uri, split.split_id.clone())
                    .await;
                splits_guard.insert(split.split_id.clone(), (SplitState::Loading, uri));
            }
        }
        for delete in deleted {
            let res = splits_guard.get(delete).expect("Should be there");
            let status = res.0.clone();
            let uri = res.1.clone();
            match status {
                SplitState::Loading => {
                    splits_guard.insert(delete.clone(), (SplitState::Deleted, uri));
                }
                SplitState::Ready => {
                    self.delete(storage_resolver, &uri, delete.clone()).await;
                    splits_guard.remove(delete);
                }
                SplitState::Deleted => {
                    // Don't need to do anything, delete is async and might take a while
                }
            };
        }
        Ok(())
    }

    async fn load(&self, storage_resolver: &StorageResolver, storage_uri: &Uri, split_id: String) {
        let storage = storage_resolver
            .resolve(storage_uri)
            .await
            .expect("Should be able to resolve");
        // TODO: Error handling
        let cache = storage_resolver
            .resolve(
                &self
                    .storage_config
                    .cache_uri
                    .clone()
                    .unwrap()
                    .parse::<Uri>()
                    .unwrap(),
            )
            .await
            .unwrap();
        let splits = self.splits.clone();
        tokio::spawn({
            async move {
                // TODO: Need to figure out a better way to translate between paths and ids
                let path_str = split_id.clone() + ".split";
                let path = Path::new(&path_str);
                // TODO: Replace with real async copy
                let mut buf = Vec::new();
                // TODO: Need some error hanlding here
                storage
                    .copy_to(path, &mut buf)
                    .await
                    .expect("Need real error handling");
                cache
                    .put(path, Box::new(buf))
                    .await
                    .expect("Need real error handling");
                let mut splits_guard = splits.write().await;
                if let Some((state, uri)) = splits_guard.get(&split_id) {
                    match state {
                        SplitState::Loading => {
                            let uri = uri.clone();
                            splits_guard.insert(split_id, (SplitState::Ready, uri.clone()));
                        }
                        SplitState::Ready => {
                            // TODO: Shouldn't be here?
                        }
                        SplitState::Deleted => {
                            // TODO: Delete split
                        }
                    };
                };
            }
        });
    }

    async fn delete(
        &self,
        _storage_resolver: &StorageResolver,
        _storage_uri: &Uri,
        _split_id: String,
    ) {
        // TODO: implement cleanup
    }
}

struct SendableAsyncStorage {
    storage: Arc<dyn Storage>,
}

#[derive(Clone)]
enum SplitState {
    Loading,
    Ready,
    Deleted,
}

/// Storage resolver for [`CacheStorage`].
pub struct CacheStorageFactory {
    storage_config: CacheStorageConfig,
    splits: Arc<RwLock<HashMap<String, (SplitState, Uri)>>>,
}

#[async_trait]
impl StorageFactory for CacheStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Cache
    }

    async fn resolve(
        &self,
        storage_resolver: &StorageResolver,
        uri: &Uri,
    ) -> Result<Arc<dyn Storage>, StorageResolverError> {
        if uri.protocol().is_cache() {
            let cache_uri = self
                .storage_config
                .cache_uri()
                .ok_or_else(|| {
                    StorageResolverError::InvalidConfig("Expected cache uri in config.".to_string())
                })?
                .parse::<Uri>()
                .map_err(|err| {
                    let message = format!("Cannot parse cache uri `{:?}`.", err.to_string());
                    StorageResolverError::InvalidConfig(message)
                })?;
            let cache = storage_resolver.resolve(&cache_uri).await?;
            let upstream_uri = uri
                .scheme_specific_part()
                .ok_or_else(|| {
                    StorageResolverError::InvalidConfig(
                        "Expected cache uri with child part in index config".to_string(),
                    )
                })?
                .parse::<Uri>()
                .map_err(|err| {
                    let message =
                        format!("Cannot parse index storage uri `{:?}`.", err.to_string());
                    StorageResolverError::InvalidConfig(message)
                })?;
            let storage = storage_resolver.resolve(&upstream_uri).await?;
            let cache_storage = CacheStorage {
                uri: uri.clone(),
                storage,
                cache,
                cache_splits: self.splits.clone(),
            };
            Ok(Arc::new(cache_storage))
        } else {
            let message = format!("URI `{uri}` is not a valid Cache URI.");
            Err(StorageResolverError::InvalidUri(message))
        }
    }

    fn as_cache_storage_factory<'a>(&'a self) -> Option<&'a CacheStorageFactory> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {

    use quickwit_config::CacheStorageConfig;

    use super::*;
    use crate::storage_test_suite;

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let mut ram_storage = CacheStorage::for_test();
        storage_test_suite(&mut ram_storage).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_storage_factory() {
        let storage_resolver = StorageResolver::ram_for_test();
        let storage_config = CacheStorageConfig::for_test().into();
        let cache_storage_factory = CacheStorageFactory::new(storage_config);
        let cache_uri = Uri::from_well_formed("s3:///foo");
        let err = cache_storage_factory
            .resolve(&storage_resolver, &cache_uri)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));

        let data_uri = Uri::from_well_formed("cache:///");
        let data_storage = cache_storage_factory
            .resolve(&storage_resolver, &data_uri)
            .await
            .ok()
            .unwrap();

        let data_storage_two = cache_storage_factory
            .resolve(&storage_resolver, &data_uri)
            .await
            .ok()
            .unwrap();
        assert_eq!(data_storage.uri(), data_storage_two.uri());
    }
}
