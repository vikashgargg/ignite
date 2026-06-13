// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use object_store::ObjectStoreExt;

use super::{ActionCommit, Transaction};
use crate::io::StoreContext;
use crate::spec::manifest::{Manifest, ManifestEntry, ManifestWriterBuilder};
use crate::spec::manifest_list::ManifestListWriter;
use crate::spec::types::Literal;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, Operation, PartitionSpec, Schema,
    SnapshotBuilder, SnapshotReference, SnapshotRetention, TableRequirement, TableUpdate,
    MAIN_BRANCH,
};
use crate::utils::join_table_uri;

pub trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> &'static str;
}

pub struct SnapshotProducer<'a> {
    pub tx: &'a Transaction,
    pub added_data_files: Vec<DataFile>,
    pub store_ctx: Option<StoreContext>,
    pub manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
    pub write_path_mode: crate::utils::WritePathMode,
    /// If true, create a snapshot with no parent (for bootstrap scenarios)
    pub is_bootstrap: bool,
    pub row_lineage_start_row_id: Option<i64>,
    /// When set, only parent files whose partition values are in this set are removed.
    /// Files with other partition values are kept (dynamic partition overwrite semantics).
    pub partition_filter: Option<HashSet<Vec<Option<Literal>>>>,
    /// Extra key/value entries merged into the new snapshot's summary (e.g. the streaming
    /// micro-batch id used for idempotent exactly-once commits).
    pub snapshot_properties: HashMap<String, String>,
}

impl<'a> SnapshotProducer<'a> {
    pub fn new(
        tx: &'a Transaction,
        added_data_files: Vec<DataFile>,
        store_ctx: Option<StoreContext>,
        manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
    ) -> Self {
        Self {
            tx,
            added_data_files,
            store_ctx,
            manifest_metadata,
            write_path_mode: crate::utils::WritePathMode::Absolute,
            is_bootstrap: false,
            row_lineage_start_row_id: None,
            partition_filter: None,
            snapshot_properties: HashMap::new(),
        }
    }

    /// Merge extra entries into the new snapshot's summary (e.g. streaming batch id).
    pub fn with_snapshot_properties(mut self, props: HashMap<String, String>) -> Self {
        self.snapshot_properties = props;
        self
    }

    pub fn with_partition_filter(mut self, filter: HashSet<Vec<Option<Literal>>>) -> Self {
        self.partition_filter = Some(filter);
        self
    }

    pub fn with_write_path_mode(mut self, mode: crate::utils::WritePathMode) -> Self {
        self.write_path_mode = mode;
        self
    }

    /// Enable bootstrap mode: create a snapshot with no parent.
    /// This is used when creating the first snapshot for a table.
    pub fn with_bootstrap(mut self, is_bootstrap: bool) -> Self {
        self.is_bootstrap = is_bootstrap;
        self
    }

    pub fn with_row_lineage_start_row_id(mut self, start_row_id: Option<i64>) -> Self {
        self.row_lineage_start_row_id = start_row_id;
        self
    }

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        // TODO: Implement this function to validate the added data files
        Ok(())
    }

    pub async fn commit(self, op: impl SnapshotProduceOperation) -> Result<ActionCommit, String> {
        let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
        let is_overwrite = op.operation() == Operation::Overwrite.as_str();
        let mut summary = if is_overwrite {
            crate::spec::snapshots::Summary::new(Operation::Overwrite)
        } else {
            crate::spec::snapshots::Summary::new(Operation::Append)
        };
        // Merge caller-supplied summary entries (e.g. the streaming micro-batch id, which a
        // streaming sink reads back for idempotent exactly-once commits).
        for (k, v) in &self.snapshot_properties {
            summary = summary.with_property(k, v);
        }

        // Build manifest metadata: prefer caller-provided metadata derived from table schema/spec
        // Fall back to deriving from the current transaction snapshot if not provided
        let metadata = if let Some(meta) = self.manifest_metadata.clone() {
            meta
        } else {
            let schema_id = self.tx.snapshot().schema_id().unwrap_or_default();
            let schema = Schema::builder()
                .with_schema_id(schema_id)
                .with_fields(vec![])
                .build()
                .map_err(|e| format!("schema build error: {e}"))?;
            let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
            crate::spec::manifest::ManifestMetadata::new(
                std::sync::Arc::new(schema.clone()),
                schema_id,
                partition_spec,
                FormatVersion::V2,
                ManifestContentType::Data,
            )
        };
        let format_version = metadata.format_version;

        let store_ctx = self
            .store_ctx
            .as_ref()
            .ok_or_else(|| "store context not available".to_string())?;

        // Generate new snapshot ID using UUID (not timestamp) and sequence number
        let new_snapshot_id = crate::utils::snapshot_id::generate_snapshot_id();
        let new_sequence_number = if self.is_bootstrap {
            1 // First snapshot starts at sequence 1
        } else {
            self.tx.snapshot().sequence_number() + 1
        };

        let parent_snapshot = self.tx.snapshot();
        let parent_manifest_list_path_str = parent_snapshot.manifest_list();
        let mut parent_manifest_entries = Vec::new();
        let is_partition_overwrite = self.partition_filter.is_some();

        let load_parent = !self.is_bootstrap
            && (!is_overwrite || is_partition_overwrite)
            && !parent_manifest_list_path_str.is_empty();

        if load_parent {
            let (store_ref, manifest_list_path) = store_ctx
                .resolve(parent_manifest_list_path_str)
                .map_err(|e| format!("{}", e))?;

            log::trace!(
                "snapshot producer: loading parent manifest list: {}",
                &manifest_list_path
            );
            let manifest_list_data = store_ref
                .get(&manifest_list_path)
                .await
                .map_err(|e| format!("Failed to get parent manifest list: {}", e))?
                .bytes()
                .await
                .map_err(|e| format!("Failed to read parent manifest list bytes: {}", e))?;
            let parent_manifest_list =
                crate::spec::ManifestList::parse_with_version(&manifest_list_data, format_version)?;
            log::trace!(
                "snapshot producer: found parent manifest files: {}",
                parent_manifest_list.entries().len()
            );

            if let Some(filter) = &self.partition_filter {
                // Partition overwrite: load each manifest, filter out data files in affected
                // partitions, retain everything else.
                for mf in parent_manifest_list.entries() {
                    // Only process data manifests — keep delete manifests as-is.
                    if !matches!(mf.content, ManifestContentType::Data) {
                        parent_manifest_entries.push(mf.clone());
                        continue;
                    }
                    let (mf_store, mf_path) = store_ctx
                        .resolve(&mf.manifest_path)
                        .map_err(|e| format!("resolve manifest path: {e}"))?;
                    let mf_bytes = mf_store
                        .get(&mf_path)
                        .await
                        .map_err(|e| format!("read manifest: {e}"))?
                        .bytes()
                        .await
                        .map_err(|e| format!("read manifest bytes: {e}"))?;
                    let manifest = Manifest::parse_avro(&mf_bytes)
                        .map_err(|e| format!("parse manifest: {e}"))?;

                    let total = manifest.entries().len();
                    let retained: Vec<ManifestEntry> = manifest
                        .entries()
                        .iter()
                        .filter(|entry| !filter.contains(&entry.data_file.partition))
                        .map(|e| (**e).clone())
                        .collect();

                    if retained.len() == total {
                        // Nothing removed — reuse the original manifest file reference.
                        parent_manifest_entries.push(mf.clone());
                    } else if !retained.is_empty() {
                        // Partial removal — write a new manifest with retained entries.
                        let (_, manifest_meta) = manifest.into_parts();
                        let mut writer =
                            ManifestWriterBuilder::new(None, None, manifest_meta).build();
                        for entry in retained {
                            writer.add_existing(entry.data_file);
                        }
                        let new_manifest = writer.finish();
                        let new_manifest_bytes = new_manifest
                            .to_avro_bytes_v2()
                            .map_err(|e| format!("serialize retained manifest: {e}"))?;
                        let new_rel =
                            format!("metadata/manifest-retained-{}.avro", uuid::Uuid::new_v4());
                        let new_path = object_store::path::Path::from(new_rel.as_str());
                        store_ctx
                            .prefixed
                            .put(
                                &new_path,
                                object_store::PutPayload::from(bytes::Bytes::from(
                                    new_manifest_bytes.clone(),
                                )),
                            )
                            .await
                            .map_err(|e| format!("write retained manifest: {e}"))?;
                        let retained_manifest_file =
                            crate::spec::manifest_list::ManifestFile::builder()
                                .with_manifest_path(join_table_uri(
                                    self.tx.table_uri(),
                                    &new_rel,
                                    &self.write_path_mode,
                                ))
                                .with_manifest_length(new_manifest_bytes.len() as i64)
                                .with_partition_spec_id(mf.partition_spec_id)
                                .with_content(ManifestContentType::Data)
                                .with_sequence_number(mf.sequence_number)
                                .with_min_sequence_number(mf.min_sequence_number)
                                .with_added_snapshot_id(mf.added_snapshot_id)
                                .with_file_counts(0, new_manifest.entries().len() as i32, 0)
                                .build()
                                .map_err(|e| format!("build retained manifest file: {e}"))?;
                        parent_manifest_entries.push(retained_manifest_file);
                    }
                    // else: all entries removed — drop this manifest entirely.
                }
            } else {
                parent_manifest_entries.extend(parent_manifest_list.entries().iter().cloned());
            }
        }

        let new_added_rows: i64 = self
            .added_data_files
            .iter()
            .map(|df| df.record_count as i64)
            .sum();
        let mut row_lineage_next_row_id = self.row_lineage_start_row_id;
        let mut snapshot_added_rows = 0;

        if let Some(next_row_id) = &mut row_lineage_next_row_id {
            for entry in &mut parent_manifest_entries {
                if matches!(entry.content, ManifestContentType::Data)
                    && entry.first_row_id.is_none()
                {
                    entry.first_row_id = Some(*next_row_id);
                    let assigned_rows = entry.added_rows_count.unwrap_or(0)
                        + entry.existing_rows_count.unwrap_or(0);
                    *next_row_id += assigned_rows;
                    snapshot_added_rows += assigned_rows;
                }
            }
        }

        let new_manifest_first_row_id = row_lineage_next_row_id;
        if self.row_lineage_start_row_id.is_some() {
            snapshot_added_rows += new_added_rows;
        }

        let mut writer = ManifestWriterBuilder::new(None, None, metadata.clone()).build();
        let added_data_files = self.added_data_files.clone();
        for df in &added_data_files {
            writer.add(df.clone());
        }
        let manifest = writer.finish();
        let manifest_bytes = manifest.to_avro_bytes_v2()?;

        let manifest_len = manifest_bytes.len() as i64;
        let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
        let manifest_path = object_store::path::Path::from(manifest_rel.as_str());
        store_ctx
            .prefixed
            .put(
                &manifest_path,
                object_store::PutPayload::from(Bytes::from(manifest_bytes)),
            )
            .await
            .map_err(|e| format!("{}", e))?;

        let mut manifest_file_builder = crate::spec::manifest_list::ManifestFile::builder()
            .with_manifest_path(join_table_uri(
                self.tx.table_uri(),
                &manifest_rel,
                &self.write_path_mode,
            ))
            .with_manifest_length(manifest_len)
            .with_partition_spec_id(metadata.partition_spec.spec_id())
            .with_content(ManifestContentType::Data)
            .with_sequence_number(new_sequence_number)
            .with_min_sequence_number(new_sequence_number)
            .with_added_snapshot_id(new_snapshot_id)
            .with_file_counts(added_data_files.len() as i32, 0, 0)
            .with_row_counts(new_added_rows, 0, 0);
        if let Some(first_row_id) = new_manifest_first_row_id {
            manifest_file_builder = manifest_file_builder.with_first_row_id(first_row_id);
        }
        let manifest_file = manifest_file_builder.build()?;

        let mut list_writer = ManifestListWriter::new();
        let mut total_manifest_count = 0;

        for entry in parent_manifest_entries {
            list_writer.append(entry);
            total_manifest_count += 1;
        }

        log::trace!(
            "Creating new snapshot: id={} seq={} parent_id={}",
            new_snapshot_id,
            new_sequence_number,
            self.tx.snapshot().snapshot_id()
        );

        list_writer.append(manifest_file);
        total_manifest_count += 1;
        log::trace!(
            "snapshot producer: new manifest list will have files: {}",
            total_manifest_count
        );
        let list_bytes = list_writer.to_bytes(format_version)?;
        let list_rel = format!("metadata/snap-{}.avro", new_snapshot_id);
        let list_path = object_store::path::Path::from(list_rel.as_str());
        store_ctx
            .prefixed
            .put(
                &list_path,
                object_store::PutPayload::from(Bytes::from(list_bytes)),
            )
            .await
            .map_err(|e| format!("{}", e))?;

        let manifest_list_uri =
            join_table_uri(self.tx.table_uri(), &list_rel, &self.write_path_mode);

        let schema_id = if let Some(meta) = &self.manifest_metadata {
            meta.schema_id
        } else {
            self.tx.snapshot().schema_id().unwrap_or_default()
        };

        let mut snapshot_builder = SnapshotBuilder::new()
            .with_snapshot_id(new_snapshot_id)
            .with_sequence_number(new_sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(manifest_list_uri)
            .with_summary(summary)
            .with_schema_id(schema_id);

        // Only set parent snapshot ID if not in bootstrap mode
        if !self.is_bootstrap {
            snapshot_builder =
                snapshot_builder.with_parent_snapshot_id(self.tx.snapshot().snapshot_id());
        }

        if let Some(start_row_id) = self.row_lineage_start_row_id {
            snapshot_builder = snapshot_builder
                .with_first_row_id(start_row_id)
                .with_added_rows(snapshot_added_rows);
        }

        let new_snapshot = snapshot_builder.build()?;

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot.clone(),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference {
                    snapshot_id: new_snapshot.snapshot_id(),
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ];

        // For bootstrap mode, expect no existing snapshot (None)
        // For normal mode, expect the current snapshot ID
        let expected_snapshot_id = if self.is_bootstrap {
            None
        } else {
            Some(self.tx.snapshot().snapshot_id())
        };

        let requirements = vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: expected_snapshot_id,
        }];

        Ok(ActionCommit::new(updates, requirements))
    }
}
