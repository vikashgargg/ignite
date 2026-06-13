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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::stream::once;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use url::Url;

use crate::io::StoreContext;
use crate::operations::bootstrap::{
    bootstrap_first_snapshot, bootstrap_new_table, PersistStrategy,
};
use crate::operations::helpers::format_version_for_schema;
use crate::operations::{SnapshotProduceOperation, Transaction, TransactionAction};
use crate::physical_plan::action_schema::decode_actions_and_meta_from_batch;
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::catalog::TableUpdate;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{PartitionSpec, Schema as IcebergSchema, TableMetadata, TableRequirement};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path,
};
use crate::utils::get_object_store_from_context;
use crate::utils::metadata::metadata_files_for_version;
const MAX_COMMIT_RETRIES: usize = 5;

/// Snapshot-summary key recording the streaming micro-batch id committed by this snapshot, and
/// the owning query's app id. Used for idempotent exactly-once streaming commits: a replayed
/// batch whose id is `<=` the table's last committed id (for the same app) is skipped. Mirrors
/// Flink's `flink.max-committed-checkpoint-id` / Spark Iceberg streaming committer.
pub const STREAM_BATCH_ID_PROP: &str = "vajra.streaming.batch-id";
pub const STREAM_APP_ID_PROP: &str = "vajra.streaming.app-id";

/// Identifies a streaming micro-batch commit for idempotent exactly-once.
#[derive(Debug, Clone)]
pub struct StreamingCommit {
    pub batch_id: u64,
    pub app_id: String,
}

#[derive(Debug)]
pub struct IcebergCommitExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    /// When set, this commit is a streaming micro-batch: record `batch_id` in the snapshot summary
    /// and skip the commit if that batch was already committed (idempotent replay).
    streaming_commit: Option<StreamingCommit>,
    cache: Arc<PlanProperties>,
}

impl IcebergCommitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: Url) -> Self {
        Self::new_with_streaming(input, table_url, None)
    }

    pub fn new_with_streaming(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        streaming_commit: Option<StreamingCommit>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            true,
        )]));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            table_url,
            streaming_commit,
            cache,
        }
    }

    /// Read the last committed streaming batch id for `app_id` from a snapshot summary.
    fn committed_batch_id(summary: &crate::spec::snapshots::Summary, app_id: &str) -> Option<u64> {
        if summary.additional_properties.get(STREAM_APP_ID_PROP).map(String::as_str) != Some(app_id)
        {
            return None;
        }
        summary
            .additional_properties
            .get(STREAM_BATCH_ID_PROP)
            .and_then(|v| v.parse::<u64>().ok())
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    fn apply_schema_update(table_meta: &mut TableMetadata, new_schema: IcebergSchema) {
        let schema_id = new_schema.schema_id();
        let highest_field_id = new_schema.highest_field_id();

        let mut replaced = false;
        for schema in table_meta.schemas.iter_mut() {
            if schema.schema_id() == schema_id {
                *schema = new_schema.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            table_meta.schemas.push(new_schema.clone());
        }

        table_meta.current_schema_id = schema_id;
        table_meta.last_column_id = table_meta.last_column_id.max(highest_field_id);
        table_meta.format_version = table_meta
            .format_version
            .max(format_version_for_schema(&new_schema));
    }

    fn apply_partition_spec_update(table_meta: &mut TableMetadata, new_spec: PartitionSpec) {
        let spec_id = new_spec.spec_id();
        let mut replaced = false;
        for spec in table_meta.partition_specs.iter_mut() {
            if spec.spec_id() == spec_id {
                *spec = new_spec.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            table_meta.partition_specs.push(new_spec.clone());
        }
        table_meta.default_spec_id = spec_id;
        if let Some(highest) = new_spec.highest_field_id() {
            table_meta.last_partition_id = table_meta.last_partition_id.max(highest);
        }
    }

    fn validate_requirements(
        table_meta: Option<&TableMetadata>,
        requirements: &[TableRequirement],
    ) -> Result<()> {
        for requirement in requirements {
            match requirement {
                TableRequirement::NotExist => {
                    if table_meta.is_some() {
                        return Err(DataFusionError::Plan(
                            "Iceberg table already exists but commit asserted non-existence."
                                .to_string(),
                        ));
                    }
                }
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating field id requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.last_column_id != last_assigned_field_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected last assigned field id {} but found {}. Reload table metadata and retry.",
                            last_assigned_field_id, meta.last_column_id
                        )));
                    }
                }
                TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating schema requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.current_schema_id != current_schema_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected current schema id {} but found {}. Reload table metadata and retry.",
                            current_schema_id, meta.current_schema_id
                        )));
                    }
                }
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: reference,
                    snapshot_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating snapshot requirement"
                                .to_string(),
                        )
                    })?;
                    let actual = if reference == MAIN_BRANCH {
                        meta.current_snapshot_id
                    } else {
                        meta.refs
                            .get(reference)
                            .map(|ref_entry| ref_entry.snapshot_id)
                    };
                    if &actual != snapshot_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: reference '{}' expected snapshot {:?} but found {:?}",
                            reference, snapshot_id, actual
                        )));
                    }
                }
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Table requirement '{other:?}' is not supported in local commits"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionPlan for IcebergCommitExec {
    fn name(&self) -> &'static str {
        "IcebergCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergCommitExec requires exactly one child");
        }
        Ok(Arc::new(Self::new_with_streaming(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.streaming_commit.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("IcebergCommitExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "IcebergCommitExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let input_stream = self.input.execute(0, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let schema = self.schema();
        let streaming_commit = self.streaming_commit.clone();
        let future = async move {
            let object_store = get_object_store_from_context(&context, &table_url)?;
            let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

            // Read writer result as Arrow-native action batches (may be empty for IgnoreIfExists).
            let mut data = input_stream;
            let mut added_data_files = Vec::new();
            let mut commit_meta = None;
            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let (adds, _deletes, meta) = decode_actions_and_meta_from_batch(&batch)?;
                added_data_files.extend(adds);
                if meta.is_some() {
                    commit_meta = meta;
                }
            }

            // No-op path (e.g. IgnoreIfExists on existing table): no rows, no meta.
            if commit_meta.is_none() && added_data_files.is_empty() {
                let array = Arc::new(UInt64Array::from(vec![0u64]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let commit_meta = commit_meta.ok_or_else(|| {
                DataFusionError::Internal(
                    "missing commit_meta action from writer output".to_string(),
                )
            })?;

            // Record the streaming micro-batch id in the snapshot summary so a replayed batch can
            // be recognized and skipped (idempotent exactly-once).
            let mut snapshot_properties = std::collections::HashMap::new();
            if let Some(sc) = &streaming_commit {
                snapshot_properties.insert(STREAM_BATCH_ID_PROP.to_string(), sc.batch_id.to_string());
                snapshot_properties.insert(STREAM_APP_ID_PROP.to_string(), sc.app_id.clone());
            }

            let commit_info = IcebergCommitInfo {
                table_uri: commit_meta.table_uri,
                row_count: commit_meta.row_count,
                data_files: added_data_files,
                manifest_path: String::new(),
                manifest_list_path: String::new(),
                updates: vec![],
                requirements: commit_meta.requirements,
                table_properties: commit_meta.table_properties,
                operation: commit_meta.operation,
                schema: commit_meta.schema,
                partition_spec: commit_meta.partition_spec,
                snapshot_properties,
            };

            // Load table metadata JSON if exists; for overwrite on new table we bootstrap
            let latest_meta_res =
                crate::table::find_latest_metadata_file(&object_store, &table_url).await;

            if latest_meta_res.is_err()
                && (matches!(commit_info.operation, crate::spec::Operation::Overwrite)
                    || matches!(commit_info.operation, crate::spec::Operation::Append)
                    || matches!(
                        commit_info.operation,
                        crate::spec::Operation::OverwritePartitions
                    ))
            {
                Self::validate_requirements(None, &commit_info.requirements)?;
                // Bootstrap a new table using the unified bootstrap helper
                bootstrap_new_table(&table_url, &store_ctx, &commit_info).await?;

                let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let initial_latest_meta =
                latest_meta_res.map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut attempt = 0;
            loop {
                attempt += 1;
                let latest_meta = if attempt == 1 {
                    initial_latest_meta.clone()
                } else {
                    crate::table::find_latest_metadata_file(&object_store, &table_url)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                };

                let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
                let mut table_meta = TableMetadata::from_json(&bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Self::validate_requirements(Some(&table_meta), &commit_info.requirements)?;
                if let Some(new_schema) = commit_info.schema.clone() {
                    Self::apply_schema_update(&mut table_meta, new_schema);
                }
                let mut partition_spec_for_commit = table_meta
                    .default_partition_spec()
                    .cloned()
                    .unwrap_or_else(PartitionSpec::unpartitioned_spec);
                if let Some(new_spec) = commit_info.partition_spec.clone() {
                    let spec = if new_spec.spec_id() == 0 && table_meta.default_spec_id != 0 {
                        new_spec.with_spec_id(table_meta.default_spec_id)
                    } else {
                        new_spec
                    };
                    Self::apply_partition_spec_update(&mut table_meta, spec.clone());
                    partition_spec_for_commit = spec;
                }
                let maybe_snapshot = table_meta.current_snapshot().cloned();
                let schema_iceberg = table_meta.current_schema().cloned().ok_or_else(|| {
                    DataFusionError::Plan("No current schema in table metadata".to_string())
                })?;
                table_meta.format_version = table_meta
                    .format_version
                    .max(format_version_for_schema(&schema_iceberg));
                let row_lineage_start_row_id = table_meta.row_lineage_start_row_id();

                // If metadata exists but there is no current snapshot (e.g. from a CREATE TABLE),
                // bootstrap the first snapshot into the existing metadata using InPlace strategy
                // (per user preference to keep external SQL catalogs in sync).
                if maybe_snapshot.is_none()
                    && (matches!(commit_info.operation, crate::spec::Operation::Overwrite)
                        || matches!(commit_info.operation, crate::spec::Operation::Append)
                        || matches!(
                            commit_info.operation,
                            crate::spec::Operation::OverwritePartitions
                        ))
                {
                    bootstrap_first_snapshot(
                        &table_url,
                        &store_ctx,
                        &commit_info,
                        table_meta,
                        &latest_meta,
                        PersistStrategy::InPlace,
                    )
                    .await?;

                    let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                    let batch = RecordBatch::try_new(schema, vec![array])?;
                    return Ok(batch);
                }

                let snapshot = maybe_snapshot.ok_or_else(|| {
                    DataFusionError::Plan("No current snapshot in table metadata".to_string())
                })?;

                // Idempotent exactly-once: if this streaming micro-batch (or an earlier one) was
                // already committed to the table, skip — a crashed-then-replayed batch must not
                // append its data twice. The committed id lives in the current snapshot summary.
                if let Some(sc) = &streaming_commit {
                    if let Some(committed) = Self::committed_batch_id(snapshot.summary(), &sc.app_id)
                    {
                        if committed >= sc.batch_id {
                            log::info!(
                                "Iceberg streaming commit: batch {} already committed (table at {}), skipping",
                                sc.batch_id,
                                committed
                            );
                            let array = Arc::new(UInt64Array::from(vec![0u64]));
                            let batch = RecordBatch::try_new(schema, vec![array])?;
                            return Ok(batch);
                        }
                    }
                }

                let current_version = metadata_file_version_from_path(&latest_meta).unwrap_or(0);
                let next_version = current_version + 1;

                let existing_for_next =
                    metadata_files_for_version(&store_ctx, next_version).await?;
                if !existing_for_next.is_empty() {
                    log::warn!(
                        "Detected existing metadata files for version {}: {:?}. Retrying attempt {}",
                        next_version,
                        existing_for_next,
                        attempt
                    );
                    if attempt >= MAX_COMMIT_RETRIES {
                        return Err(commit_conflict_error());
                    }
                    continue;
                }

                // Build transaction and action based on operation
                let tx = Transaction::new(table_url.to_string(), snapshot);
                let manifest_meta = tx.default_manifest_metadata(
                    &schema_iceberg,
                    &partition_spec_for_commit,
                    table_meta.format_version,
                );
                let action_commit = match commit_info.operation {
                    crate::spec::Operation::Append => {
                        let mut action = tx
                            .fast_append()
                            .with_store_context(store_ctx.clone())
                            .with_manifest_metadata(manifest_meta)
                            .with_row_lineage_start_row_id(row_lineage_start_row_id)
                            .set_snapshot_properties(commit_info.snapshot_properties.clone());
                        for df in commit_info.data_files.clone().into_iter() {
                            action.add_file(df);
                        }
                        Arc::new(action)
                            .commit(&tx)
                            .await
                            .map_err(DataFusionError::Execution)?
                    }
                    crate::spec::Operation::Overwrite => {
                        let producer = crate::operations::SnapshotProducer::new(
                            &tx,
                            commit_info.data_files.clone(),
                            Some(store_ctx.clone()),
                            Some(manifest_meta),
                        )
                        .with_row_lineage_start_row_id(row_lineage_start_row_id);
                        struct LocalOverwriteOperation;
                        impl SnapshotProduceOperation for LocalOverwriteOperation {
                            fn operation(&self) -> &'static str {
                                "overwrite"
                            }
                        }
                        producer
                            .commit(LocalOverwriteOperation)
                            .await
                            .map_err(DataFusionError::Execution)?
                    }
                    crate::spec::Operation::OverwritePartitions => {
                        // Dynamic partition overwrite: replace only the partitions present in
                        // the new data; leave all other partitions untouched.
                        let partition_filter: HashSet<Vec<Option<crate::spec::types::Literal>>> =
                            commit_info
                                .data_files
                                .iter()
                                .map(|df| df.partition.clone())
                                .collect();
                        let producer = crate::operations::SnapshotProducer::new(
                            &tx,
                            commit_info.data_files.clone(),
                            Some(store_ctx.clone()),
                            Some(manifest_meta),
                        )
                        .with_partition_filter(partition_filter)
                        .with_row_lineage_start_row_id(row_lineage_start_row_id);
                        struct LocalPartitionOverwriteOperation;
                        impl SnapshotProduceOperation for LocalPartitionOverwriteOperation {
                            fn operation(&self) -> &'static str {
                                "overwrite"
                            }
                        }
                        producer
                            .commit(LocalPartitionOverwriteOperation)
                            .await
                            .map_err(DataFusionError::Execution)?
                    }
                    _ => {
                        return Err(DataFusionError::NotImplemented(
                            "Unsupported Iceberg operation in commit".to_string(),
                        ));
                    }
                };

                // Apply updates (only handle the ones we emit: AddSnapshot, SetSnapshotRef)
                let action_requirements = action_commit.requirements().to_vec();
                Self::validate_requirements(Some(&table_meta), &action_requirements)?;
                let updates = action_commit.into_updates();
                log::trace!("commit_exec: applying updates: {:?}", &updates);
                let mut newest_snapshot_seq: Option<i64> = None;
                let mut newest_snapshot_added_rows: Option<i64> = None;
                let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
                for upd in updates {
                    match upd {
                        TableUpdate::AddSnapshot { snapshot } => {
                            newest_snapshot_seq = Some(snapshot.sequence_number());
                            newest_snapshot_added_rows = snapshot.added_rows;
                            table_meta.snapshots.push(snapshot.clone());
                            table_meta.current_snapshot_id = Some(snapshot.snapshot_id());
                            table_meta.snapshot_log.push(SnapshotLog {
                                timestamp_ms,
                                snapshot_id: snapshot.snapshot_id(),
                            });
                        }
                        TableUpdate::SetSnapshotRef {
                            ref_name,
                            reference,
                        } => {
                            table_meta.refs.insert(ref_name, reference);
                        }
                        _ => {}
                    }
                }
                if let Some(seq) = newest_snapshot_seq {
                    if seq > table_meta.last_sequence_number {
                        table_meta.last_sequence_number = seq;
                    }
                }
                table_meta.last_updated_ms = timestamp_ms;
                if let Some(added_rows) = newest_snapshot_added_rows {
                    table_meta.advance_next_row_id(added_rows);
                }

                // Add metadata_log entry referencing previous metadata file
                table_meta
                    .metadata_log
                    .push(crate::spec::metadata::table_metadata::MetadataLog {
                        timestamp_ms,
                        metadata_file: latest_meta.clone(),
                    });

                let new_meta_json = table_meta
                    .to_json()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let file_extension =
                    metadata_file_extension_from_properties(&table_meta.properties)?;
                let new_meta_rel = format!("metadata/v{next_version}{file_extension}");
                let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_json)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                log::trace!(
                    "Writing metadata: {} snapshot_id={:?} table_url={}",
                    &new_meta_rel,
                    table_meta.current_snapshot_id,
                    &table_url
                );

                let new_meta_path = object_store::path::Path::from(new_meta_rel.as_str());
                let put_opts = object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                };
                let payload = object_store::PutPayload::from(Bytes::from(new_meta_bytes));
                match store_ctx
                    .prefixed
                    .put_opts(&new_meta_path, payload, put_opts)
                    .await
                {
                    Ok(_) => {}
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        log::warn!(
                            "Metadata file {} already exists for version {}. Retrying attempt {}",
                            new_meta_rel,
                            next_version,
                            attempt
                        );
                        if attempt >= MAX_COMMIT_RETRIES {
                            return Err(commit_conflict_error());
                        }
                        continue;
                    }
                    Err(e) => return Err(DataFusionError::External(Box::new(e))),
                }
                let version_files = metadata_files_for_version(&store_ctx, next_version).await?;
                let conflict_after_write = version_files.iter().any(|path| path != &new_meta_rel);
                if conflict_after_write {
                    log::warn!(
                        "Concurrent metadata writes detected for version {}: {:?}. Retrying attempt {}",
                        next_version,
                        version_files,
                        attempt
                    );
                    if let Err(err) = store_ctx.prefixed.delete(&new_meta_path).await {
                        log::warn!(
                            "Failed to delete conflicted metadata file {}: {:?}",
                            new_meta_rel,
                            err
                        );
                    }
                    if attempt >= MAX_COMMIT_RETRIES {
                        return Err(commit_conflict_error());
                    }
                    continue;
                }
                log::trace!("Metadata written successfully");

                let hint_bytes = Bytes::from(next_version.to_string().into_bytes());
                let hint_path = object_store::path::Path::from("metadata/version-hint.text");
                store_ctx
                    .prefixed
                    .put(&hint_path, object_store::PutPayload::from(hint_bytes))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergCommitExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

fn commit_conflict_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg commit failed after {MAX_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}
