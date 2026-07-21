use std::sync::Arc;

use datafusion::common::Result;
use zelox_common_datafusion::datasource::TableFormatRegistry;
use zelox_data_source::formats::arrow::ArrowTableFormat;
use zelox_data_source::formats::avro::AvroTableFormat;
use zelox_data_source::formats::binary::BinaryTableFormat;
use zelox_data_source::formats::console::ConsoleTableFormat;
use zelox_data_source::formats::csv::CsvTableFormat;
use zelox_data_source::formats::json::JsonTableFormat;
use zelox_data_source::formats::kafka::KafkaTableFormat;
use zelox_data_source::formats::parquet::ParquetTableFormat;
use zelox_data_source::formats::python::{discover_data_sources, PythonTableFormat};
use zelox_data_source::formats::rate::RateTableFormat;
use zelox_data_source::formats::socket::SocketTableFormat;
use zelox_data_source::formats::text::TextTableFormat;
use zelox_delta_lake::DeltaTableFormat;
use zelox_iceberg::IcebergTableFormat;
use zelox_vortex::VortexTableFormat;

pub fn create_table_format_registry() -> Result<Arc<TableFormatRegistry>> {
    let registry = Arc::new(TableFormatRegistry::new());
    register_builtin_formats(&registry)?;
    register_external_formats(&registry)?;
    Ok(registry)
}

fn register_builtin_formats(registry: &Arc<TableFormatRegistry>) -> Result<()> {
    registry.register(Arc::new(ArrowTableFormat::default()))?;
    registry.register(Arc::new(AvroTableFormat::default()))?;
    registry.register(Arc::new(BinaryTableFormat::default()))?;
    registry.register(Arc::new(CsvTableFormat::default()))?;
    registry.register(Arc::new(JsonTableFormat::default()))?;
    registry.register(Arc::new(ParquetTableFormat::default()))?;
    registry.register(Arc::new(TextTableFormat::default()))?;
    registry.register(Arc::new(SocketTableFormat))?;
    registry.register(Arc::new(RateTableFormat))?;
    registry.register(Arc::new(KafkaTableFormat))?;
    registry.register(Arc::new(ConsoleTableFormat))?;
    Ok(())
}

fn register_external_formats(registry: &Arc<TableFormatRegistry>) -> Result<()> {
    DeltaTableFormat::register(registry)?;
    IcebergTableFormat::register(registry)?;
    VortexTableFormat::register(registry)?;

    // Register Python data sources
    {
        discover_data_sources()?;
        PythonTableFormat::register_all(registry)?;
    }

    Ok(())
}
