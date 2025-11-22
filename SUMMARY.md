# Project Summary - Dataflow Pipeline Simulation

## Overview
This project is a comprehensive ETL (Extract, Transform, Load) pipeline simulation that mimics Google Cloud Platform's Dataflow, BigQuery, and Pub/Sub services using Python and Apache Beam.

## Components Implemented

### 1. Pub/Sub Simulation (`src/pubsub_simulation/`)
- **Message**: Represents individual messages with data, ID, and timestamp
- **Topic**: Publishing endpoint for messages
- **Subscription**: Consumer endpoint that receives messages from topics
- **PubSubSimulator**: Main orchestrator for managing topics and subscriptions

Features:
- Thread-safe message queuing
- Pull-based and callback-based subscription models
- Realistic message flow simulation

### 2. BigQuery Models (`src/bigquery_models/`)
- **TableSchema**: Base schema class with validation
- **RawTransactionSchema**: Schema for unprocessed transaction data
- **ProcessedTransactionSchema**: Schema for enriched transaction data
- **TransactionSummarySchema**: Schema for aggregated metrics
- **BigQuerySimulator**: In-memory table management with insert/query operations

Features:
- Schema validation
- Multiple table support
- JSON export functionality
- Row counting and querying

### 3. ETL Pipeline (`src/pipeline/`)
- **ParseJsonMessage**: Extracts data from Pub/Sub messages
- **FilterValidTransactions**: Validates and filters transaction data
- **EnrichTransaction**: Adds categories, flags, and currency conversions
- **AggregateByUserCategory**: Groups and aggregates transaction metrics
- **FormatForBigQuery**: Ensures proper data formatting
- **ExtractUserCategoryKey**: Creates composite keys for grouping
- **DataflowETLPipeline**: Main pipeline orchestrator

Pipeline Stages:
1. Raw Ingestion: Parse → Filter → Store raw data
2. Transformation: Parse → Filter → Enrich → Store processed data
3. Aggregation: Parse → Filter → Enrich → Group → Aggregate → Store summaries

### 4. Utilities (`src/utils/`)
- **TransactionDataGenerator**: Creates realistic sample transaction data
- **ConfigLoader**: Manages YAML configuration loading

### 5. SQL Schemas (`sql/`)
- Table definitions for BigQuery (with partitioning and clustering)
- Analytical queries for common business questions

## Data Flow

```
Transaction Data Generator
          ↓
     Pub/Sub Topic
          ↓
   Pub/Sub Subscription
          ↓
    ETL Pipeline Stage 1: Raw Ingestion
          ↓
    raw_transactions table
          ↓
    ETL Pipeline Stage 2: Transformation
          ↓
    processed_transactions table
          ↓
    ETL Pipeline Stage 3: Aggregation
          ↓
    transaction_summary table
          ↓
    JSON Export Files
```

## Key Features

### Transaction Enrichment
- **Categorization**: Automatic categorization based on merchant
- **High-Value Flagging**: Identifies transactions over $1000
- **Currency Normalization**: Converts all amounts to USD
- **Timestamps**: Adds processing timestamps

### Aggregations
- Sum, average, min, max amounts per user/category
- Transaction counts
- Daily summaries

### Categories Supported
- Shopping (Amazon, Walmart, Target)
- Food & Beverage (Starbucks, McDonalds)
- Gas & Fuel (Shell, Chevron)
- Entertainment (Netflix, Spotify)
- Transportation (Uber, Lyft)
- Other (uncategorized)

## Testing
- 20 unit tests covering all major components
- Test coverage for Pub/Sub, BigQuery, and data generation
- All tests passing with 100% success rate

## Configuration
All configurable via `config.yaml`:
- Pipeline settings (runner type)
- Pub/Sub topic/subscription names
- BigQuery dataset and table names
- Data generation parameters
- Transformation rules
- Currency conversion rates

## Usage Examples

### Basic Usage
```bash
python main.py
```

### Simple Example
```bash
python example.py
```

### Running Tests
```bash
pytest tests/ -v
```

## Output Files
All output in `data/output/`:
- `raw_transactions.json`: Unprocessed data
- `processed_transactions.json`: Enriched data
- `transaction_summary.json`: Aggregated analytics

## Performance Considerations
- Partitioning by date for efficient querying
- Clustering on frequently-queried fields (user_id, category)
- Batch processing for BigQuery writes
- Scalable design adaptable to distributed processing

## Future Enhancements
- Real Apache Beam with distributed runners (Dataflow, Flink)
- Actual GCP integration for production use
- Real-time dashboard visualization
- Advanced analytics (fraud detection, anomaly detection)
- Stream processing with windowing
- External currency rate API integration
- Authentication and authorization

## Dependencies
- apache-beam[gcp]==2.60.0
- pandas==2.1.4
- numpy==1.26.4
- pyyaml==6.0.1
- pytest==7.4.3
- pytest-cov==4.1.0
- python-json-logger==2.0.7

## Security
- CodeQL scan completed: 0 vulnerabilities found
- No hardcoded credentials
- Input validation on all data
- Schema validation before insertion

## License
MIT License

## Author
Satyam Singh
