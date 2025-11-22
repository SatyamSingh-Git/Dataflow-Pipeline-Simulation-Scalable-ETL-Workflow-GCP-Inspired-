# Dataflow Pipeline Simulation – Scalable ETL Workflow (GCP Inspired)

A comprehensive ETL pipeline simulation that mimics Google Cloud Platform's Dataflow, BigQuery, and Pub/Sub services using Python and Apache Beam. This project demonstrates real-time data ingestion, transformation, and analytical querying capabilities.

## 🚀 Features

- **Pub/Sub Simulation**: Real-time message publishing and subscription system
- **Apache Beam ETL Pipeline**: Scalable data processing with transformations
- **BigQuery Data Models**: Structured schemas for efficient analytical querying
- **Data Generation**: Automated sample transaction data generation
- **Multiple Pipeline Stages**:
  - Raw data ingestion
  - Data transformation and enrichment
  - Aggregation and analytics

## 📋 Architecture

```
┌─────────────────┐
│  Data Generator │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Pub/Sub Topic │ ◄── Real-time message publishing
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Subscription   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│        Apache Beam Pipeline             │
│  ┌──────────────────────────────────┐   │
│  │ 1. Parse & Filter                │   │
│  └──────────┬───────────────────────┘   │
│             ▼                            │
│  ┌──────────────────────────────────┐   │
│  │ 2. Enrich & Transform            │   │
│  └──────────┬───────────────────────┘   │
│             ▼                            │
│  ┌──────────────────────────────────┐   │
│  │ 3. Aggregate                     │   │
│  └──────────┬───────────────────────┘   │
└─────────────┼───────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────┐
│          BigQuery Tables                 │
│  ┌────────────────────────────────────┐  │
│  │ • raw_transactions                 │  │
│  │ • processed_transactions           │  │
│  │ • transaction_summary              │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
```

## 📁 Project Structure

```
.
├── config.yaml                 # Configuration file
├── main.py                     # Main application entry point
├── requirements.txt            # Python dependencies
├── src/
│   ├── pubsub_simulation/      # Pub/Sub simulation module
│   │   ├── __init__.py
│   │   └── pubsub.py
│   ├── pipeline/               # Apache Beam pipeline
│   │   ├── __init__.py
│   │   ├── transformations.py  # DoFn transformations
│   │   └── etl_pipeline.py     # Pipeline orchestration
│   ├── bigquery_models/        # BigQuery schemas
│   │   ├── __init__.py
│   │   └── schemas.py
│   └── utils/                  # Utility modules
│       ├── __init__.py
│       ├── data_generator.py
│       └── config_loader.py
├── sql/                        # BigQuery SQL schemas
│   ├── raw_transactions.sql
│   ├── processed_transactions.sql
│   ├── transaction_summary.sql
│   └── analytical_queries.sql
├── data/
│   ├── input/                  # Input data files
│   └── output/                 # Pipeline output
└── tests/                      # Unit tests
```

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/SatyamSingh-Git/Dataflow-Pipeline-Simulation-Scalable-ETL-Workflow-GCP-Inspired-.git
   cd Dataflow-Pipeline-Simulation-Scalable-ETL-Workflow-GCP-Inspired-
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## 🚦 Usage

### Running the Complete Pipeline

```bash
python main.py
```

This will:
1. Initialize Pub/Sub simulator
2. Create BigQuery tables
3. Generate sample transaction data
4. Publish messages to Pub/Sub
5. Run the ETL pipeline (ingestion → transformation → aggregation)
6. Export results to JSON files

### Configuration

Edit `config.yaml` to customize:
- Pipeline settings
- Pub/Sub topic/subscription names
- BigQuery dataset and table names
- Data generation parameters
- Transformation rules

### Example Configuration

```yaml
pipeline:
  name: "etl-pipeline-simulation"
  runner: "DirectRunner"

pubsub:
  topic: "transactions-topic"
  subscription: "transactions-subscription"
  max_messages: 1000

bigquery:
  dataset: "analytics"
  tables:
    raw: "raw_transactions"
    processed: "processed_transactions"
    aggregated: "transaction_summary"

data:
  sample_size: 100
```

## 📊 Pipeline Stages

### 1. Raw Data Ingestion
- Reads messages from Pub/Sub simulation
- Parses JSON data
- Filters valid transactions
- Writes to `raw_transactions` table

### 2. Data Transformation
- Enriches transactions with categories
- Flags high-value transactions (>$1000)
- Converts currencies to USD
- Adds processing timestamps
- Writes to `processed_transactions` table

### 3. Data Aggregation
- Groups by user and category
- Calculates metrics (sum, avg, min, max)
- Generates daily summaries
- Writes to `transaction_summary` table

## 🔍 Example Queries

View sample analytical queries in `sql/analytical_queries.sql`:

```sql
-- Top spending users
SELECT user_id, SUM(total_amount) as total_spent
FROM analytics.transaction_summary
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;

-- High-value transactions
SELECT user_id, merchant, amount_usd, category
FROM analytics.processed_transactions
WHERE is_high_value = TRUE
ORDER BY amount_usd DESC;
```

## 📦 Output

The pipeline generates JSON files in `data/output/`:
- `raw_transactions.json`: Unprocessed transaction data
- `processed_transactions.json`: Enriched and transformed data
- `transaction_summary.json`: Aggregated analytics

## 🧪 Testing

Run tests with pytest:
```bash
pytest tests/ -v
```

## 🔧 Key Components

### Pub/Sub Simulator
- **Topic**: Publish messages
- **Subscription**: Subscribe and pull messages
- **Message Queue**: Thread-safe message handling

### Apache Beam Transformations
- **ParseJsonMessage**: Parse incoming JSON
- **FilterValidTransactions**: Validate and filter data
- **EnrichTransaction**: Add categories and metadata
- **AggregateByUserCategory**: Group and aggregate metrics

### BigQuery Models
- **RawTransactionSchema**: Raw data structure
- **ProcessedTransactionSchema**: Enriched data structure
- **TransactionSummarySchema**: Aggregated analytics structure

## 📈 Performance Considerations

- **Partitioning**: Tables partitioned by date for efficient querying
- **Clustering**: Data clustered by frequently queried fields
- **Batch Processing**: Configurable batch sizes for BigQuery writes
- **Scalable Design**: Can be adapted for distributed processing

## 🎯 Use Cases

- **Financial Analytics**: Transaction monitoring and analysis
- **E-commerce**: Order processing and customer insights
- **IoT Data**: Sensor data aggregation
- **Log Processing**: Application log analysis
- **Real-time Analytics**: Stream processing demonstrations

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License.

## 👨‍💻 Author

**Satyam Singh**
- GitHub: [@SatyamSingh-Git](https://github.com/SatyamSingh-Git)

## 🙏 Acknowledgments

- Inspired by Google Cloud Platform's Dataflow and BigQuery
- Built with Apache Beam for scalable data processing
- Demonstrates ETL best practices and design patterns

---

**Note**: This is a simulation for educational and demonstration purposes. For production GCP workloads, use the actual Google Cloud services.
