"""
Example: Simple ETL Pipeline Run
This example demonstrates a simple run of the pipeline with sample data
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from pubsub_simulation import PubSubSimulator
from bigquery_models import BigQuerySimulator, RawTransactionSchema, ProcessedTransactionSchema, TransactionSummarySchema
from pipeline import DataflowETLPipeline
from utils import TransactionDataGenerator

def main():
    print("="*80)
    print("Simple ETL Pipeline Example")
    print("="*80)
    
    # 1. Setup BigQuery
    print("\n1. Setting up BigQuery simulator...")
    bq = BigQuerySimulator("analytics")
    bq.create_table(RawTransactionSchema())
    bq.create_table(ProcessedTransactionSchema())
    bq.create_table(TransactionSummarySchema())
    
    # 2. Setup Pub/Sub
    print("2. Setting up Pub/Sub simulator...")
    pubsub = PubSubSimulator()
    topic = pubsub.create_topic("transactions")
    subscription = pubsub.create_subscription("transactions-sub", "transactions")
    
    # 3. Generate sample data
    print("3. Generating 50 sample transactions...")
    generator = TransactionDataGenerator(num_users=20)
    transactions = generator.generate_transactions(50)
    
    # 4. Publish to Pub/Sub
    print("4. Publishing messages to Pub/Sub...")
    messages = []
    for txn in transactions:
        msg_id = pubsub.publish_message("transactions", txn)
        messages.append({"message_id": msg_id, "data": txn})
    
    # 5. Run ETL pipeline
    print("5. Running ETL pipeline...")
    pipeline = DataflowETLPipeline(config_path='config.yaml')
    pipeline.run_complete_pipeline(messages, bq)
    
    # 6. Display results
    print("\n" + "="*80)
    print("RESULTS")
    print("="*80)
    print(f"Raw transactions: {bq.get_table_count('raw_transactions')} rows")
    print(f"Processed transactions: {bq.get_table_count('processed_transactions')} rows")
    print(f"Transaction summary: {bq.get_table_count('transaction_summary')} rows")
    
    # Show a sample summary record
    summaries = bq.query('transaction_summary', limit=5)
    print("\nSample aggregations:")
    for i, summary in enumerate(summaries[:5], 1):
        print(f"  {i}. User {summary['user_id']} - {summary['category']}: "
              f"{summary['transaction_count']} transactions, ${summary['total_amount']:.2f} total")
    
    print("\n" + "="*80)
    print("Pipeline completed successfully!")
    print("="*80)

if __name__ == "__main__":
    main()
