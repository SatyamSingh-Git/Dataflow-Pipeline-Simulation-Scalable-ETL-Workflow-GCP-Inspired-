"""
Main Application
Orchestrates the entire ETL pipeline simulation
"""

import sys
import os
import logging
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from pubsub_simulation import PubSubSimulator, Message
from bigquery_models import (
    BigQuerySimulator,
    RawTransactionSchema,
    ProcessedTransactionSchema,
    TransactionSummarySchema
)
from pipeline import DataflowETLPipeline
from utils import TransactionDataGenerator, ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_bigquery(config):
    """Initialize BigQuery simulator and create tables"""
    logger.info("Setting up BigQuery simulator...")
    
    dataset_name = config.get_bigquery_config()['dataset']
    bq_simulator = BigQuerySimulator(dataset_name)
    
    # Create tables
    bq_simulator.create_table(RawTransactionSchema())
    bq_simulator.create_table(ProcessedTransactionSchema())
    bq_simulator.create_table(TransactionSummarySchema())
    
    logger.info("BigQuery tables created successfully")
    return bq_simulator


def setup_pubsub(config):
    """Initialize Pub/Sub simulator"""
    logger.info("Setting up Pub/Sub simulator...")
    
    pubsub_config = config.get_pubsub_config()
    topic_name = pubsub_config['topic']
    subscription_name = pubsub_config['subscription']
    
    pubsub = PubSubSimulator()
    topic = pubsub.create_topic(topic_name)
    subscription = pubsub.create_subscription(subscription_name, topic_name)
    
    logger.info(f"Created topic '{topic_name}' and subscription '{subscription_name}'")
    return pubsub, topic, subscription


def generate_sample_data(config):
    """Generate sample transaction data"""
    logger.info("Generating sample transaction data...")
    
    data_config = config.get_data_config()
    sample_size = data_config.get('sample_size', 100)
    
    generator = TransactionDataGenerator(num_users=50)
    transactions = generator.generate_transactions(sample_size)
    
    logger.info(f"Generated {len(transactions)} sample transactions")
    return transactions


def publish_to_pubsub(pubsub, topic_name, transactions):
    """Publish transactions to Pub/Sub"""
    logger.info(f"Publishing {len(transactions)} messages to Pub/Sub...")
    
    messages = []
    for transaction in transactions:
        message_id = pubsub.publish_message(topic_name, transaction)
        messages.append({'message_id': message_id, 'data': transaction})
    
    logger.info(f"Published {len(messages)} messages")
    return messages


def run_pipeline(config, messages, bq_simulator):
    """Run the ETL pipeline"""
    logger.info("Starting ETL pipeline...")
    
    pipeline = DataflowETLPipeline(config_path='config.yaml')
    pipeline.run_complete_pipeline(messages, bq_simulator)
    
    logger.info("ETL pipeline completed successfully")


def display_results(bq_simulator):
    """Display pipeline results"""
    logger.info("\n" + "="*80)
    logger.info("PIPELINE RESULTS")
    logger.info("="*80)
    
    tables = ['raw_transactions', 'processed_transactions', 'transaction_summary']
    
    for table_name in tables:
        count = bq_simulator.get_table_count(table_name)
        logger.info(f"\nTable: {table_name}")
        logger.info(f"Total rows: {count}")
        
        if count > 0:
            sample_rows = bq_simulator.query(table_name, limit=3)
            logger.info(f"Sample data (first 3 rows):")
            for i, row in enumerate(sample_rows, 1):
                logger.info(f"  Row {i}: {row}")
    
    logger.info("\n" + "="*80)


def export_results(bq_simulator, output_dir):
    """Export results to JSON files"""
    logger.info(f"Exporting results to {output_dir}...")
    
    os.makedirs(output_dir, exist_ok=True)
    
    tables = ['raw_transactions', 'processed_transactions', 'transaction_summary']
    for table_name in tables:
        output_file = os.path.join(output_dir, f"{table_name}.json")
        bq_simulator.export_to_json(table_name, output_file)
        logger.info(f"Exported {table_name} to {output_file}")


def main():
    """Main application entry point"""
    logger.info("Starting Dataflow Pipeline Simulation")
    logger.info("="*80)
    
    try:
        # Load configuration
        config = ConfigLoader('config.yaml')
        
        # Setup components
        bq_simulator = setup_bigquery(config)
        pubsub, topic, subscription = setup_pubsub(config)
        
        # Generate and publish data
        transactions = generate_sample_data(config)
        messages = publish_to_pubsub(pubsub, config.get_pubsub_config()['topic'], transactions)
        
        # Run ETL pipeline
        run_pipeline(config, messages, bq_simulator)
        
        # Display and export results
        display_results(bq_simulator)
        output_dir = config.get_data_config().get('output_dir', 'data/output')
        export_results(bq_simulator, output_dir)
        
        logger.info("\n" + "="*80)
        logger.info("Pipeline simulation completed successfully!")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
