"""
Main ETL Pipeline
Apache Beam pipeline for processing transaction data
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime
import logging
import yaml

from .transformations import (
    ParseJsonMessage,
    FilterValidTransactions,
    EnrichTransaction,
    AggregateByUserCategory,
    FormatForBigQuery,
    WriteToBigQuery,
    ExtractUserCategoryKey
)

logger = logging.getLogger(__name__)


class DataflowETLPipeline:
    """Main ETL Pipeline class"""
    
    def __init__(self, config_path: str = 'config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.pipeline_config = self.config.get('pipeline', {})
        self.bq_config = self.config.get('bigquery', {})
        
    def create_pipeline_options(self):
        """Create pipeline options"""
        options = PipelineOptions()
        options.view_as(StandardOptions).runner = self.pipeline_config.get('runner', 'DirectRunner')
        return options
    
    def run_raw_ingestion_pipeline(self, input_data, bq_simulator):
        """
        Pipeline to ingest raw transaction data
        
        Steps:
        1. Read from Pub/Sub simulation
        2. Parse JSON messages
        3. Filter valid transactions
        4. Write to raw_transactions table
        """
        options = self.create_pipeline_options()
        
        with beam.Pipeline(options=options) as pipeline:
            (
                pipeline
                | 'Create Input' >> beam.Create(input_data)
                | 'Parse Messages' >> beam.ParDo(ParseJsonMessage())
                | 'Filter Valid' >> beam.ParDo(FilterValidTransactions())
                | 'Format for BQ' >> beam.ParDo(FormatForBigQuery())
                | 'Write Raw Data' >> beam.ParDo(
                    WriteToBigQuery(
                        bq_simulator,
                        self.bq_config['tables']['raw']
                    )
                )
            )
        
        logger.info("Raw ingestion pipeline completed")
    
    def run_transformation_pipeline(self, input_data, bq_simulator):
        """
        Pipeline to transform and enrich transaction data
        
        Steps:
        1. Read from raw transactions
        2. Enrich with categories and flags
        3. Write to processed_transactions table
        """
        options = self.create_pipeline_options()
        
        with beam.Pipeline(options=options) as pipeline:
            (
                pipeline
                | 'Create Input' >> beam.Create(input_data)
                | 'Parse Messages' >> beam.ParDo(ParseJsonMessage())
                | 'Filter Valid' >> beam.ParDo(FilterValidTransactions())
                | 'Enrich Data' >> beam.ParDo(EnrichTransaction())
                | 'Format for BQ' >> beam.ParDo(FormatForBigQuery())
                | 'Write Processed Data' >> beam.ParDo(
                    WriteToBigQuery(
                        bq_simulator,
                        self.bq_config['tables']['processed']
                    )
                )
            )
        
        logger.info("Transformation pipeline completed")
    
    def run_aggregation_pipeline(self, input_data, bq_simulator):
        """
        Pipeline to aggregate transaction data
        
        Steps:
        1. Read from processed transactions
        2. Group by user and category
        3. Aggregate metrics
        4. Write to transaction_summary table
        """
        options = self.create_pipeline_options()
        
        with beam.Pipeline(options=options) as pipeline:
            (
                pipeline
                | 'Create Input' >> beam.Create(input_data)
                | 'Parse Messages' >> beam.ParDo(ParseJsonMessage())
                | 'Filter Valid' >> beam.ParDo(FilterValidTransactions())
                | 'Enrich Data' >> beam.ParDo(EnrichTransaction())
                | 'Extract Key' >> beam.ParDo(ExtractUserCategoryKey())
                | 'Group By Key' >> beam.GroupByKey()
                | 'Aggregate' >> beam.ParDo(AggregateByUserCategory())
                | 'Format for BQ' >> beam.ParDo(FormatForBigQuery())
                | 'Write Summary' >> beam.ParDo(
                    WriteToBigQuery(
                        bq_simulator,
                        self.bq_config['tables']['aggregated']
                    )
                )
            )
        
        logger.info("Aggregation pipeline completed")
    
    def run_complete_pipeline(self, input_data, bq_simulator):
        """
        Run the complete ETL pipeline with all stages
        """
        logger.info("Starting complete ETL pipeline")
        
        # Stage 1: Raw ingestion
        logger.info("Stage 1: Raw data ingestion")
        self.run_raw_ingestion_pipeline(input_data, bq_simulator)
        
        # Stage 2: Transformation
        logger.info("Stage 2: Data transformation and enrichment")
        self.run_transformation_pipeline(input_data, bq_simulator)
        
        # Stage 3: Aggregation
        logger.info("Stage 3: Data aggregation")
        self.run_aggregation_pipeline(input_data, bq_simulator)
        
        logger.info("Complete ETL pipeline finished")
