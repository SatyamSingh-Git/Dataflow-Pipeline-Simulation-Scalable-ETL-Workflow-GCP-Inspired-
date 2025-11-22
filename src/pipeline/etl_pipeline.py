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
    CollectRecords,
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
    
    def _apply_transformations(self, data, parser, filter_fn, enricher=None):
        """Apply transformation functions to data"""
        results = []
        
        for item in data:
            # Parse
            parsed_items = list(parser.process(item))
            for parsed in parsed_items:
                # Filter
                filtered_items = list(filter_fn.process(parsed))
                for filtered in filtered_items:
                    # Enrich if provided
                    if enricher:
                        enriched_items = list(enricher.process(filtered))
                        results.extend(enriched_items)
                    else:
                        results.append(filtered)
        
        return results
    
    def run_raw_ingestion_pipeline(self, input_data, bq_simulator):
        """
        Pipeline to ingest raw transaction data
        """
        logger.info("Running raw ingestion pipeline...")
        
        parser = ParseJsonMessage()
        filter_fn = FilterValidTransactions()
        formatter = FormatForBigQuery()
        
        results = self._apply_transformations(input_data, parser, filter_fn)
        
        # Format for BigQuery
        formatted_results = []
        for item in results:
            formatted_items = list(formatter.process(item))
            formatted_results.extend(formatted_items)
        
        # Insert into BigQuery
        if formatted_results:
            count = bq_simulator.insert_rows(self.bq_config['tables']['raw'], formatted_results)
            logger.info(f"Inserted {count} rows into {self.bq_config['tables']['raw']}")
        
        logger.info("Raw ingestion pipeline completed")
        return formatted_results
    
    def run_transformation_pipeline(self, input_data, bq_simulator):
        """
        Pipeline to transform and enrich transaction data
        """
        logger.info("Running transformation pipeline...")
        
        parser = ParseJsonMessage()
        filter_fn = FilterValidTransactions()
        enricher = EnrichTransaction()
        formatter = FormatForBigQuery()
        
        results = self._apply_transformations(input_data, parser, filter_fn, enricher)
        
        # Format for BigQuery
        formatted_results = []
        for item in results:
            formatted_items = list(formatter.process(item))
            formatted_results.extend(formatted_items)
        
        # Insert into BigQuery
        if formatted_results:
            count = bq_simulator.insert_rows(self.bq_config['tables']['processed'], formatted_results)
            logger.info(f"Inserted {count} rows into {self.bq_config['tables']['processed']}")
        
        logger.info("Transformation pipeline completed")
        return formatted_results
    
    def run_aggregation_pipeline(self, input_data, bq_simulator):
        """
        Pipeline to aggregate transaction data
        """
        logger.info("Running aggregation pipeline...")
        
        parser = ParseJsonMessage()
        filter_fn = FilterValidTransactions()
        enricher = EnrichTransaction()
        key_extractor = ExtractUserCategoryKey()
        aggregator = AggregateByUserCategory()
        formatter = FormatForBigQuery()
        
        # Process and enrich data
        enriched = self._apply_transformations(input_data, parser, filter_fn, enricher)
        
        # Extract keys and group
        keyed_data = {}
        for item in enriched:
            key_items = list(key_extractor.process(item))
            for key, value in key_items:
                if key not in keyed_data:
                    keyed_data[key] = []
                keyed_data[key].append(value)
        
        # Aggregate
        aggregated_results = []
        for key, items in keyed_data.items():
            agg_items = list(aggregator.process((key, items)))
            aggregated_results.extend(agg_items)
        
        # Format for BigQuery
        formatted_results = []
        for item in aggregated_results:
            formatted_items = list(formatter.process(item))
            formatted_results.extend(formatted_items)
        
        # Insert into BigQuery
        if formatted_results:
            count = bq_simulator.insert_rows(self.bq_config['tables']['aggregated'], formatted_results)
            logger.info(f"Inserted {count} rows into {self.bq_config['tables']['aggregated']}")
        
        logger.info("Aggregation pipeline completed")
        return formatted_results
    
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
