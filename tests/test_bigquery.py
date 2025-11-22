"""
Unit tests for BigQuery Models
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from bigquery_models import (
    RawTransactionSchema,
    ProcessedTransactionSchema,
    TransactionSummarySchema,
    BigQuerySimulator
)


class TestTableSchemas:
    """Test table schema classes"""
    
    def test_raw_transaction_schema(self):
        schema = RawTransactionSchema()
        assert schema.table_name == 'raw_transactions'
        assert len(schema.fields) > 0
    
    def test_processed_transaction_schema(self):
        schema = ProcessedTransactionSchema()
        assert schema.table_name == 'processed_transactions'
        assert len(schema.fields) > 0
    
    def test_transaction_summary_schema(self):
        schema = TransactionSummarySchema()
        assert schema.table_name == 'transaction_summary'
        assert len(schema.fields) > 0
    
    def test_schema_validation(self):
        schema = RawTransactionSchema()
        
        # Valid record
        valid_record = {
            'transaction_id': 'txn-1',
            'user_id': 'user-1',
            'amount': 100.0,
            'currency': 'USD',
            'timestamp': '2024-01-01T00:00:00',
            'status': 'completed'
        }
        assert schema.validate_record(valid_record)
        
        # Invalid record (missing required field)
        invalid_record = {
            'transaction_id': 'txn-1'
        }
        assert not schema.validate_record(invalid_record)


class TestBigQuerySimulator:
    """Test BigQuery simulator"""
    
    def test_create_table(self):
        bq = BigQuerySimulator('test-dataset')
        schema = RawTransactionSchema()
        bq.create_table(schema)
        
        assert 'raw_transactions' in bq.tables
        assert bq.get_table_schema('raw_transactions') == schema
    
    def test_insert_rows(self):
        bq = BigQuerySimulator('test-dataset')
        schema = RawTransactionSchema()
        bq.create_table(schema)
        
        rows = [
            {
                'transaction_id': 'txn-1',
                'user_id': 'user-1',
                'amount': 100.0,
                'currency': 'USD',
                'timestamp': '2024-01-01T00:00:00',
                'status': 'completed'
            }
        ]
        
        count = bq.insert_rows('raw_transactions', rows)
        assert count == 1
        assert bq.get_table_count('raw_transactions') == 1
    
    def test_query_rows(self):
        bq = BigQuerySimulator('test-dataset')
        schema = RawTransactionSchema()
        bq.create_table(schema)
        
        rows = [
            {
                'transaction_id': f'txn-{i}',
                'user_id': 'user-1',
                'amount': 100.0 * i,
                'currency': 'USD',
                'timestamp': '2024-01-01T00:00:00',
                'status': 'completed'
            }
            for i in range(5)
        ]
        
        bq.insert_rows('raw_transactions', rows)
        results = bq.query('raw_transactions', limit=3)
        
        assert len(results) == 3
