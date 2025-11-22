"""
BigQuery Data Models
Defines schemas and table structures for the ETL pipeline
"""

from typing import Dict, Any, List
from datetime import datetime
import json


class TableSchema:
    """Base class for BigQuery table schemas"""
    
    def __init__(self, table_name: str, fields: List[Dict[str, str]]):
        self.table_name = table_name
        self.fields = fields
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'table_name': self.table_name,
            'fields': self.fields
        }
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate a record against the schema"""
        required_fields = [f['name'] for f in self.fields if f.get('mode') == 'REQUIRED']
        return all(field in record for field in required_fields)


class RawTransactionSchema(TableSchema):
    """Schema for raw transaction data"""
    
    def __init__(self):
        fields = [
            {'name': 'transaction_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'currency', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'merchant', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
        ]
        super().__init__('raw_transactions', fields)


class ProcessedTransactionSchema(TableSchema):
    """Schema for processed transaction data with enrichments"""
    
    def __init__(self):
        fields = [
            {'name': 'transaction_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'currency', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'merchant', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'is_high_value', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
            {'name': 'processing_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'amount_usd', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ]
        super().__init__('processed_transactions', fields)


class TransactionSummarySchema(TableSchema):
    """Schema for aggregated transaction summary"""
    
    def __init__(self):
        fields = [
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'category', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'transaction_count', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'avg_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'max_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'min_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'summary_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        ]
        super().__init__('transaction_summary', fields)


class BigQuerySimulator:
    """Simulates BigQuery dataset and table management"""
    
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.tables: Dict[str, TableSchema] = {}
        self.data: Dict[str, List[Dict[str, Any]]] = {}
    
    def create_table(self, schema: TableSchema):
        """Create a table with the given schema"""
        self.tables[schema.table_name] = schema
        self.data[schema.table_name] = []
    
    def insert_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Insert rows into a table"""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        schema = self.tables[table_name]
        valid_rows = [row for row in rows if schema.validate_record(row)]
        self.data[table_name].extend(valid_rows)
        return len(valid_rows)
    
    def query(self, table_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Query rows from a table"""
        if table_name not in self.data:
            return []
        return self.data[table_name][:limit]
    
    def get_table_schema(self, table_name: str) -> TableSchema:
        """Get the schema for a table"""
        return self.tables.get(table_name)
    
    def export_to_json(self, table_name: str, output_file: str):
        """Export table data to JSON file"""
        if table_name not in self.data:
            raise ValueError(f"Table {table_name} does not exist")
        
        with open(output_file, 'w') as f:
            json.dump(self.data[table_name], f, indent=2, default=str)
    
    def get_table_count(self, table_name: str) -> int:
        """Get the number of rows in a table"""
        if table_name not in self.data:
            return 0
        return len(self.data[table_name])
