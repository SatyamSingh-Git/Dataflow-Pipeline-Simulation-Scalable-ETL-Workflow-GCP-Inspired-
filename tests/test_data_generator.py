"""
Unit tests for Data Generator
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils import TransactionDataGenerator


class TestTransactionDataGenerator:
    """Test transaction data generator"""
    
    def test_generator_creation(self):
        gen = TransactionDataGenerator(num_users=10)
        assert gen.num_users == 10
        assert len(gen.user_ids) == 10
    
    def test_generate_transaction(self):
        gen = TransactionDataGenerator()
        txn = gen.generate_transaction()
        
        # Check required fields
        assert 'transaction_id' in txn
        assert 'user_id' in txn
        assert 'amount' in txn
        assert 'currency' in txn
        assert 'merchant' in txn
        assert 'timestamp' in txn
        assert 'status' in txn
        
        # Check data types
        assert isinstance(txn['amount'], float)
        assert txn['amount'] > 0
    
    def test_generate_transactions(self):
        gen = TransactionDataGenerator()
        txns = gen.generate_transactions(10)
        
        assert len(txns) == 10
        assert all('transaction_id' in txn for txn in txns)
    
    def test_generate_streaming_data(self):
        gen = TransactionDataGenerator()
        stream = gen.generate_streaming_data(5)
        
        count = 0
        for txn in stream:
            count += 1
            assert 'transaction_id' in txn
        
        assert count == 5
