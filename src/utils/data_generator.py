"""
Data Generator
Generates sample transaction data for testing
"""

import random
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid


class TransactionDataGenerator:
    """Generate sample transaction data"""
    
    MERCHANTS = [
        'Amazon', 'Walmart', 'Target', 'Starbucks', 'McDonalds',
        'Shell', 'Chevron', 'Netflix', 'Spotify', 'Uber',
        'Lyft', 'Apple Store', 'Best Buy', 'Home Depot', 'Costco'
    ]
    
    CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
    
    LOCATIONS = [
        'New York, NY', 'Los Angeles, CA', 'Chicago, IL',
        'Houston, TX', 'Phoenix, AZ', 'Philadelphia, PA',
        'San Antonio, TX', 'San Diego, CA', 'Dallas, TX',
        'San Jose, CA'
    ]
    
    STATUSES = ['completed', 'pending', 'failed']
    
    def __init__(self, num_users: int = 100):
        self.num_users = num_users
        self.user_ids = [f"user_{i:04d}" for i in range(num_users)]
    
    def generate_transaction(self) -> Dict[str, Any]:
        """Generate a single transaction"""
        return {
            'transaction_id': str(uuid.uuid4()),
            'user_id': random.choice(self.user_ids),
            'amount': round(random.uniform(5.0, 2000.0), 2),
            'currency': random.choice(self.CURRENCIES),
            'merchant': random.choice(self.MERCHANTS),
            'timestamp': (
                datetime.utcnow() - timedelta(
                    days=random.randint(0, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )
            ).isoformat(),
            'location': random.choice(self.LOCATIONS),
            'status': random.choice(self.STATUSES),
        }
    
    def generate_transactions(self, count: int) -> List[Dict[str, Any]]:
        """Generate multiple transactions"""
        return [self.generate_transaction() for _ in range(count)]
    
    def generate_to_file(self, count: int, output_file: str):
        """Generate transactions and save to JSON file"""
        transactions = self.generate_transactions(count)
        with open(output_file, 'w') as f:
            json.dump(transactions, f, indent=2)
        return transactions
    
    def generate_streaming_data(self, count: int):
        """Generate transactions as a stream (generator)"""
        for _ in range(count):
            yield self.generate_transaction()
