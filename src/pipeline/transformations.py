"""
Apache Beam ETL Pipeline Transformations
Custom DoFn and PTransform classes for data processing
"""

import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from datetime import datetime
from typing import Dict, Any, Iterable
import logging

logger = logging.getLogger(__name__)


class ParseJsonMessage(beam.DoFn):
    """Parse JSON messages from Pub/Sub"""
    
    def process(self, element):
        try:
            # Extract data from message
            if isinstance(element, dict):
                if 'data' in element:
                    yield element['data']
                else:
                    yield element
            else:
                yield element
        except Exception as e:
            logger.error(f"Error parsing message: {e}")


class FilterValidTransactions(beam.DoFn):
    """Filter out invalid transactions"""
    
    def process(self, element):
        try:
            # Check if transaction has required fields
            required_fields = ['transaction_id', 'user_id', 'amount', 'status']
            if all(field in element for field in required_fields):
                # Filter out negative amounts
                if element.get('amount', 0) > 0:
                    yield element
        except Exception as e:
            logger.error(f"Error filtering transaction: {e}")


class EnrichTransaction(beam.DoFn):
    """Enrich transaction data with additional information"""
    
    # Category mapping based on merchant
    MERCHANT_CATEGORIES = {
        'amazon': 'Shopping',
        'walmart': 'Shopping',
        'target': 'Shopping',
        'starbucks': 'Food & Beverage',
        'mcdonalds': 'Food & Beverage',
        'shell': 'Gas & Fuel',
        'chevron': 'Gas & Fuel',
        'netflix': 'Entertainment',
        'spotify': 'Entertainment',
        'uber': 'Transportation',
        'lyft': 'Transportation',
    }
    
    # Currency conversion rates (can be overridden from config)
    # Note: In production, these should be fetched from an external API or config
    DEFAULT_CURRENCY_RATES = {
        'USD': 1.0,
        'EUR': 1.08,
        'GBP': 1.27,
        'JPY': 0.0067,
        'CAD': 0.74,
    }
    
    def __init__(self, currency_rates=None):
        self.currency_rates = currency_rates or self.DEFAULT_CURRENCY_RATES
    
    def process(self, element):
        try:
            # Add processing timestamp
            element['processing_timestamp'] = datetime.utcnow().isoformat()
            
            # Categorize transaction based on merchant
            merchant = element.get('merchant', '').lower()
            category = 'Other'
            for key, cat in self.MERCHANT_CATEGORIES.items():
                if key in merchant:
                    category = cat
                    break
            element['category'] = category
            
            # Flag high-value transactions (>1000)
            element['is_high_value'] = element.get('amount', 0) > 1000
            
            # Convert to USD for normalization
            currency = element.get('currency', 'USD')
            amount = element.get('amount', 0)
            rate = self.currency_rates.get(currency, 1.0)
            element['amount_usd'] = round(amount * rate, 2)
            
            yield element
        except Exception as e:
            logger.error(f"Error enriching transaction: {e}")


class AggregateByUserCategory(beam.DoFn):
    """Aggregate transactions by user and category"""
    
    def process(self, element):
        try:
            # Element is a tuple of ((user_id, category), [transactions])
            (user_id, category), transactions = element
            
            amounts = [t.get('amount', 0) for t in transactions]
            
            summary = {
                'user_id': user_id,
                'category': category,
                'total_amount': round(sum(amounts), 2),
                'transaction_count': len(transactions),
                'avg_amount': round(sum(amounts) / len(transactions), 2) if transactions else 0,
                'max_amount': max(amounts) if amounts else 0,
                'min_amount': min(amounts) if amounts else 0,
                'summary_date': datetime.utcnow().date().isoformat(),
            }
            
            yield summary
        except Exception as e:
            logger.error(f"Error aggregating transactions: {e}")


class FormatForBigQuery(beam.DoFn):
    """Format records for BigQuery insertion"""
    
    def process(self, element):
        try:
            # Ensure all timestamp fields are properly formatted
            if 'timestamp' in element and not isinstance(element['timestamp'], str):
                element['timestamp'] = element['timestamp'].isoformat()
            if 'processing_timestamp' in element and not isinstance(element['processing_timestamp'], str):
                element['processing_timestamp'] = element['processing_timestamp'].isoformat()
            
            yield element
        except Exception as e:
            logger.error(f"Error formatting for BigQuery: {e}")


class CollectRecords(beam.DoFn):
    """Collect records for BigQuery insertion"""
    
    def process(self, element):
        yield element


class ExtractUserCategoryKey(beam.DoFn):
    """Extract composite key for grouping"""
    
    def process(self, element):
        try:
            user_id = element.get('user_id')
            category = element.get('category', 'Other')
            yield ((user_id, category), element)
        except Exception as e:
            logger.error(f"Error extracting key: {e}")
