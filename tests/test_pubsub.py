"""
Unit tests for Pub/Sub Simulation
"""

import pytest
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pubsub_simulation import PubSubSimulator, Topic, Subscription, Message


class TestMessage:
    """Test Message class"""
    
    def test_message_creation(self):
        data = {'test': 'data'}
        msg = Message(data)
        assert msg.data == data
        assert msg.message_id is not None
        assert msg.publish_time is not None
    
    def test_message_to_dict(self):
        data = {'test': 'data'}
        msg = Message(data, 'test-id')
        msg_dict = msg.to_dict()
        assert msg_dict['data'] == data
        assert msg_dict['message_id'] == 'test-id'


class TestTopic:
    """Test Topic class"""
    
    def test_topic_creation(self):
        topic = Topic('test-topic')
        assert topic.name == 'test-topic'
        assert len(topic.subscriptions) == 0
    
    def test_topic_publish(self):
        topic = Topic('test-topic')
        msg = Message({'test': 'data'})
        message_id = topic.publish(msg)
        assert message_id == msg.message_id


class TestSubscription:
    """Test Subscription class"""
    
    def test_subscription_creation(self):
        topic = Topic('test-topic')
        sub = Subscription('test-sub', topic)
        assert sub.name == 'test-sub'
        assert sub.topic == topic
        assert len(topic.subscriptions) == 1
    
    def test_subscription_pull(self):
        topic = Topic('test-topic')
        sub = Subscription('test-sub', topic)
        
        # Publish messages
        msg1 = Message({'id': 1})
        msg2 = Message({'id': 2})
        topic.publish(msg1)
        topic.publish(msg2)
        
        # Pull messages
        messages = sub.pull(max_messages=10)
        assert len(messages) == 2


class TestPubSubSimulator:
    """Test PubSubSimulator class"""
    
    def test_create_topic(self):
        pubsub = PubSubSimulator()
        topic = pubsub.create_topic('test-topic')
        assert topic.name == 'test-topic'
        assert pubsub.get_topic('test-topic') == topic
    
    def test_create_subscription(self):
        pubsub = PubSubSimulator()
        pubsub.create_topic('test-topic')
        sub = pubsub.create_subscription('test-sub', 'test-topic')
        assert sub.name == 'test-sub'
        assert pubsub.get_subscription('test-sub') == sub
    
    def test_publish_message(self):
        pubsub = PubSubSimulator()
        pubsub.create_topic('test-topic')
        sub = pubsub.create_subscription('test-sub', 'test-topic')
        
        data = {'test': 'message'}
        message_id = pubsub.publish_message('test-topic', data)
        
        messages = sub.pull(max_messages=1)
        assert len(messages) == 1
        assert messages[0].data == data
