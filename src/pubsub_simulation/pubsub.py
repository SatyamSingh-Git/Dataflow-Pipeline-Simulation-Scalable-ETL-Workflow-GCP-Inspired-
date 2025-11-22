"""
Pub/Sub Simulation Module
Simulates Google Cloud Pub/Sub for real-time message ingestion
"""

import json
import time
import threading
import queue
from typing import List, Dict, Any, Callable, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Message:
    """Represents a Pub/Sub message"""
    
    def __init__(self, data: Dict[str, Any], message_id: Optional[str] = None):
        self.data = data
        self.message_id = message_id or f"msg-{int(time.time() * 1000)}"
        self.publish_time = datetime.utcnow().isoformat()
        self.attributes = {}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'message_id': self.message_id,
            'data': self.data,
            'publish_time': self.publish_time,
            'attributes': self.attributes
        }


class Topic:
    """Simulates a Pub/Sub Topic"""
    
    def __init__(self, name: str):
        self.name = name
        self.subscriptions: List['Subscription'] = []
        logger.info(f"Created topic: {name}")
    
    def publish(self, message: Message) -> str:
        """Publish a message to all subscriptions"""
        logger.info(f"Publishing message {message.message_id} to topic {self.name}")
        for subscription in self.subscriptions:
            subscription.receive_message(message)
        return message.message_id
    
    def add_subscription(self, subscription: 'Subscription'):
        """Add a subscription to this topic"""
        self.subscriptions.append(subscription)
        logger.info(f"Added subscription {subscription.name} to topic {self.name}")


class Subscription:
    """Simulates a Pub/Sub Subscription"""
    
    def __init__(self, name: str, topic: Topic):
        self.name = name
        self.topic = topic
        self.message_queue = queue.Queue()
        self.topic.add_subscription(self)
        self._running = False
        self._callback = None
        logger.info(f"Created subscription: {name}")
    
    def receive_message(self, message: Message):
        """Receive a message from the topic"""
        self.message_queue.put(message)
    
    def pull(self, max_messages: int = 10) -> List[Message]:
        """Pull messages from the subscription"""
        messages = []
        for _ in range(max_messages):
            try:
                message = self.message_queue.get_nowait()
                messages.append(message)
            except queue.Empty:
                break
        logger.info(f"Pulled {len(messages)} messages from subscription {self.name}")
        return messages
    
    def subscribe(self, callback: Callable[[Message], None]):
        """Subscribe with a callback function for continuous processing"""
        self._callback = callback
        self._running = True
        
        def process_messages():
            while self._running:
                try:
                    message = self.message_queue.get(timeout=1)
                    self._callback(message)
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        thread = threading.Thread(target=process_messages, daemon=True)
        thread.start()
        logger.info(f"Started continuous subscription for {self.name}")
    
    def stop(self):
        """Stop the subscription"""
        self._running = False
        logger.info(f"Stopped subscription {self.name}")


class PubSubSimulator:
    """Main Pub/Sub Simulator class"""
    
    def __init__(self):
        self.topics: Dict[str, Topic] = {}
        self.subscriptions: Dict[str, Subscription] = {}
        logger.info("Initialized Pub/Sub Simulator")
    
    def create_topic(self, topic_name: str) -> Topic:
        """Create a new topic"""
        if topic_name not in self.topics:
            self.topics[topic_name] = Topic(topic_name)
        return self.topics[topic_name]
    
    def get_topic(self, topic_name: str) -> Optional[Topic]:
        """Get an existing topic"""
        return self.topics.get(topic_name)
    
    def create_subscription(self, subscription_name: str, topic_name: str) -> Subscription:
        """Create a new subscription"""
        topic = self.get_topic(topic_name)
        if not topic:
            raise ValueError(f"Topic {topic_name} does not exist")
        
        if subscription_name not in self.subscriptions:
            self.subscriptions[subscription_name] = Subscription(subscription_name, topic)
        return self.subscriptions[subscription_name]
    
    def get_subscription(self, subscription_name: str) -> Optional[Subscription]:
        """Get an existing subscription"""
        return self.subscriptions.get(subscription_name)
    
    def publish_message(self, topic_name: str, data: Dict[str, Any]) -> str:
        """Publish a message to a topic"""
        topic = self.get_topic(topic_name)
        if not topic:
            raise ValueError(f"Topic {topic_name} does not exist")
        
        message = Message(data)
        return topic.publish(message)
