#!/usr/bin/env python3
"""
Advanced Event Data Generator for Fraud Detection Assessment
Generates realistic e-commerce events with fraud patterns
"""

import json
import time
import random
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer
from typing import Dict, List, Optional
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EventDataGenerator:
    def __init__(self, kafka_bootstrap_servers: str = 'localhost:9092'):
        """Initialize the event data generator"""
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_bootstrap_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            batch_size=16384,
            linger_ms=10,
            compression_type='gzip'
        )
        
        # IP pools by country for geographic testing
        self.ip_pools = {
            'US': ['192.168.1.1', '10.0.0.1', '172.16.1.1', '198.51.100.1'],
            'UK': ['172.16.0.1', '192.168.100.1', '203.0.113.1', '198.18.0.1'],
            'IN': ['203.192.1.1', '117.239.1.1', '110.227.1.1', '202.131.1.1'],
            'DE': ['85.214.1.1', '217.160.1.1', '62.146.1.1', '91.189.1.1'],
            'CA': ['142.150.1.1', '99.232.1.1', '206.167.1.1', '184.75.1.1']
        }
        
        # Product catalog
        self.products = [
            {'id': 'PROD_001', 'name': 'Laptop', 'price': 1200.00, 'category': 'Electronics'},
            {'id': 'PROD_002', 'name': 'Smartphone', 'price': 800.00, 'category': 'Electronics'},
            {'id': 'PROD_003', 'name': 'Headphones', 'price': 150.00, 'category': 'Electronics'},
            {'id': 'PROD_004', 'name': 'Book', 'price': 25.00, 'category': 'Books'},
            {'id': 'PROD_005', 'name': 'Shoes', 'price': 120.00, 'category': 'Fashion'},
            {'id': 'PROD_006', 'name': 'Watch', 'price': 300.00, 'category': 'Fashion'},
            {'id': 'PROD_007', 'name': 'Gaming Console', 'price': 500.00, 'category': 'Electronics'},
            {'id': 'PROD_008', 'name': 'Tablet', 'price': 400.00, 'category': 'Electronics'},
        ]
        
        # User profiles
        self.users = self._generate_user_profiles(1000)
        
        # Tracking for fraud scenarios
        self.user_sessions = {}
        self.running = False
        
    def _generate_user_profiles(self, count: int) -> List[Dict]:
        """Generate user profiles with spending patterns"""
        users = []
        for i in range(1, count + 1):
            users.append({
                'user_id': f'user_{i:04d}',
                'home_country': random.choice(list(self.ip_pools.keys())),
                'avg_purchase_amount': random.uniform(50, 500),
                'purchase_frequency': random.choice(['low', 'medium', 'high']),
                'device_preference': random.choice(['mobile', 'desktop', 'tablet'])
            })
        return users
    
    def _get_current_timestamp(self) -> int:
        """Get current timestamp in milliseconds"""
        return int(time.time() * 1000)
    
    def _get_user_ip(self, user: Dict, force_country: Optional[str] = None) -> str:
        """Get IP for user, optionally forcing a specific country"""
        country = force_country or user['home_country']
        return random.choice(self.ip_pools[country])
    
    def generate_user_login(self, user: Dict, timestamp: Optional[int] = None, 
                          force_country: Optional[str] = None) -> Dict:
        """Generate user login event"""
        if timestamp is None:
            timestamp = self._get_current_timestamp()
            
        event = {
            'event_type': 'user_login',
            'user_id': user['user_id'],
            'timestamp': timestamp,
            'device': random.choice(['iphone', 'android', 'web', 'tablet']),
            'ip': self._get_user_ip(user, force_country)
        }
        
        # Track user session
        self.user_sessions[user['user_id']] = {
            'login_time': timestamp,
            'country': force_country or user['home_country'],
            'ip': event['ip']
        }
        
        return event
    
    def generate_page_view(self, user: Dict, timestamp: Optional[int] = None) -> Dict:
        """Generate page view event"""
        if timestamp is None:
            timestamp = self._get_current_timestamp()
            
        pages = ['home', 'product_list', 'product_detail', 'cart_view', 'checkout', 'account']
        referrers = ['google.com', 'facebook.com', 'direct', 'bing.com', 'twitter.com']
        
        return {
            'event_type': 'page_view',
            'user_id': user['user_id'],
            'timestamp': timestamp,
            'page': random.choice(pages),
            'referrer': random.choice(referrers)
        }
    
    def generate_add_to_cart(self, user: Dict, timestamp: Optional[int] = None) -> Dict:
        """Generate add to cart event"""
        if timestamp is None:
            timestamp = self._get_current_timestamp()
            
        product = random.choice(self.products)
        
        return {
            'event_type': 'add_to_cart',
            'user_id': user['user_id'],
            'timestamp': timestamp,
            'product_id': product['id'],
            'price': product['price']
        }
    
    def generate_purchase(self, user: Dict, timestamp: Optional[int] = None,
                         amount: Optional[float] = None) -> Dict:
        """Generate purchase event"""
        if timestamp is None:
            timestamp = self._get_current_timestamp()
            
        if amount is None:
            # Generate amount based on user profile
            base_amount = user['avg_purchase_amount']
            amount = round(random.uniform(base_amount * 0.5, base_amount * 2), 2)
        
        # Use session IP if available
        ip = self.user_sessions.get(user['user_id'], {}).get('ip', self._get_user_ip(user))
        
        return {
            'event_type': 'purchase',
            'user_id': user['user_id'],
            'timestamp': timestamp,
            'amount': amount,
            'ip': ip,
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay'])
        }
    
    def send_event(self, event: Dict, topic: str = 'ecommerce-events'):
        """Send event to Kafka topic"""
        try:
            self.producer.send(topic, value=event)
            logger.debug(f"Sent {event['event_type']} for user {event['user_id']}")
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
    
    def generate_normal_traffic(self, events_per_second: int = 100, duration_seconds: int = 300):
        """Generate normal e-commerce traffic"""
        logger.info(f"Generating normal traffic: {events_per_second} events/sec for {duration_seconds}s")
        
        end_time = time.time() + duration_seconds
        event_interval = 1.0 / events_per_second
        
        while time.time() < end_time and self.running:
            user = random.choice(self.users)
            
            # Event distribution: 40% page_view, 25% purchase, 20% add_to_cart, 15% login
            event_type = random.choices(
                ['page_view', 'purchase', 'add_to_cart', 'user_login'],
                weights=[40, 25, 20, 15]
            )[0]
            
            if event_type == 'user_login':
                event = self.generate_user_login(user)
            elif event_type == 'page_view':
                event = self.generate_page_view(user)
            elif event_type == 'add_to_cart':
                event = self.generate_add_to_cart(user)
            else:  # purchase
                event = self.generate_purchase(user)
            
            self.send_event(event)
            time.sleep(event_interval)
    
    def generate_velocity_fraud(self, user_id: str = None, purchases_count: int = 15):
        """Generate velocity fraud pattern: >10 purchases in 1 minute"""
        if user_id is None:
            user_id = f"fraud_velocity_{int(time.time())}"
        
        user = {'user_id': user_id, 'home_country': 'US', 'avg_purchase_amount': 100}
        
        logger.info(f"Generating velocity fraud: {purchases_count} purchases from {user_id}")
        
        base_time = self._get_current_timestamp()
        
        # Generate login first
        login_event = self.generate_user_login(user, base_time)
        self.send_event(login_event)
        
        # Generate rapid purchases within 1 minute
        for i in range(purchases_count):
            purchase_time = base_time + (i * 3000)  # 3 seconds apart
            purchase_event = self.generate_purchase(user, purchase_time, amount=99.99)
            self.send_event(purchase_event)
            time.sleep(0.1)  # Small delay to avoid overwhelming
    
    def generate_geographic_fraud(self, user_id: str = None):
        """Generate geographic anomaly: login from new country + immediate purchase"""
        if user_id is None:
            user_id = f"fraud_geo_{int(time.time())}"
        
        user = {'user_id': user_id, 'home_country': 'US', 'avg_purchase_amount': 200}
        
        logger.info(f"Generating geographic fraud for {user_id}")
        
        # First, establish user's normal location
        normal_login = self.generate_user_login(user)  # US login
        self.send_event(normal_login)
        
        time.sleep(1)
        
        # Login from suspicious new location
        suspicious_time = self._get_current_timestamp()
        suspicious_login = self.generate_user_login(user, suspicious_time, force_country='IN')
        self.send_event(suspicious_login)
        
        # Immediate purchase from new location
        purchase_time = suspicious_time + 30000  # 30 seconds later
        purchase_event = self.generate_purchase(user, purchase_time, amount=1500.00)
        self.send_event(purchase_event)
    
    def generate_mixed_fraud_scenarios(self):
        """Generate multiple fraud scenarios simultaneously"""
        logger.info("Generating mixed fraud scenarios...")
        
        # Velocity fraud
        threading.Thread(target=self.generate_velocity_fraud, 
                        args=("velocity_user_001", 12)).start()
        
        time.sleep(5)
        
        # Geographic fraud
        threading.Thread(target=self.generate_geographic_fraud, 
                        args=("geo_user_001",)).start()
        
        time.sleep(10)
        
        # Another velocity fraud with different pattern
        threading.Thread(target=self.generate_velocity_fraud, 
                        args=("velocity_user_002", 20)).start()
    
    def start_continuous_generation(self, events_per_second: int = 100):
        """Start continuous event generation"""
        self.running = True
        logger.info("Starting continuous event generation...")
        
        # Start normal traffic in background
        normal_traffic_thread = threading.Thread(
            target=self.generate_normal_traffic,
            args=(events_per_second, 3600)  # 1 hour
        )
        normal_traffic_thread.daemon = True
        normal_traffic_thread.start()
        
        # Generate fraud scenarios every 2 minutes
        while self.running:
            time.sleep(120)  # Wait 2 minutes
            self.generate_mixed_fraud_scenarios()
    
    def stop(self):
        """Stop event generation"""
        self.running = False
        self.producer.close()
        logger.info("Event generation stopped")

def main():
    parser = argparse.ArgumentParser(description='Fraud Detection Event Data Generator')
    parser.add_argument('--mode', choices=['normal', 'velocity', 'geographic', 'mixed', 'continuous'],
                       default='normal', help='Generation mode')
    parser.add_argument('--events-per-second', type=int, default=100,
                       help='Events per second for normal traffic')
    parser.add_argument('--duration', type=int, default=300,
                       help='Duration in seconds')
    parser.add_argument('--kafka-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    generator = EventDataGenerator(args.kafka_servers)
    
    try:
        if args.mode == 'normal':
            generator.running = True
            generator.generate_normal_traffic(args.events_per_second, args.duration)
        
        elif args.mode == 'velocity':
            generator.generate_velocity_fraud()
        
        elif args.mode == 'geographic':
            generator.generate_geographic_fraud()
        
        elif args.mode == 'mixed':
            generator.generate_mixed_fraud_scenarios()
        
        elif args.mode == 'continuous':
            generator.start_continuous_generation(args.events_per_second)
            
            # Keep running until interrupted
            try:
                while generator.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
    
    except KeyboardInterrupt:
        logger.info("Stopping event generation...")
    
    finally:
        generator.stop()

if __name__ == "__main__":
    main()