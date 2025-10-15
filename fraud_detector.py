#!/usr/bin/env python3
"""
Fraud Detection Template for Assessment
Candidates need to implement the fraud detection logic
"""

import json
import time
from kafka import KafkaConsumer
import redis
from collections import defaultdict
from typing import Dict, List

class FraudDetector:
    def __init__(self):
        # TODO: Initialize Kafka consumer
        # TODO: Initialize Redis connection
        # TODO: Initialize any tracking data structures
        pass
    
    def detect_velocity_fraud(self, user_id: str, timestamp: int) -> bool:
        """
        Detect velocity fraud: >10 purchases in 1 minute from same user
        
        Args:
            user_id: User making the purchase
            timestamp: Purchase timestamp in milliseconds
            
        Returns:
            True if fraud detected, False otherwise
        """
        # TODO: Implement velocity fraud detection
        # Hint: Track purchase timestamps per user
        # Hint: Use sliding window of 1 minute (60,000 ms)
        pass
    
    def detect_geographic_fraud(self, user_id: str, login_ip: str, 
                               purchase_ip: str, time_diff: int) -> bool:
        """
        Detect geographic fraud: login from new country + immediate purchase
        
        Args:
            user_id: User ID
            login_ip: IP address from login event
            purchase_ip: IP address from purchase event  
            time_diff: Time difference between login and purchase (ms)
            
        Returns:
            True if fraud detected, False otherwise
        """
        # TODO: Implement geographic fraud detection
        # Hint: Use Redis to lookup IP -> country mapping
        # Hint: Check if country is new for this user
        # Hint: Define "immediate" threshold (e.g., < 5 minutes)
        pass
    
    def get_country_from_ip(self, ip: str) -> str:
        """
        Helper function to get country from IP address
        
        Args:
            ip: IP address
            
        Returns:
            Country code (US, UK, IN, etc.) or 'UNKNOWN'
        """
        # TODO: Lookup IP in Redis
        # Key format: "ip:{ip_address}" -> country
        pass
    
    def is_new_country_for_user(self, user_id: str, country: str) -> bool:
        """
        Check if this country is new for the user
        
        Args:
            user_id: User ID
            country: Country code
            
        Returns:
            True if new country, False if seen before
        """
        # TODO: Track user's known countries
        # Hint: Use Redis sets for user countries
        pass
    
    def send_fraud_alert(self, alert_type: str, user_id: str, details: Dict):
        """
        Send fraud alert (print to console for assessment)
        
        Args:
            alert_type: 'velocity' or 'geographic'
            user_id: User ID
            details: Additional alert details
        """
        alert = {
            'timestamp': int(time.time() * 1000),
            'type': alert_type,
            'user_id': user_id,
            'details': details
        }
        print(f"🚨 FRAUD ALERT: {json.dumps(alert)}")
    
    def process_events(self):
        """
        Main event processing loop
        """
        # TODO: Setup Kafka consumer for 'ecommerce-events' topic
        # TODO: Process each event based on event_type
        # TODO: Maintain user session state
        # TODO: Apply fraud detection rules
        # TODO: Send alerts when fraud detected
        
        print("Starting fraud detection...")
        print("Listening for events on 'ecommerce-events' topic...")
        
        # Sample consumer setup (candidates need to complete):
        # consumer = KafkaConsumer(
        #     'ecommerce-events',
        #     bootstrap_servers=['localhost:9092'],
        #     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        # )
        # 
        # for message in consumer:
        #     event = message.value
        #     self.handle_event(event)
    
    def handle_event(self, event: Dict):
        """
        Handle individual event
        
        Args:
            event: Parsed event from Kafka
        """
        event_type = event.get('event_type')
        user_id = event.get('user_id')
        timestamp = event.get('timestamp')
        
        # TODO: Route events to appropriate handlers
        # TODO: Track user sessions
        # TODO: Apply fraud rules
        
        if event_type == 'user_login':
            # TODO: Handle login event
            pass
        elif event_type == 'purchase':
            # TODO: Handle purchase event
            # TODO: Check for velocity fraud
            # TODO: Check for geographic fraud
            pass
        elif event_type == 'add_to_cart':
            # TODO: Handle add to cart (optional)
            pass
        elif event_type == 'page_view':
            # TODO: Handle page view (optional)
            pass

def main():
    """
    Main function to run fraud detector
    """
    detector = FraudDetector()
    
    try:
        detector.process_events()
    except KeyboardInterrupt:
        print("\nStopping fraud detector...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()