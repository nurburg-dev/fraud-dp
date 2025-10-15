#!/usr/bin/env python3
"""
Load IP to location mapping into Redis
"""

import redis
import json

def load_ip_data():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # Simple IP to country mapping (matches data_generator.py)
    ip_countries = {
        '192.168.1.1': 'US',
        '10.0.0.1': 'US', 
        '172.16.1.1': 'US',
        '198.51.100.1': 'US',
        '172.16.0.1': 'UK',
        '192.168.100.1': 'UK',
        '203.0.113.1': 'UK', 
        '198.18.0.1': 'UK',
        '203.192.1.1': 'IN',
        '117.239.1.1': 'IN',
        '110.227.1.1': 'IN',
        '202.131.1.1': 'IN',
        '85.214.1.1': 'DE',
        '217.160.1.1': 'DE',
        '62.146.1.1': 'DE',
        '91.189.1.1': 'DE',
        '142.150.1.1': 'CA',
        '99.232.1.1': 'CA',
        '206.167.1.1': 'CA',
        '184.75.1.1': 'CA'
    }
    
    for ip, country in ip_countries.items():
        r.set(f"ip:{ip}", country)
    
    print(f"Loaded {len(ip_countries)} IP mappings into Redis")

if __name__ == "__main__":
    load_ip_data()