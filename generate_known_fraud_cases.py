#!/usr/bin/env python3
"""
Generate known fraud cases for DBT validation
Creates CSV seed data with velocity and geographic fraud patterns
"""

import csv
import random
from typing import List, Dict

def generate_known_fraud_cases(output_file: str) -> None:
    """Generate known fraud cases CSV for DBT validation"""
    
    fraud_cases = []
    
    # Generate velocity fraud cases (should be detected)
    velocity_fraud_users = [
        f"velocity_fraud_user_{i:03d}" for i in range(1, 21)  # 20 velocity fraud cases
    ]
    
    for user_id in velocity_fraud_users:
        fraud_cases.append({
            'user_id': user_id,
            'fraud_type': 'velocity_fraud',
            'should_detect': True,
            'description': f'User {user_id} performed >10 purchases in 1 minute',
            'pattern_details': 'High frequency purchasing within 60 seconds'
        })
    
    # Generate geographic fraud cases (should be detected)  
    geographic_fraud_users = [
        f"geo_fraud_user_{i:03d}" for i in range(1, 16)  # 15 geographic fraud cases
    ]
    
    for user_id in geographic_fraud_users:
        fraud_cases.append({
            'user_id': user_id,
            'fraud_type': 'geographic_fraud', 
            'should_detect': True,
            'description': f'User {user_id} logged in from new country and made immediate purchase',
            'pattern_details': 'Login from new country + purchase within 5 minutes'
        })
    
    # Generate normal users (should NOT be detected)
    normal_users = [
        f"normal_user_{i:03d}" for i in range(1, 31)  # 30 normal users
    ]
    
    for user_id in normal_users:
        fraud_cases.append({
            'user_id': user_id,
            'fraud_type': 'normal_user',
            'should_detect': False,
            'description': f'User {user_id} has normal purchase patterns',
            'pattern_details': 'Regular purchase behavior, no fraud indicators'
        })
    
    # Generate edge cases (borderline users for testing)
    edge_case_users = [
        f"edge_case_user_{i:03d}" for i in range(1, 6)  # 5 edge cases
    ]
    
    for user_id in edge_case_users:
        fraud_cases.append({
            'user_id': user_id,
            'fraud_type': 'edge_case',
            'should_detect': False,  # Most edge cases should not be flagged
            'description': f'User {user_id} has borderline behavior patterns',
            'pattern_details': 'Near threshold behavior but within normal limits'
        })
    
    # Write to CSV
    fieldnames = ['user_id', 'fraud_type', 'should_detect', 'description', 'pattern_details']
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(fraud_cases)
    
    print(f"Generated {len(fraud_cases)} known fraud cases:")
    print(f"  - Velocity fraud cases: {len(velocity_fraud_users)}")
    print(f"  - Geographic fraud cases: {len(geographic_fraud_users)}")
    print(f"  - Normal users: {len(normal_users)}")
    print(f"  - Edge cases: {len(edge_case_users)}")
    print(f"  - Total cases: {len(fraud_cases)}")
    print(f"  - Output file: {output_file}")

if __name__ == "__main__":
    output_path = ".nurburgdev/dbt/seeds/known_fraud_cases.csv"
    generate_known_fraud_cases(output_path)
    print(f"\n✅ Known fraud cases generated successfully!")
    print(f"📁 Location: {output_path}")
    print(f"🔍 Use this data for DBT validation testing")