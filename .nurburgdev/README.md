---
title: "Real-time Fraud Detection Assessment"
author: "Nurburg Team"
authorLink: "https://nurburg.dev"
authorTitle: "Platform Engineers"
summary: "Build a real-time fraud detection system that processes e-commerce events and identifies fraudulent patterns using data engineering best practices"
publishedOn: 2024-01-15
published: true
tags:
  - kafka
  - snowflake
  - postgresql
  - dbt
  - fraud-detection
---

# Real-time Fraud Detection Assessment

## Overview

You are tasked with implementing a **real-time fraud detection system** for an e-commerce platform. This challenge tests your ability to:

- **Stream Processing**: Consume events from Kafka in real-time
- **Data Warehousing**: Store and process events in Snowflake
- **Fraud Detection Logic**: Implement algorithms to detect fraudulent patterns
- **Data Engineering**: Build reliable data pipelines and validation systems

## Challenge Architecture

## Implementation Requirements

### 1. Kafka Event Consumer

**Your Task**: Implement a Python consumer that reads events from the `ecommerce-events` Kafka topic.

**Event Types You'll Receive**:
```json
{
  "event_type": "user_login",
  "user_id": "user_12345",
  "timestamp": 1672531200000,
  "ip_address": "192.168.1.1",
  "user_agent": "Mozilla/5.0...",
  "session_id": "sess_abcd1234"
}

{
  "event_type": "purchase", 
  "user_id": "user_12345",
  "timestamp": 1672531260000,
  "ip_address": "192.168.1.1",
  "amount": 299.99,
  "product_id": "PROD_001",
  "session_id": "sess_abcd1234"
}
```

### 2. Snowflake Data Warehouse

**Your Task**: Store all events in Snowflake for analysis and fraud detection.

**Expected Snowflake Schema**:
```sql
-- Database: FRAUD_DETECTION
-- Schema: EVENTS

CREATE TABLE user_events (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    timestamp TIMESTAMP,
    ip_address STRING,
    session_id STRING,
    amount FLOAT,
    product_id STRING,
    user_agent STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

**Connection Details**:
- **Database**: `FRAUD_DETECTION`
- **Schema**: `EVENTS`
- **Warehouse**: `COMPUTE_WH`

### 3. Fraud Detection Logic

**Your Task**: Implement detection algorithms for these fraud patterns:

#### A. Velocity Fraud Detection
- **Rule**: More than 10 purchases in 1 minute from the same user
- **Implementation**: Track purchase timestamps per user using sliding windows
- **Expected Detection**: Flag users with rapid-fire purchasing behavior

#### B. Geographic Fraud Detection  
- **Rule**: User logs in from a new country and makes a purchase within 5 minutes
- **Implementation**: 
  - Track user's known countries from previous login history
  - Detect logins from new geographic locations
  - Flag immediate purchases after geographic anomalies
- **Expected Detection**: Flag suspicious location-based patterns

- **Good luck!** This challenge tests real-world data engineering skills used in production fraud detection systems. Focus on accuracy, reliability, and scalable design patterns.