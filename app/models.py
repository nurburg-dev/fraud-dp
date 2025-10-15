from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum

class FraudType(str, Enum):
    VELOCITY = "velocity"
    GEOGRAPHIC = "geographic"

class EventType(str, Enum):
    USER_LOGIN = "user_login"
    PURCHASE = "purchase"
    ADD_TO_CART = "add_to_cart"
    PAGE_VIEW = "page_view"

class EcommerceEvent(BaseModel):
    event_type: EventType
    user_id: str
    timestamp: int  # Unix timestamp in milliseconds
    ip_address: str
    session_id: str
    user_agent: Optional[str] = None
    amount: Optional[float] = None
    product_id: Optional[str] = None

class FraudDetectionResult(BaseModel):
    user_id: str
    fraud_detected: bool
    fraud_type: FraudType
    timestamp: datetime
    details: Dict[str, Any]

class FraudAlert(BaseModel):
    alert_type: FraudType
    user_id: str
    timestamp: int
    details: Dict[str, Any]
    message: str

class HealthStatus(BaseModel):
    status: str
    service: str
    timestamp: datetime
    kafka_connected: bool
    postgres_connected: bool
    snowflake_connected: bool