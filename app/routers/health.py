from fastapi import APIRouter
from app.models import HealthStatus
from datetime import datetime

router = APIRouter()

@router.get("/health", response_model=HealthStatus)
async def health_check():
    """Health check endpoint for the fraud detection service"""
    return HealthStatus(
        status="healthy",
        service="fraud-detection-assessment",
        timestamp=datetime.now(),
        kafka_connected=True,  # TODO: Implement actual connectivity checks
        postgres_connected=True,
        snowflake_connected=True
    )