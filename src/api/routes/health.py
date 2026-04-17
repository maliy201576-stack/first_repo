"""Health check endpoint."""

from fastapi import APIRouter

from src.common.models import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Return service health status."""
    return HealthResponse(
        status="ok",
        service_name="glukhov-sales-engine-api",
        last_successful_cycle=None,
    )
