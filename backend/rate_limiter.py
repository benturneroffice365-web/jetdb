# ============================================================================
# FILE 3: backend/rate_limiter.py
# Rate limiting implementation using slowapi
# ============================================================================

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# RATE LIMITER CONFIGURATION
# ============================================================================

def get_user_id_from_request(request: Request) -> str:
    """
    Extract user ID from request for rate limiting
    Falls back to IP address if no user authenticated
    """
    # Try to get user_id from auth token (if implemented)
    user_id = getattr(request.state, "user_id", None)
    
    if user_id:
        return f"user:{user_id}"
    
    # Fall back to IP address
    return f"ip:{get_remote_address(request)}"

# Initialize rate limiter
limiter = Limiter(
    key_func=get_user_id_from_request,
    default_limits=["100/minute"],  # Global default
    storage_uri="memory://",  # Use memory storage (Redis for production)
    strategy="fixed-window"
)

# ============================================================================
# RATE LIMIT CONFIGURATIONS
# ============================================================================

# Upload rate limit: 10 uploads per hour per user
UPLOAD_RATE_LIMIT = "10/hour"

# AI query rate limit: 50 queries per hour per user
AI_QUERY_RATE_LIMIT = "50/hour"

# SQL query rate limit: 200 queries per hour per user
SQL_QUERY_RATE_LIMIT = "200/hour"

# Global API rate limit: 100 requests per minute
GLOBAL_RATE_LIMIT = "100/minute"

# ============================================================================
# CUSTOM RATE LIMIT HANDLER
# ============================================================================

async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> dict:
    """
    Custom handler for rate limit exceeded errors
    Returns user-friendly message with retry information
    """
    request_id = getattr(request.state, "request_id", None)
    user_identifier = get_user_id_from_request(request)
    
    logger.warning(
        f"Rate limit exceeded for {user_identifier} on {request.url.path}",
        extra={
            "request_id": request_id,
            "path": request.url.path,
            "user": user_identifier
        }
    )
    
    # Calculate retry-after in seconds
    retry_after = 3600  # Default 1 hour
    
    if "hour" in str(exc.detail):
        retry_after = 3600
    elif "minute" in str(exc.detail):
        retry_after = 60
    
    return {
        "error": {
            "code": "RATE_LIMIT_EXCEEDED",
            "message": "You have exceeded the rate limit for this endpoint",
            "status": 429,
            "request_id": request_id,
            "details": {
                "retry_after_seconds": retry_after,
                "limit": str(exc.detail)
            }
        }
    }

# ============================================================================
# RATE LIMIT DECORATORS (for endpoints)
# ============================================================================

# Use these decorators on your FastAPI endpoints:
# 
# @app.post("/upload")
# @limiter.limit(UPLOAD_RATE_LIMIT)
# async def upload_file(request: Request, ...):
#     ...
#
# @app.post("/query/natural")
# @limiter.limit(AI_QUERY_RATE_LIMIT)
# async def natural_language_query(request: Request, ...):
#     ...
#
# @app.post("/query/sql")
# @limiter.limit(SQL_QUERY_RATE_LIMIT)
# async def sql_query(request: Request, ...):
#     ...