# ============================================================================
# FILE 2: backend/error_handlers.py
# Standardized error handling and responses
# ============================================================================

from fastapi import Request, status
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# ERROR CODES AND MESSAGES
# ============================================================================

ERROR_MESSAGES = {
    # Authentication errors (401)
    "AUTH_MISSING_TOKEN": "Authentication token is required",
    "AUTH_INVALID_TOKEN": "Invalid authentication token",
    "AUTH_EXPIRED_TOKEN": "Authentication token has expired",
    "AUTH_INSUFFICIENT_PERMISSIONS": "Insufficient permissions for this action",
    
    # Authorization errors (403)
    "FORBIDDEN_DATASET_ACCESS": "You do not have permission to access this dataset",
    "FORBIDDEN_ACTION": "You are not authorized to perform this action",
    
    # Not found errors (404)
    "DATASET_NOT_FOUND": "Dataset not found",
    "USER_NOT_FOUND": "User not found",
    "STATE_NOT_FOUND": "Spreadsheet state not found",
    
    # Validation errors (400)
    "INVALID_FILE_TYPE": "Invalid file type. Only CSV, Excel, and Parquet files are allowed",
    "INVALID_FILE_SIZE": "File size exceeds maximum limit of 10GB",
    "INVALID_QUERY": "Invalid SQL query",
    "INVALID_DATASET_ID": "Invalid dataset ID format",
    "MISSING_REQUIRED_FIELD": "Required field is missing",
    
    # Rate limiting (429)
    "RATE_LIMIT_EXCEEDED": "Rate limit exceeded. Please try again later",
    "UPLOAD_LIMIT_EXCEEDED": "Upload limit exceeded. Maximum 10 uploads per hour",
    
    # Query errors (400/408)
    "QUERY_TIMEOUT": "Query execution timeout. Try adding filters or LIMIT clause",
    "QUERY_TOO_LARGE": "Query result too large. Maximum 10,000 rows returned",
    "QUERY_BLOCKED": "Query blocked for security reasons",
    
    # Server errors (500)
    "INTERNAL_ERROR": "An internal error occurred. Please try again",
    "DATABASE_ERROR": "Database operation failed",
    "STORAGE_ERROR": "File storage operation failed",
    "EXTERNAL_API_ERROR": "External API call failed",
}

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class JetDBException(Exception):
    """Base exception for JetDB errors"""
    def __init__(
        self,
        error_code: str,
        status_code: int = 500,
        detail: Optional[str] = None,
        **kwargs
    ):
        self.error_code = error_code
        self.status_code = status_code
        self.detail = detail or ERROR_MESSAGES.get(error_code, "An error occurred")
        self.extra = kwargs
        super().__init__(self.detail)

class AuthenticationError(JetDBException):
    """Authentication related errors"""
    def __init__(self, error_code: str = "AUTH_INVALID_TOKEN", **kwargs):
        super().__init__(error_code, status_code=401, **kwargs)

class AuthorizationError(JetDBException):
    """Authorization/permission errors"""
    def __init__(self, error_code: str = "FORBIDDEN_ACTION", **kwargs):
        super().__init__(error_code, status_code=403, **kwargs)

class NotFoundError(JetDBException):
    """Resource not found errors"""
    def __init__(self, error_code: str = "DATASET_NOT_FOUND", **kwargs):
        super().__init__(error_code, status_code=404, **kwargs)

class ValidationError(JetDBException):
    """Input validation errors"""
    def __init__(self, error_code: str = "INVALID_FILE_TYPE", **kwargs):
        super().__init__(error_code, status_code=400, **kwargs)

class RateLimitError(JetDBException):
    """Rate limiting errors"""
    def __init__(self, retry_after: int = 3600, **kwargs):
        super().__init__("RATE_LIMIT_EXCEEDED", status_code=429, retry_after=retry_after, **kwargs)

class QueryError(JetDBException):
    """Query execution errors"""
    def __init__(self, error_code: str = "INVALID_QUERY", **kwargs):
        super().__init__(error_code, status_code=400, **kwargs)

# ============================================================================
# ERROR RESPONSE BUILDER
# ============================================================================

def build_error_response(
    error_code: str,
    status_code: int,
    detail: str,
    request_id: Optional[str] = None,
    **extra
) -> Dict[str, Any]:
    """
    Build standardized error response
    
    Args:
        error_code: Machine-readable error code
        status_code: HTTP status code
        detail: Human-readable error message
        request_id: Request ID for tracing
        **extra: Additional error context
    
    Returns:
        Error response dictionary
    """
    response = {
        "error": {
            "code": error_code,
            "message": detail,
            "status": status_code
        }
    }
    
    if request_id:
        response["error"]["request_id"] = request_id
    
    if extra:
        response["error"]["details"] = extra
    
    return response

# ============================================================================
# EXCEPTION HANDLERS (for FastAPI)
# ============================================================================

async def jetdb_exception_handler(request: Request, exc: JetDBException) -> JSONResponse:
    """Handler for JetDB custom exceptions"""
    request_id = getattr(request.state, "request_id", None)
    
    logger.warning(
        f"JetDB Exception: {exc.error_code} - {exc.detail}",
        extra={
            "request_id": request_id,
            "status_code": exc.status_code,
            "error_code": exc.error_code
        }
    )
    
    response = build_error_response(
        error_code=exc.error_code,
        status_code=exc.status_code,
        detail=exc.detail,
        request_id=request_id,
        **exc.extra
    )
    
    headers = {}
    if isinstance(exc, RateLimitError):
        headers["Retry-After"] = str(exc.extra.get("retry_after", 3600))
    
    return JSONResponse(
        status_code=exc.status_code,
        content=response,
        headers=headers
    )

async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handler for unhandled exceptions"""
    request_id = getattr(request.state, "request_id", None)
    
    logger.error(
        f"Unhandled exception: {str(exc)}",
        exc_info=True,
        extra={"request_id": request_id}
    )
    
    response = build_error_response(
        error_code="INTERNAL_ERROR",
        status_code=500,
        detail="An unexpected error occurred. Please try again later.",
        request_id=request_id
    )
    
    return JSONResponse(
        status_code=500,
        content=response
    )

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_user_friendly_message(error_code: str) -> str:
    """Get user-friendly error message"""
    return ERROR_MESSAGES.get(error_code, "An error occurred. Please try again.")

def is_retryable_error(error_code: str) -> bool:
    """Check if error is retryable"""
    retryable_codes = [
        "DATABASE_ERROR",
        "STORAGE_ERROR",
        "EXTERNAL_API_ERROR",
        "QUERY_TIMEOUT"
    ]
    return error_code in retryable_codes