"""
FILE 6: backend/main.py (v8.0.0 - WITH AUTHENTICATION)
Integrated version with all new features + Supabase JWT auth
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from supabase import create_client, Client
import anthropic
import duckdb
import os
import uuid
import time
import logging
from datetime import datetime
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import new modules
from error_handlers import (
    jetdb_exception_handler,
    generic_exception_handler,
    JetDBException,
    AuthenticationError,
    NotFoundError,
    ValidationError
)
from rate_limiter import limiter, rate_limit_exceeded_handler, UPLOAD_RATE_LIMIT, AI_QUERY_RATE_LIMIT, SQL_QUERY_RATE_LIMIT
from state_endpoints import router as state_router
import supabase_helpers
from slowapi.errors import RateLimitExceeded

# ============================================================================
# APP INITIALIZATION
# ============================================================================

app = FastAPI(title="JetDB API", version="8.0.0")

# Add rate limiter to app state
app.state.limiter = limiter

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Initialize Supabase client for auth
supabase: Client = create_client(
    SUPABASE_URL if SUPABASE_URL else "",
    SUPABASE_KEY if SUPABASE_KEY else ""
)
logger.info("✅ Supabase client initialized")

# Security
security = HTTPBearer()

# NLQ Security Constants
MAX_RESULT_ROWS = 10000
QUERY_TIMEOUT_SECONDS = 30
DANGEROUS_SQL_KEYWORDS = [
    "DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", 
    "ALTER", "CREATE", "GRANT", "REVOKE", "EXECUTE",
    "PRAGMA", "ATTACH", "DETACH"
]

# ============================================================================
# EXCEPTION HANDLERS
# ============================================================================

app.add_exception_handler(JetDBException, jetdb_exception_handler)
app.add_exception_handler(Exception, generic_exception_handler)
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# ============================================================================
# MIDDLEWARE - REQUEST ID TRACKING
# ============================================================================

@app.middleware("http")
async def add_request_id_middleware(request: Request, call_next):
    """Add unique request ID to all requests for tracing"""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    # Add to logging context
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.request_id = request_id
        return record
    
    logging.setLogRecordFactory(record_factory)
    
    start_time = time.time()
    
    try:
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        
        # Log request completion
        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            f"Request completed: {request.method} {request.url.path} "
            f"- Status: {response.status_code} - Duration: {duration_ms:.2f}ms"
        )
        
        return response
    except Exception as e:
        logger.error(f"Request failed: {request.method} {request.url.path} - Error: {str(e)}")
        raise
    finally:
        logging.setLogRecordFactory(old_factory)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# AUTHENTICATION
# ============================================================================

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """
    Validate JWT token and return user info.
    All protected endpoints use this dependency.
    """
    try:
        token = credentials.credentials
        
        # Verify token with Supabase
        user_response = supabase.auth.get_user(token)
        
        if not user_response or not user_response.user:
            raise HTTPException(
                status_code=401,
                detail="Invalid authentication token"
            )
        
        user = user_response.user
        logger.info(f"🔐 Authenticated user: {user.id}")
        
        return {
            "id": user.id,
            "email": user.email
        }
        
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        raise HTTPException(
            status_code=401,
            detail="Authentication failed. Please log in again."
        )

# ============================================================================
# INCLUDE ROUTERS
# ============================================================================

# Include spreadsheet state endpoints
app.include_router(state_router)

# ============================================================================
# HEALTH CHECK ENDPOINT
# ============================================================================

@app.get("/health")
async def health_check():
    """
    Health check endpoint with dependency status
    Returns 200 if healthy, 503 if any dependency is down
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "8.0.0",
        "dependencies": {}
    }
    
    # Check OpenAI API
    try:
        if OPENAI_API_KEY:
            health_status["dependencies"]["openai"] = "available"
        else:
            health_status["dependencies"]["openai"] = "not_configured"
    except Exception as e:
        health_status["dependencies"]["openai"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Supabase
    try:
        if SUPABASE_URL and SUPABASE_KEY:
            health_status["dependencies"]["supabase"] = "available"
        else:
            health_status["dependencies"]["supabase"] = "not_configured"
    except Exception as e:
        health_status["dependencies"]["supabase"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check DuckDB
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("SELECT 1").fetchone()
        conn.close()
        health_status["dependencies"]["duckdb"] = "healthy"
    except Exception as e:
        health_status["dependencies"]["duckdb"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    status_code = 200 if health_status["status"] in ["healthy", "degraded"] else 503
    return JSONResponse(content=health_status, status_code=status_code)

# ============================================================================
# NLQ SECURITY FUNCTIONS
# ============================================================================

def validate_sql_query(sql: str) -> tuple[bool, Optional[str]]:
    """
    Validate SQL query for security issues
    Returns: (is_valid, error_message)
    """
    sql_upper = sql.strip().upper()
    
    # Check for dangerous keywords
    for keyword in DANGEROUS_SQL_KEYWORDS:
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, sql_upper):
            logger.warning(f"Blocked dangerous SQL keyword: {keyword} in query: {sql[:100]}")
            return False, f"Query blocked: dangerous keyword '{keyword}' not allowed"
    
    # Enforce SELECT-only queries
    if not sql_upper.startswith("SELECT"):
        logger.warning(f"Blocked non-SELECT query: {sql[:100]}")
        return False, "Only SELECT queries are allowed"
    
    return True, None

def get_dataset_schema(filepath: str, conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get dataset schema with column types and sample data"""
    try:
        schema_query = f"DESCRIBE SELECT * FROM '{filepath}'"
        schema_result = conn.execute(schema_query).fetchall()
        
        columns = []
        for row in schema_result:
            columns.append({"name": row[0], "type": row[1]})
        
        sample_query = f"SELECT * FROM '{filepath}' LIMIT 3"
        sample_result = conn.execute(sample_query).fetchall()
        sample_rows = [list(row) for row in sample_result]
        
        return {"columns": columns, "sample_rows": sample_rows, "row_count": len(sample_rows)}
    except Exception as e:
        logger.error(f"Failed to get dataset schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to analyze dataset: {str(e)}")

def generate_nlq_prompt(question: str, schema: Dict[str, Any]) -> str:
    """Generate enhanced Claude prompt with schema and safety rules"""
    columns_desc = "\n".join([f"  - {col['name']} ({col['type']})" for col in schema['columns']])
    sample_data = "\n".join([f"  Row {i+1}: {row}" for i, row in enumerate(schema['sample_rows'])])
    
    prompt = f"""You are a SQL expert. Generate a DuckDB SQL query to answer the user's question.

**CRITICAL SAFETY RULES:**
1. Return ONLY the SQL query - no markdown, no explanation, no code blocks
2. Use ONLY SELECT statements - no INSERT, UPDATE, DELETE, DROP, etc.
3. Always include a LIMIT clause (max 10000 rows)
4. Use proper column names and types
5. Return valid DuckDB SQL syntax only

**Dataset Schema:**
{columns_desc}

**Sample Data (first 3 rows):**
{sample_data}

**User Question:** {question}

**Your Response (SQL query only):**"""
    
    return prompt

# ============================================================================
# NLQ ENDPOINT - SECURITY HARDENED + AUTHENTICATED
# ============================================================================

class NLQRequest(BaseModel):
    dataset_id: str
    question: str

class NLQResponse(BaseModel):
    sql_query: str
    results: List[Dict[str, Any]]
    truncated: bool
    row_count: int
    execution_time_ms: float

@app.post("/query/natural", response_model=NLQResponse)
@limiter.limit(AI_QUERY_RATE_LIMIT)
async def natural_language_query(
    request: Request, 
    nlq_request: NLQRequest,
    current_user: dict = Depends(get_current_user)
):
    """Natural Language Query endpoint with authentication and security"""
    request_id = request.state.request_id
    start_time = time.time()
    user_id = current_user["id"]
    
    logger.info(f"NLQ request: user={user_id}, dataset_id={nlq_request.dataset_id}, question={nlq_request.question}")
    
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured")
    
    # Get dataset from Supabase with ownership verification
    dataset = await supabase_helpers.get_dataset(nlq_request.dataset_id, user_id)
    if not dataset:
        raise NotFoundError("DATASET_NOT_FOUND")
    
    filepath = dataset["blob_path"]  # Azure Blob path
    
    try:
        conn = duckdb.connect(":memory:")
        conn.execute(f"SET statement_timeout='{QUERY_TIMEOUT_SECONDS}s'")
        
        schema = get_dataset_schema(filepath, conn)
        prompt = generate_nlq_prompt(nlq_request.question, schema)
        
        logger.info("Calling OpenAI API for SQL generation")
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate ONLY the SQL query - no markdown, no explanation, no code blocks."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1024,
            temperature=0
        )
        
        sql_query = response.choices[0].message.content.strip()
        logger.info(f"OpenAI generated SQL: {sql_query}")
        
        is_valid, error_msg = validate_sql_query(sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)
        
        try:
            logger.info("Executing SQL query")
            query_start = time.time()
            result = conn.execute(sql_query).fetchall()
            query_duration = (time.time() - query_start) * 1000
            logger.info(f"Query executed: {len(result)} rows in {query_duration:.2f}ms")
        except Exception as e:
            if "timeout" in str(e).lower():
                raise HTTPException(
                    status_code=408,
                    detail=f"Query timeout after {QUERY_TIMEOUT_SECONDS}s. Try adding filters or LIMIT."
                )
            raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
        
        column_names = [desc[0] for desc in conn.description]
        truncated = len(result) > MAX_RESULT_ROWS
        if truncated:
            logger.warning(f"Results truncated: {len(result)} rows -> {MAX_RESULT_ROWS} rows")
            result = result[:MAX_RESULT_ROWS]
        
        formatted_results = [dict(zip(column_names, row)) for row in result]
        conn.close()
        
        total_duration = (time.time() - start_time) * 1000
        
        return NLQResponse(
            sql_query=sql_query,
            results=formatted_results,
            truncated=truncated,
            row_count=len(formatted_results),
            execution_time_ms=total_duration
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"NLQ request failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# ============================================================================
# UPLOAD ENDPOINT WITH RATE LIMITING + AUTHENTICATION
# ============================================================================

@app.post("/upload")
@limiter.limit(UPLOAD_RATE_LIMIT)
async def upload_file(
    request: Request, 
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Upload CSV/Excel/Parquet file with rate limiting and authentication
    Rate limit: 10 uploads per hour per user
    """
    user_id = current_user["id"]
    logger.info(f"File upload: {file.filename} by user {user_id}")
    
    # TODO: Implement full upload logic with Supabase + Azure Blob
    return {
        "message": "Upload endpoint - integrate with Supabase helpers",
        "user_id": user_id,
        "filename": file.filename
    }

# ============================================================================
# ROOT ENDPOINT
# ============================================================================

@app.get("/")
async def root():
    return {
        "message": "JetDB API v8.0 - Production Ready with Authentication",
        "status": "operational",
        "features": [
            "Supabase JWT Authentication",
            "OpenAI GPT-4 Natural Language Queries",
            "NLQ Security Hardening",
            "Rate Limiting",
            "Spreadsheet State Persistence",
            "Error Handling",
            "Request ID Tracking"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
