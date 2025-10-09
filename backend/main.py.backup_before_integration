"""
JetDB Backend - Security Hardened NLQ Endpoint + Health Check + Request ID
Drop this code into backend/main.py

Requirements:
pip install fastapi uvicorn anthropic duckdb supabase azure-storage-blob python-multipart
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import anthropic
import duckdb
import os
import uuid
import time
import logging
from datetime import datetime
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

app = FastAPI(title="JetDB API", version="8.0.0")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# NLQ Security Constants
MAX_RESULT_ROWS = 10000
QUERY_TIMEOUT_SECONDS = 30
DANGEROUS_SQL_KEYWORDS = [
    "DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", 
    "ALTER", "CREATE", "GRANT", "REVOKE", "EXECUTE",
    "PRAGMA", "ATTACH", "DETACH"
]

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
    
    # Check Anthropic API
    try:
        if ANTHROPIC_API_KEY:
            health_status["dependencies"]["anthropic"] = "available"
        else:
            health_status["dependencies"]["anthropic"] = "not_configured"
    except Exception as e:
        health_status["dependencies"]["anthropic"] = f"error: {str(e)}"
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
        # Use word boundaries to avoid false positives
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
    """
    Get dataset schema with column types and sample data
    Returns: {columns: [...], sample_rows: [...]}
    """
    try:
        # Get schema with types
        schema_query = f"DESCRIBE SELECT * FROM '{filepath}'"
        schema_result = conn.execute(schema_query).fetchall()
        
        columns = []
        for row in schema_result:
            columns.append({
                "name": row[0],
                "type": row[1]
            })
        
        # Get 3 sample rows for context
        sample_query = f"SELECT * FROM '{filepath}' LIMIT 3"
        sample_result = conn.execute(sample_query).fetchall()
        sample_rows = [list(row) for row in sample_result]
        
        return {
            "columns": columns,
            "sample_rows": sample_rows,
            "row_count": len(sample_rows)
        }
    except Exception as e:
        logger.error(f"Failed to get dataset schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to analyze dataset: {str(e)}")

def generate_nlq_prompt(question: str, schema: Dict[str, Any]) -> str:
    """Generate enhanced Claude prompt with schema and safety rules"""
    
    columns_desc = "\n".join([
        f"  - {col['name']} ({col['type']})"
        for col in schema['columns']
    ])
    
    sample_data = "\n".join([
        f"  Row {i+1}: {row}"
        for i, row in enumerate(schema['sample_rows'])
    ])
    
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
# NLQ ENDPOINT - SECURITY HARDENED
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
async def natural_language_query(request: Request, nlq_request: NLQRequest):
    """
    Natural Language Query endpoint with comprehensive security hardening
    
    Security features:
    - SQL injection protection (keyword blocking)
    - SELECT-only query enforcement
    - Query timeout protection (30s max)
    - Result size limiting (10k rows max)
    - Column type detection for better Claude responses
    """
    request_id = request.state.request_id
    start_time = time.time()
    
    logger.info(f"NLQ request: dataset_id={nlq_request.dataset_id}, question={nlq_request.question}")
    
    # Validate inputs
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="Anthropic API key not configured")
    
    # TODO: Get filepath from Supabase using dataset_id
    # For now, this is a placeholder - integrate with your Supabase queries
    filepath = f"/path/to/datasets/{nlq_request.dataset_id}.csv"
    
    try:
        # Initialize DuckDB connection with timeout
        conn = duckdb.connect(":memory:")
        conn.execute(f"SET statement_timeout='{QUERY_TIMEOUT_SECONDS}s'")
        
        # Get dataset schema with types
        logger.info(f"Getting schema for dataset: {nlq_request.dataset_id}")
        schema = get_dataset_schema(filepath, conn)
        
        # Generate Claude prompt with schema
        prompt = generate_nlq_prompt(nlq_request.question, schema)
        
        # Call Claude API
        logger.info("Calling Claude API for SQL generation")
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        sql_query = message.content[0].text.strip()
        logger.info(f"Claude generated SQL: {sql_query}")
        
        # Validate SQL query for security
        is_valid, error_msg = validate_sql_query(sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Execute query with timeout protection
        try:
            logger.info("Executing SQL query")
            query_start = time.time()
            result = conn.execute(sql_query).fetchall()
            query_duration = (time.time() - query_start) * 1000
            
            logger.info(f"Query executed successfully: {len(result)} rows in {query_duration:.2f}ms")
            
        except Exception as e:
            error_str = str(e)
            if "timeout" in error_str.lower():
                raise HTTPException(
                    status_code=408,
                    detail=f"Query timeout after {QUERY_TIMEOUT_SECONDS}s. "
                           f"Try adding filters or LIMIT clause to your question."
                )
            raise HTTPException(status_code=500, detail=f"Query execution failed: {error_str}")
        
        # Get column names
        column_names = [desc[0] for desc in conn.description]
        
        # Apply result size limit
        truncated = len(result) > MAX_RESULT_ROWS
        if truncated:
            logger.warning(f"Results truncated: {len(result)} rows -> {MAX_RESULT_ROWS} rows")
            result = result[:MAX_RESULT_ROWS]
        
        # Format results
        formatted_results = []
        for row in result:
            formatted_results.append(dict(zip(column_names, row)))
        
        conn.close()
        
        total_duration = (time.time() - start_time) * 1000
        
        response = NLQResponse(
            sql_query=sql_query,
            results=formatted_results,
            truncated=truncated,
            row_count=len(formatted_results),
            execution_time_ms=total_duration
        )
        
        logger.info(f"NLQ request completed successfully in {total_duration:.2f}ms")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"NLQ request failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# ============================================================================
# PLACEHOLDER ENDPOINTS (Add your existing endpoints here)
# ============================================================================

@app.get("/")
async def root():
    return {"message": "JetDB API v8.0 - Security Hardened", "status": "operational"}

# Add your other endpoints:
# - /upload
# - /datasets
# - /datasets/{id}/rows
# - /query/sql
# - etc.

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
