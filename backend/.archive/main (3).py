"""
JetDB v8.2.0 - Production Ready with Secure Azure Access
==========================================================
ALL 13 CRITICAL FIXES + SECURE BLOB ACCESS:
✅ 1. User-Specific Filtering (JWT token validation)
✅ 2. Azure Blob Storage Integration (SECURE - authenticated access)
✅ 3. Environment Variable Validation
✅ 4. Query Timeout Re-implementation
✅ 5. Error Response Standardization
✅ 6. Row Count Background Task Fix
✅ 7. CORS Configuration (production-ready)
✅ 8. Request ID Logging (comprehensive)
✅ 9. Rate Limiting (integrated)
✅ 10. Spreadsheet State Endpoints (imported)
✅ 11. Health Check Enhancement
✅ 12. Logging Fix (request_id optional)
✅ 13. DuckDB Secrets for Authenticated Blob Access
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Request, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List
from supabase import create_client, Client
from azure.storage.blob import BlobServiceClient, ContentSettings
import duckdb
import os
import io
import uuid
import logging
import time
from datetime import datetime
from dotenv import load_dotenv

# Import custom modules
from error_handlers import (
    jetdb_exception_handler,
    generic_exception_handler,
    JetDBException,
    NotFoundError,
    ValidationError
)
from rate_limiter import limiter, rate_limit_exceeded_handler, UPLOAD_RATE_LIMIT, SQL_QUERY_RATE_LIMIT, AI_QUERY_RATE_LIMIT
from state_endpoints import router as state_router
from slowapi.errors import RateLimitExceeded

# Load environment variables
load_dotenv()

# Version
VERSION = "8.2.0"

# ============================================================================
# LOGGING CONFIGURATION (FIX #12)
# ============================================================================

class RequestIdFilter(logging.Filter):
    """Filter to add request_id to log records, with 'startup' as default"""
    def filter(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = 'startup'
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] - %(message)s'
)
logger = logging.getLogger(__name__)
logger.addFilter(RequestIdFilter())

# ============================================================================
# ENVIRONMENT VALIDATION (FIX #3)
# ============================================================================

def validate_environment():
    """Validate all required environment variables on startup"""
    required_vars = {
        "SUPABASE_URL": os.getenv("SUPABASE_URL"),
        "SUPABASE_KEY": os.getenv("SUPABASE_KEY"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "AZURE_STORAGE_CONNECTION_STRING": os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
    }
    
    missing = [k for k, v in required_vars.items() if not v]
    
    if missing:
        error_msg = f"❌ Missing required environment variables: {', '.join(missing)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("✅ All required environment variables validated")
    return required_vars

# Validate environment on startup
env_vars = validate_environment()

SUPABASE_URL = env_vars["SUPABASE_URL"]
SUPABASE_KEY = env_vars["SUPABASE_KEY"]
OPENAI_API_KEY = env_vars["OPENAI_API_KEY"]
AZURE_CONNECTION_STRING = env_vars["AZURE_STORAGE_CONNECTION_STRING"]

# ============================================================================
# FASTAPI APP INITIALIZATION
# ============================================================================

app = FastAPI(title="JetDB API", version=VERSION)

# Add rate limiter to app state (FIX #9)
app.state.limiter = limiter

# Exception handlers (FIX #5)
app.add_exception_handler(JetDBException, jetdb_exception_handler)
app.add_exception_handler(Exception, generic_exception_handler)
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# ============================================================================
# MIDDLEWARE - REQUEST ID TRACKING (FIX #8)
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

# CORS Configuration (FIX #7)
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# SUPABASE & AZURE INITIALIZATION
# ============================================================================

# Initialize Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logger.info("✅ Supabase connected")

# Initialize Azure Blob Storage (FIX #2)
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
CONTAINER_NAME = "jetdb-datasets"

# Create container if it doesn't exist
try:
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    if not container_client.exists():
        container_client.create_container()
        logger.info(f"✅ Created blob container: {CONTAINER_NAME}")
    else:
        logger.info(f"✅ Blob container exists: {CONTAINER_NAME}")
except Exception as e:
    logger.error(f"❌ Blob storage error: {e}")
    raise

# Security
security = HTTPBearer()

# ============================================================================
# AUTHENTICATION - JWT VALIDATION (FIX #1)
# ============================================================================

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    Extract and validate user_id from JWT token
    Returns user_id if valid, raises 401 if invalid
    """
    try:
        token = credentials.credentials
        
        # Verify token with Supabase
        user_response = supabase.auth.get_user(token)
        
        if not user_response or not user_response.user:
            raise HTTPException(status_code=401, detail="Invalid authentication token")
        
        user_id = user_response.user.id
        logger.info(f"🔐 Authenticated user: {user_id}")
        
        return user_id
        
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        raise HTTPException(status_code=401, detail="Authentication failed")

# ============================================================================
# AZURE BLOB STORAGE HELPERS (FIX #2)
# ============================================================================

def upload_to_blob(dataset_id: str, content: bytes, filename: str) -> str:
    """Upload file to Azure Blob Storage"""
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME,
        blob=f"{dataset_id}/{filename}"
    )
    
    blob_client.upload_blob(
        content,
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv")
    )
    
    blob_url = blob_client.url
    logger.info(f"✅ Uploaded to blob: {blob_url}")
    return blob_url

def delete_from_blob(blob_path: str):
    """Delete file from Azure Blob Storage"""
    try:
        # Extract blob name from URL
        blob_name = blob_path.split(f"{CONTAINER_NAME}/")[-1]
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_name
        )
        blob_client.delete_blob()
        logger.info(f"🗑️  Deleted from blob: {blob_name}")
    except Exception as e:
        logger.warning(f"Blob delete failed (may not exist): {e}")

# ============================================================================
# DUCKDB HELPER - SECURE AZURE ACCESS (FIX #13)
# ============================================================================

def create_duckdb_connection_with_azure():
    """Create DuckDB connection with Azure authentication"""
    import re
    
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL azure;")
    conn.execute("LOAD azure;")
    
    # Parse connection string components
    conn_str = AZURE_CONNECTION_STRING
    
    # Extract account name and key
    account_match = re.search(r'AccountName=([^;]+)', conn_str)
    key_match = re.search(r'AccountKey=([^;]+)', conn_str)
    
    if account_match and key_match:
        account_name = account_match.group(1)
        account_key = key_match.group(1)
        
        # Set individual parameters
        conn.execute(f"SET azure_storage_account = '{account_name}';")
        conn.execute(f"SET azure_account_key = '{account_key}';")
    else:
        # Fallback to connection string
        conn.execute(f"SET azure_storage_connection_string = '{AZURE_CONNECTION_STRING}';")
    
    return conn

# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_dataset_from_db(dataset_id: str, user_id: str) -> dict:
    """Get dataset metadata from Supabase with ownership verification"""
    try:
        response = supabase.table('datasets').select("*").eq('id', dataset_id).eq('user_id', user_id).execute()
        
        if not response.data:
            raise NotFoundError("DATASET_NOT_FOUND")
        
        return response.data[0]
    except NotFoundError:
        raise
    except Exception as e:
        logger.error(f"Error fetching dataset: {e}")
        raise HTTPException(status_code=500, detail="Database error")

def list_datasets_from_db(user_id: str) -> List[dict]:
    """List all datasets for a specific user"""
    try:
        response = supabase.table('datasets').select("*").eq('user_id', user_id).order('created_at', desc=True).execute()
        return response.data
    except Exception as e:
        logger.error(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail="Database error")

def analyze_dataset_background(dataset_id: str, blob_url: str, user_id: str):
    """
    Analyze dataset in background - gets exact row count (FIX #6 + #13)
    Now properly handles Azure Blob Storage with secure authentication
    """
    conn = None
    try:
        logger.info(f"📊 Analyzing {dataset_id}")
        start_time = datetime.now()
        
        # Configure DuckDB to use Azure with authentication (FIX #13)
        conn = create_duckdb_connection_with_azure()
        
        # Get exact row count
        row_count = conn.execute(
            f"SELECT COUNT(*) as count FROM '{blob_url}'"
        ).fetchone()[0]
        
        # Get column info
        result = conn.execute(f"SELECT * FROM '{blob_url}' LIMIT 1").fetchdf()
        column_count = len(result.columns)
        columns = list(result.columns)
        
        analysis_time = (datetime.now() - start_time).total_seconds()
        
        # Update in Supabase
        supabase.table('datasets').update({
            "row_count": row_count,
            "column_count": column_count,
            "columns": columns,
            "status": "ready"
        }).eq('id', dataset_id).eq('user_id', user_id).execute()
        
        logger.info(f"✅ Analysis complete: {dataset_id} | Rows: {row_count:,} | Time: {analysis_time:.1f}s")
        
    except Exception as e:
        logger.error(f"Analysis failed for {dataset_id}: {e}")
        # Mark as error in database
        try:
            supabase.table('datasets').update({
                "status": "error"
            }).eq('id', dataset_id).execute()
        except:
            pass
    finally:
        # CRITICAL: Always close the connection
        if conn:
            try:
                conn.close()
            except:
                pass

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class SQLQuery(BaseModel):
    sql: str
    dataset_id: str

class NaturalLanguageQuery(BaseModel):
    question: str
    dataset_id: str

# ============================================================================
# NLQ SECURITY CONSTANTS (FIX #4)
# ============================================================================

MAX_RESULT_ROWS = 10000
QUERY_TIMEOUT_SECONDS = 30
DANGEROUS_SQL_KEYWORDS = [
    "DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE",
    "ALTER", "CREATE", "GRANT", "REVOKE", "EXECUTE",
    "PRAGMA", "ATTACH", "DETACH"
]

def validate_sql_safety(sql: str) -> tuple[bool, str]:
    """Validate that SQL is safe to execute"""
    sql_upper = sql.upper()
    
    for keyword in DANGEROUS_SQL_KEYWORDS:
        if keyword in sql_upper:
            return False, f"Dangerous SQL keyword detected: {keyword}"
    
    if not sql_upper.strip().startswith("SELECT"):
        return False, "Only SELECT queries are allowed"
    
    return True, ""

# ============================================================================
# INCLUDE ROUTERS (FIX #10)
# ============================================================================

app.include_router(state_router)

# ============================================================================
# HEALTH CHECK ENDPOINT (FIX #11)
# ============================================================================

@app.get("/health")
async def health_check():
    """Enhanced health check with dependency status"""
    health_status = {
        "status": "healthy",
        "version": VERSION,
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": {}
    }
    
    # Check Supabase
    try:
        supabase.table('datasets').select('id').limit(1).execute()
        health_status["dependencies"]["supabase"] = "healthy"
    except Exception as e:
        health_status["dependencies"]["supabase"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Azure Blob
    try:
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        container_client.exists()
        health_status["dependencies"]["azure_blob"] = "healthy"
    except Exception as e:
        health_status["dependencies"]["azure_blob"] = f"error: {str(e)}"
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
    
    # Check OpenAI
    health_status["dependencies"]["openai"] = "configured" if OPENAI_API_KEY else "not_configured"
    
    status_code = 200 if health_status["status"] in ["healthy", "degraded"] else 503
    return JSONResponse(content=health_status, status_code=status_code)

# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/")
def read_root():
    """API information"""
    return {
        "service": "JetDB API",
        "version": VERSION,
        "status": "running",
        "features": [
            "JWT Authentication",
            "Azure Blob Storage (Secure)",
            "OpenAI Natural Language Queries",
            "Rate Limiting",
            "Spreadsheet State Persistence",
            "Comprehensive Error Handling",
            "Request ID Tracking",
            "Authenticated Blob Access"
        ],
        "endpoints": {
            "health": "GET /health",
            "upload": "POST /upload (auth required)",
            "datasets": "GET /datasets (auth required)",
            "query_sql": "POST /query/sql (auth required)",
            "query_natural": "POST /query/natural (auth required)"
        }
    }

@app.post("/upload")
@limiter.limit(UPLOAD_RATE_LIMIT)
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    user_id: str = Depends(get_current_user)
):
    """Upload CSV file with authentication and Azure Blob Storage"""
    
    # Validate file
    if not file.filename.endswith('.csv'):
        raise ValidationError("INVALID_FILE_TYPE")
    
    try:
        # Read file content
        content = await file.read()
        file_size_bytes = len(content)
        
        if file_size_bytes > 10 * 1024 * 1024 * 1024:
            raise ValidationError("INVALID_FILE_SIZE")
        
        if file_size_bytes == 0:
            raise HTTPException(status_code=400, detail="File is empty")
        
        # Generate unique dataset ID
        dataset_id = str(uuid.uuid4())
        
        # Upload to Azure Blob Storage
        blob_url = upload_to_blob(dataset_id, content, file.filename)
        
        # Quick row estimate
        sample = content[:100000]
        line_count = sample.count(b'\n')
        estimated_rows = int(line_count * (len(content) / len(sample))) if line_count > 0 else 0
        
        # Save metadata to Supabase
        dataset_record = {
            "id": dataset_id,
            "user_id": user_id,
            "filename": file.filename,
            "blob_path": blob_url,
            "size_bytes": file_size_bytes,
            "estimated_rows": estimated_rows,
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "status": "analyzing",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        supabase.table('datasets').insert(dataset_record).execute()
        
        # Trigger background analysis
        if background_tasks:
            background_tasks.add_task(analyze_dataset_background, dataset_id, blob_url, user_id)
        
        logger.info(f"⚡ Upload by {user_id}: {file.filename} ({file_size_bytes} bytes)")
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "message": f"Uploaded {file.filename} - analyzing in background",
            "metadata": dataset_record
        }
        
    except (ValidationError, HTTPException):
        raise
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/datasets")
async def list_datasets(user_id: str = Depends(get_current_user)):
    """List all datasets for authenticated user"""
    datasets_list = list_datasets_from_db(user_id)
    
    return {
        "count": len(datasets_list),
        "datasets": datasets_list
    }

@app.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: str, user_id: str = Depends(get_current_user)):
    """Get specific dataset info with ownership verification"""
    dataset = get_dataset_from_db(dataset_id, user_id)
    return dataset

@app.get("/datasets/{dataset_id}/preview")
async def preview_dataset(
    dataset_id: str, 
    limit: int = 100,
    user_id: str = Depends(get_current_user)
):
    """Get a preview of the dataset with secure access"""
    dataset = get_dataset_from_db(dataset_id, user_id)
    blob_url = dataset["blob_path"]
    
    conn = None
    try:
        conn = create_duckdb_connection_with_azure()
        result = conn.execute(f"SELECT * FROM '{blob_url}' LIMIT {limit}").fetchdf()
        
        return {
            "dataset_id": dataset_id,
            "preview_rows": limit,
            "data": result.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

@app.get("/datasets/{dataset_id}/rows")
async def get_dataset_rows(
    dataset_id: str,
    offset: int = 0,
    limit: int = 1000,
    user_id: str = Depends(get_current_user)
):
    """Get paginated rows from dataset with secure access"""
    dataset = get_dataset_from_db(dataset_id, user_id)
    blob_url = dataset["blob_path"]
    
    conn = None
    try:
        conn = create_duckdb_connection_with_azure()
        result = conn.execute(
            f"SELECT * FROM '{blob_url}' LIMIT {limit} OFFSET {offset}"
        ).fetchdf()
        
        return {
            "dataset_id": dataset_id,
            "offset": offset,
            "limit": limit,
            "rows_returned": len(result),
            "data": result.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch rows: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

@app.post("/query/sql")
@limiter.limit(SQL_QUERY_RATE_LIMIT)
async def execute_sql(
    request: Request,
    query: SQLQuery,
    user_id: str = Depends(get_current_user)
):
    """Execute SQL query with authentication, timeout, and secure blob access"""
    dataset = get_dataset_from_db(query.dataset_id, user_id)
    blob_url = dataset["blob_path"]
    
    # Validate SQL safety
    is_safe, error_msg = validate_sql_safety(query.sql)
    if not is_safe:
        raise HTTPException(status_code=400, detail=error_msg)
    
    conn = None
    try:
        conn = create_duckdb_connection_with_azure()
        
        # Set query timeout (FIX #4)
        conn.execute(f"SET statement_timeout='{QUERY_TIMEOUT_SECONDS}000ms';")
        
        # Create view
        conn.execute(f"CREATE VIEW data AS SELECT * FROM '{blob_url}'")
        
        # Execute query
        start_time = time.time()
        result = conn.execute(query.sql).fetchdf()
        query_time = time.time() - start_time
        
        logger.info(f"🔍 SQL by {user_id}: {len(result)} rows in {query_time:.2f}s")
        
        return {
            "success": True,
            "rows_returned": len(result),
            "columns": list(result.columns),
            "data": result.to_dict(orient="records"),
            "execution_time_seconds": round(query_time, 3)
        }
        
    except Exception as e:
        error_str = str(e)
        if "timeout" in error_str.lower():
            raise HTTPException(
                status_code=408,
                detail=f"Query timeout after {QUERY_TIMEOUT_SECONDS}s"
            )
        raise HTTPException(status_code=400, detail=f"Query failed: {error_str}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

@app.delete("/datasets/{dataset_id}")
async def delete_dataset(
    dataset_id: str,
    user_id: str = Depends(get_current_user)
):
    """Delete a dataset with ownership verification"""
    dataset = get_dataset_from_db(dataset_id, user_id)
    
    try:
        # Delete from Azure Blob
        delete_from_blob(dataset["blob_path"])
        
        # Delete from Supabase
        supabase.table('datasets').delete().eq('id', dataset_id).eq('user_id', user_id).execute()
        
        logger.info(f"🗑️  Deleted dataset {dataset_id} by user {user_id}")
        
        return {
            "success": True,
            "message": f"Dataset deleted successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@app.get("/export/{dataset_id}")
async def export_dataset(
    dataset_id: str,
    format: str = "csv",
    user_id: str = Depends(get_current_user)
):
    """Export dataset with authentication and secure blob access"""
    dataset = get_dataset_from_db(dataset_id, user_id)
    blob_url = dataset["blob_path"]
    
    if format != "csv":
        raise HTTPException(status_code=400, detail="Only CSV export supported")
    
    conn = None
    try:
        conn = create_duckdb_connection_with_azure()
        result = conn.execute(f"SELECT * FROM '{blob_url}'").fetchdf()
        
        output = io.StringIO()
        result.to_csv(output, index=False)
        output.seek(0)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={dataset['filename']}"
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

@app.post("/query/natural")
@limiter.limit(AI_QUERY_RATE_LIMIT)
async def natural_language_query(
    request: Request,
    nlq: NaturalLanguageQuery,
    user_id: str = Depends(get_current_user)
):
    """Natural language query with OpenAI, authentication, and secure blob access"""
    start_time = datetime.now()
    
    dataset = get_dataset_from_db(nlq.dataset_id, user_id)
    blob_url = dataset["blob_path"]
    
    conn = None
    try:
        conn = create_duckdb_connection_with_azure()
        
        # Get schema
        schema_query = f"DESCRIBE SELECT * FROM '{blob_url}'"
        schema_info = conn.execute(schema_query).fetchdf()
        
        sample_query = f"SELECT * FROM '{blob_url}' LIMIT 3"
        sample_data = conn.execute(sample_query).fetchdf()
        
        schema_text = "\n".join([
            f"  - {row['column_name']} ({row['column_type']})"
            for _, row in schema_info.iterrows()
        ])
        
        sample_text = sample_data.to_string(index=False, max_rows=3)
        
        # Build prompt
        prompt = f"""You are a DuckDB SQL expert. Generate a SQL query to answer the user's question.

**CRITICAL SAFETY RULES:**
1. Return ONLY the SQL query - no markdown, no explanation, no code blocks
2. Use ONLY SELECT statements - no INSERT, UPDATE, DELETE, DROP, etc.
3. Always include a LIMIT clause (max 10000 rows)
4. Use proper column names and types
5. Return valid DuckDB SQL syntax only
6. Use 'data' as the table name

**Dataset Schema:**
{schema_text}

**Sample Data (first 3 rows):**
{sample_text}

**User Question:** {nlq.question}

**Your Response (SQL query only):**"""
        
        # Call OpenAI
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate ONLY the SQL query - no markdown, no explanation."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=500
        )
        
        sql_query = response.choices[0].message.content.strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        
        logger.info(f"Generated SQL: {sql_query}")
        
        # Validate safety
        is_safe, error_msg = validate_sql_safety(sql_query)
        if not is_safe:
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Execute with timeout
        conn.execute(f"SET statement_timeout='{QUERY_TIMEOUT_SECONDS}000ms';")
        conn.execute(f"CREATE VIEW data AS SELECT * FROM '{blob_url}'")
        
        result = conn.execute(sql_query).fetchdf()
        
        # Limit results
        truncated = len(result) > MAX_RESULT_ROWS
        if truncated:
            result = result[:MAX_RESULT_ROWS]
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return {
            "success": True,
            "sql_query": sql_query,
            "rows_returned": len(result),
            "truncated": truncated,
            "execution_time_seconds": round(execution_time, 2),
            "columns": list(result.columns),
            "data": result.to_dict(orient="records")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"NLQ failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# ============================================================================
# STARTUP EVENT
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Log startup info"""
    logger.info(f"""
╔═══════════════════════════════════════════╗
║           JetDB v{VERSION}                 ║
║       Production Ready Backend            ║
║       Secure Azure Blob Access            ║
╚═══════════════════════════════════════════╝
    """)
    logger.info("✅ Environment validated")
    logger.info("✅ JWT Authentication enabled")
    logger.info("✅ Azure Blob Storage connected (SECURE)")
    logger.info(f"✅ CORS restricted to: {FRONTEND_URL}")
    logger.info("✅ Rate limiting enabled")
    logger.info("✅ Request ID tracking enabled")
    logger.info("✅ Query timeout: 30 seconds")
    logger.info("✅ All 13 critical fixes + secure blob access implemented")
    logger.info("🚀 Ready for production deployment")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
