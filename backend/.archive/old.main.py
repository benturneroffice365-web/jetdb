"""
JetDB v7.2 - Production Secured Backend
========================================

NEW IN v7.2:
- 🔒 SQL injection protection
- 🔒 Security headers on all responses
- 🔒 File upload validation
- 🔒 Environment variable validation
- 🤖 GPT-4o-mini integration (replacing Claude)
- 🚀 DevOps ready (Docker, health checks, monitoring)

Author: Built with Claude
License: MIT
"""

import os
import re
import io
import uuid
import time
import logging
from datetime import datetime
from typing import Optional, List
from dotenv import load_dotenv

from fastapi import FastAPI, File, UploadFile, HTTPException, Request, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import duckdb
import httpx
from supabase import create_client, Client
from azure.storage.blob import BlobServiceClient, ContentSettings
import openai

# Load environment variables
load_dotenv()

# Version
VERSION = "7.2.0"
RELEASE_DATE = "2025-01-11"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =======================================================================
# SECURITY CONSTANTS
# =======================================================================

ALLOWED_EXTENSIONS = {'.csv', '.tsv', '.txt'}
ALLOWED_MIME_TYPES = {'text/csv', 'text/plain', 'application/csv', 'text/tab-separated-values'}
MAX_FILENAME_LENGTH = 255
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB

# SQL injection patterns to block
DANGEROUS_SQL_PATTERNS = [
    r'\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE)\b',
    r';\s*DROP',
    r'--',
    r'/\*',
    r'\*/',
    r'xp_',
    r'sp_'
]

# =======================================================================
# ENVIRONMENT VALIDATION
# =======================================================================

def validate_environment():
    """Ensure all required environment variables are set"""
    required_vars = [
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "AZURE_STORAGE_CONNECTION_STRING",
        "OPENAI_API_KEY",
        "FRONTEND_URL"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        error_msg = f"❌ Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate FRONTEND_URL format
    frontend_url = os.getenv("FRONTEND_URL")
    if not frontend_url.startswith(("http://", "https://")):
        raise ValueError(f"FRONTEND_URL must start with http:// or https://. Got: {frontend_url}")
    
    # Warn if using default/example values
    if "example" in os.getenv("SUPABASE_URL", "").lower():
        logger.warning("⚠️  SUPABASE_URL contains 'example' - is this configured correctly?")
    
    if os.getenv("OPENAI_API_KEY", "").startswith("sk-proj-example"):
        logger.warning("⚠️  Using example OPENAI_API_KEY - AI features will not work")
    
    logger.info("✅ All required environment variables validated")

# Validate on startup
validate_environment()

# =======================================================================
# CONFIGURATION
# =======================================================================

FRONTEND_URL = os.getenv("FRONTEND_URL")

# Initialize Supabase
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)
logger.info("✅ Supabase connected")

# Initialize Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(
    os.getenv("AZURE_STORAGE_CONNECTION_STRING")
)
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

# Initialize OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY")
logger.info("✅ OpenAI API configured")

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

# HTTP Bearer for auth
security = HTTPBearer()

# =======================================================================
# SECURITY FUNCTIONS
# =======================================================================

def sanitize_sql_query(sql: str) -> str:
    """
    Sanitize SQL query to prevent injection attacks.
    Only allows SELECT statements.
    """
    sql = sql.strip()
    
    # Must start with SELECT
    if not sql.upper().startswith('SELECT'):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries are allowed"
        )
    
    # Check for dangerous patterns
    for pattern in DANGEROUS_SQL_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE):
            raise HTTPException(
                status_code=400,
                detail=f"SQL query contains forbidden operation"
            )
    
    # Block multiple statements
    if ';' in sql[:-1]:  # Allow trailing semicolon
        raise HTTPException(
            status_code=400,
            detail="Multiple SQL statements not allowed"
        )
    
    return sql

def validate_upload_file(file: UploadFile):
    """Validate uploaded file for security"""
    
    # Check filename length
    if len(file.filename) > MAX_FILENAME_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Filename too long. Maximum {MAX_FILENAME_LENGTH} characters."
        )
    
    # Check for path traversal
    if '..' in file.filename or '/' in file.filename or '\\' in file.filename:
        raise HTTPException(
            status_code=400,
            detail="Invalid filename. Path characters not allowed."
        )
    
    # Check file extension
    ext = os.path.splitext(file.filename)[1].lower()
    if not ext:
        raise HTTPException(
            status_code=400,
            detail="File must have an extension (.csv, .tsv, or .txt)"
        )
    
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Only CSV, TSV, and TXT files allowed. Got: {ext}"
        )
    
    # Check MIME type
    if file.content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid content type. Expected CSV/TSV, got: {file.content_type}"
        )
    
    # Check filename for suspicious patterns
    suspicious_patterns = ['.exe', '.sh', '.bat', '.cmd', '.ps1', '.py', '.js']
    filename_lower = file.filename.lower()
    for pattern in suspicious_patterns:
        if pattern in filename_lower:
            raise HTTPException(
                status_code=400,
                detail=f"Suspicious filename pattern detected: {pattern}"
            )

# =======================================================================
# FASTAPI APP SETUP
# =======================================================================

app = FastAPI(
    title="JetDB API",
    version=VERSION,
    description="Production-secured backend for massive dataset exploration"
)

# Security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses"""
    response = await call_next(request)
    
    # Security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'"
    
    return response

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# =======================================================================
# AUTHENTICATION
# =======================================================================

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify JWT token and return user info"""
    token = credentials.credentials
    
    try:
        # Verify with Supabase
        user = supabase.auth.get_user(token)
        return user.user.model_dump()
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token"
        )

# =======================================================================
# HELPER FUNCTIONS
# =======================================================================

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
    
    return blob_client.url

def delete_from_blob(dataset_id: str):
    """Delete dataset from blob storage"""
    try:
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=dataset_id
        )
        blob_client.delete_blob()
    except Exception as e:
        logger.warning(f"Blob delete failed (may not exist): {e}")

def get_dataset_from_db(dataset_id: str, user_id: str) -> dict:
    """Get dataset from Supabase, verify ownership"""
    result = supabase.table('datasets').select('*').eq('id', dataset_id).eq('user_id', user_id).execute()
    
    if not result.data:
        raise HTTPException(status_code=404, detail="Dataset not found or access denied")
    
    return result.data[0]

def estimate_row_count(content: bytes) -> int:
    """Quick estimate of row count"""
    sample = content[:100000]
    line_count = sample.count(b'\n')
    if line_count == 0:
        return 0
    ratio = len(content) / len(sample)
    return int(line_count * ratio)

# =======================================================================
# BACKGROUND TASKS
# =======================================================================

def analyze_dataset(dataset_id: str, blob_url: str):
    """Analyze dataset in background"""
    try:
        start_time = time.time()
        
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL azure; LOAD azure;")
        conn.execute(f"SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';")
        
        result = conn.execute(f"SELECT * FROM '{blob_url}' LIMIT 1").fetchdf()
        row_count = conn.execute(f"SELECT COUNT(*) FROM '{blob_url}'").fetchone()[0]
        conn.close()
        
        analysis_time = time.time() - start_time
        
        supabase.table('datasets').update({
            "row_count": row_count,
            "column_count": len(result.columns),
            "columns": list(result.columns),
            "status": "ready",
            "analysis_time_seconds": round(analysis_time, 2)
        }).eq('id', dataset_id).execute()
        
        logger.info(f"✅ Analysis complete: {dataset_id} | Rows: {row_count:,} | Time: {analysis_time:.1f}s")
        
    except Exception as e:
        logger.error(f"❌ Analysis failed for {dataset_id}: {e}")
        supabase.table('datasets').update({
            "status": "error",
            "error": str(e)
        }).eq('id', dataset_id).execute()

# =======================================================================
# PYDANTIC MODELS
# =======================================================================

class SQLQuery(BaseModel):
    sql: str
    dataset_id: str

class NaturalLanguageQuery(BaseModel):
    question: str
    dataset_id: str

# =======================================================================
# PUBLIC ENDPOINTS
# =======================================================================

@app.get("/")
def read_root():
    """API information - PUBLIC"""
    return {
        "service": "JetDB API",
        "version": VERSION,
        "status": "running",
        "authentication": "required",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
def health_check():
    """Health check - PUBLIC"""
    try:
        # Test Supabase connection
        supabase.table('datasets').select('id').limit(1).execute()
        
        # Test Blob storage
        blob_service_client.get_container_client(CONTAINER_NAME).exists()
        
        return {
            "status": "healthy",
            "version": VERSION,
            "timestamp": datetime.now().isoformat(),
            "services": {
                "supabase": "connected",
                "azure_blob": "connected",
                "openai": "configured"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )

# =======================================================================
# PROTECTED ENDPOINTS
# =======================================================================

@app.post("/upload")
@limiter.limit("10/minute")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    current_user: dict = Depends(get_current_user)
):
    """Upload CSV file - PROTECTED"""
    
    user_id = current_user["id"]
    
    # Validate file (SECURITY)
    validate_upload_file(file)
    
    content = await file.read()
    file_size_bytes = len(content)
    file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
    
    if file_size_bytes > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"File too large ({file_size_mb}MB). Maximum is 10GB."
        )
    
    # Generate dataset ID
    dataset_id = str(uuid.uuid4())
    
    try:
        # Upload to blob storage
        blob_url = upload_to_blob(dataset_id, content, file.filename)
        
        # Quick estimate
        estimated_rows = estimate_row_count(content)
        
        # Save to Supabase
        dataset_record = {
            "id": dataset_id,
            "user_id": user_id,
            "filename": file.filename,
            "file_size_bytes": file_size_bytes,
            "blob_url": blob_url,
            "estimated_rows": estimated_rows,
            "status": "analyzing",
            "uploaded_at": datetime.now().isoformat()
        }
        
        supabase.table('datasets').insert(dataset_record).execute()
        
        # Start background analysis
        background_tasks.add_task(analyze_dataset, dataset_id, blob_url)
        
        logger.info(f"📤 Upload by {user_id}: {file.filename} ({file_size_mb}MB) | Est. rows: {estimated_rows:,}")
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "filename": file.filename,
            "file_size_mb": file_size_mb,
            "estimated_rows": estimated_rows,
            "status": "analyzing",
            "message": "Upload successful. Analysis in progress."
        }
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/datasets")
def get_datasets(current_user: dict = Depends(get_current_user)):
    """List user's datasets - PROTECTED"""
    user_id = current_user["id"]
    
    try:
        result = supabase.table('datasets').select('*').eq('user_id', user_id).order('uploaded_at', desc=True).execute()
        
        return {
            "datasets": result.data,
            "count": len(result.data)
        }
    except Exception as e:
        logger.error(f"Dataset fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch datasets: {str(e)}")

@app.get("/datasets/{dataset_id}")
def get_dataset(
    dataset_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get dataset details - PROTECTED"""
    user_id = current_user["id"]
    return get_dataset_from_db(dataset_id, user_id)

@app.get("/datasets/{dataset_id}/rows")
def get_rows(
    dataset_id: str,
    offset: int = 0,
    limit: int = 1000,
    current_user: dict = Depends(get_current_user)
):
    """Get paginated rows - PROTECTED"""
    user_id = current_user["id"]
    dataset = get_dataset_from_db(dataset_id, user_id)
    blob_url = dataset['blob_url']
    
    try:
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL azure; LOAD azure;")
        conn.execute(f"SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';")
        
        result = conn.execute(
            f"SELECT * FROM '{blob_url}' LIMIT {limit} OFFSET {offset}"
        ).fetchdf()
        conn.close()
        
        return {
            "dataset_id": dataset_id,
            "offset": offset,
            "limit": limit,
            "returned_rows": len(result),
            "total_rows": dataset.get("row_count"),
            "estimated_rows": dataset.get("estimated_rows"),
            "status": dataset.get("status"),
            "data": result.to_dict(orient="records")
        }
        
    except Exception as e:
        logger.error(f"Row fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch rows: {str(e)}")

@app.post("/query/sql")
@limiter.limit("30/minute")
def execute_sql(
    request: Request,
    query: SQLQuery,
    current_user: dict = Depends(get_current_user)
):
    """Execute SQL query - PROTECTED"""
    user_id = current_user["id"]
    dataset = get_dataset_from_db(query.dataset_id, user_id)
    blob_url = dataset['blob_url']
    
    # Sanitize SQL (SECURITY)
    sanitized_sql = sanitize_sql_query(query.sql)
    
    try:
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL azure; LOAD azure;")
        conn.execute(f"SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';")
        conn.execute(f"CREATE VIEW data AS SELECT * FROM '{blob_url}'")
        
        start_time = time.time()
        result = conn.execute(sanitized_sql.replace(query.dataset_id, 'data')).fetchdf()
        query_time = time.time() - start_time
        
        conn.close()
        
        logger.info(f"🔍 SQL by {user_id}: {len(result)} rows in {query_time:.2f}s")
        
        return {
            "success": True,
            "rows": len(result),
            "columns": list(result.columns),
            "data": result.to_dict(orient="records"),
            "query_time_seconds": round(query_time, 3)
        }
        
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        raise HTTPException(status_code=400, detail=f"Query failed: {str(e)}")

@app.post("/query/natural")
@limiter.limit("20/minute")
async def natural_language_query(
    request: Request,
    query: NaturalLanguageQuery,
    current_user: dict = Depends(get_current_user)
):
    """Natural language query - PROTECTED"""
    user_id = current_user["id"]
    dataset = get_dataset_from_db(query.dataset_id, user_id)
    
    try:
        # Get schema info
        columns = dataset.get("columns", [])
        row_count = dataset.get("row_count", "unknown")
        
        # Build prompt for GPT-4o-mini
        prompt = f"""You are a SQL expert. Convert this natural language question into a DuckDB SQL query.

Dataset: {dataset['filename']}
Columns: {', '.join(columns)}
Row count: {row_count:,} rows

Question: {query.question}

Rules:
- Use 'data' as the table name
- Only generate SELECT statements
- Return ONLY the SQL query, no explanations
- Keep queries efficient for large datasets"""

        # Call OpenAI GPT-4o-mini
        response = await openai.ChatCompletion.acreate(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL expert that converts natural language to SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        generated_sql = response.choices[0].message.content.strip()
        
        # Clean up the SQL
        generated_sql = generated_sql.replace("```sql", "").replace("```", "").strip()
        
        # Sanitize (SECURITY)
        sanitized_sql = sanitize_sql_query(generated_sql)
        
        # Execute the query
        blob_url = dataset['blob_url']
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL azure; LOAD azure;")
        conn.execute(f"SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';")
        conn.execute(f"CREATE VIEW data AS SELECT * FROM '{blob_url}'")
        
        start_time = time.time()
        result = conn.execute(sanitized_sql).fetchdf()
        query_time = time.time() - start_time
        
        conn.close()
        
        logger.info(f"🤖 NLQ by {user_id}: '{query.question}' → {len(result)} rows in {query_time:.2f}s")
        
        return {
            "success": True,
            "question": query.question,
            "generated_sql": sanitized_sql,
            "rows": len(result),
            "columns": list(result.columns),
            "data": result.to_dict(orient="records"),
            "query_time_seconds": round(query_time, 3)
        }
        
    except Exception as e:
        logger.error(f"NLQ failed: {e}")
        raise HTTPException(status_code=500, detail=f"Natural language query failed: {str(e)}")

@app.get("/export/{dataset_id}")
def export_dataset(
    dataset_id: str,
    format: str = "csv",
    current_user: dict = Depends(get_current_user)
):
    """Export dataset - PROTECTED"""
    user_id = current_user["id"]
    dataset = get_dataset_from_db(dataset_id, user_id)
    blob_url = dataset['blob_url']
    
    if format != "csv":
        raise HTTPException(status_code=400, detail="Only CSV export supported")
    
    try:
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL azure; LOAD azure;")
        conn.execute(f"SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';")
        
        result = conn.execute(f"SELECT * FROM '{blob_url}'").fetchdf()
        conn.close()
        
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
        logger.error(f"Export failed: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@app.delete("/datasets/{dataset_id}")
def delete_dataset(
    dataset_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete dataset - PROTECTED"""
    user_id = current_user["id"]
    dataset = get_dataset_from_db(dataset_id, user_id)
    
    try:
        # Delete from blob storage
        delete_from_blob(dataset_id)
        
        # Delete from Supabase
        supabase.table('datasets').delete().eq('id', dataset_id).execute()
        
        logger.info(f"🗑️  Deleted dataset by {user_id}: {dataset_id}")
        
        return {
            "success": True,
            "message": f"Dataset deleted successfully"
        }
        
    except Exception as e:
        logger.error(f"Delete failed: {e}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

# =======================================================================
# STARTUP EVENT
# =======================================================================

@app.on_event("startup")
async def startup_event():
    """Log startup info"""
    logger.info(f"""
╔═══════════════════════════════════════════╗
║           JetDB v{VERSION}                 ║
║       Production Secured Backend          ║
║   Released: {RELEASE_DATE}                ║
╚═══════════════════════════════════════════╝
    """)
    logger.info("✅ Environment validated")
    logger.info("✅ Authentication enabled")
    logger.info(f"✅ CORS restricted to: {FRONTEND_URL}")
    logger.info("✅ Rate limiting enabled")
    logger.info("✅ Security headers enabled")
    logger.info("✅ SQL injection protection enabled")
    logger.info("✅ File upload validation enabled")
    logger.info("🤖 AI: OpenAI GPT-4o-mini")
    logger.info("✅ Ready for production")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
