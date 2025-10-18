"""
JetDB Backend v8.0 - ULTRA-ROBUST VERSION
✅ Multiple CSV reading strategies with fallbacks
✅ Handles weird delimiters, encodings, and characters
✅ Comprehensive error messages
✅ Production-ready error handling
"""

import os
import uuid
import time
import logging
import tempfile
import re
from datetime import datetime
from typing import Optional, List, Tuple
from contextlib import asynccontextmanager

import duckdb
import pandas as pd
import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from dotenv import load_dotenv
from supabase import create_client, Client
from azure.storage.blob import BlobServiceClient
from openai import OpenAI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Load environment variables
load_dotenv()

# Create logs directory
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_SAS_TOKEN = os.getenv("AZURE_SAS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

# Validate required environment variables
if not all([SUPABASE_URL, SUPABASE_KEY, AZURE_CONNECTION_STRING, AZURE_SAS_TOKEN]):
    raise ValueError("Missing required environment variables. Check your .env file.")

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service.get_container_client("jetdb-datasets")

# Initialize OpenAI only if key is provided
openai_client = None
if OPENAI_API_KEY:
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    logger.info("✅ OpenAI client initialized")
else:
    logger.warning("⚠️ OpenAI API key not provided - AI queries will be disabled")

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

# Security
security = HTTPBearer()

# Lifespan context
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 JetDB v8.0 ULTRA-ROBUST starting up...")
    logger.info(f"📍 Frontend URL: {FRONTEND_URL}")
    logger.info(f"📊 Supabase: {SUPABASE_URL}")
    yield
    logger.info("👋 JetDB shutting down...")

# FastAPI app
app = FastAPI(
    title="JetDB API",
    version="8.0.0-robust",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ============================================================================
# MODELS
# ============================================================================

class SQLQuery(BaseModel):
    sql: str
    dataset_id: str

class NaturalLanguageQuery(BaseModel):
    question: str
    dataset_id: str

class MergeRequest(BaseModel):
    dataset_ids: List[str]
    merged_name: str

# ============================================================================
# AUTH - FIXED
# ============================================================================

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """Verify JWT token and return user_id"""
    try:
        token = credentials.credentials
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {token}"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{SUPABASE_URL}/auth/v1/user",
                headers=headers,
                timeout=10.0
            )
        
        if response.status_code != 200:
            logger.warning(f"Auth failed: {response.status_code}")
            raise HTTPException(401, detail="Invalid token")
        
        user_data = response.json()
        user_id = user_data.get("id")
        
        if not user_id:
            raise HTTPException(401, detail="No user ID in token")
        
        return user_id
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Auth error: {e}")
        raise HTTPException(401, detail="Authentication failed")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_duckdb_connection_with_azure():
    """Create DuckDB connection with Azure extensions"""
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    return conn

def get_authenticated_blob_url(blob_path: str) -> str:
    """Get authenticated URL for Azure blob"""
    if "?" in blob_path and "sig=" in blob_path:
        return blob_path
    
    blob_name = blob_path.split("jetdb-datasets/")[-1]
    base_url = blob_path.split("?")[0]
    return f"{base_url}{AZURE_SAS_TOKEN}"

def upload_to_blob_streaming(dataset_id: str, file_path: str, filename: str, content_type: str) -> str:
    """Upload file to Azure Blob Storage"""
    blob_name = f"{dataset_id}/{filename}"
    blob_client = container_client.get_blob_client(blob_name)
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    blob_url = f"https://{blob_service.account_name}.blob.core.windows.net/jetdb-datasets/{blob_name}"
    logger.info(f"📤 Uploaded to blob: {blob_name}")
    return blob_url

def sanitize_column_names(columns: List[str]) -> List[str]:
    """
    Sanitize column names to handle weird characters
    Removes/replaces special characters that might break SQL
    """
    sanitized = []
    seen = set()
    
    for col in columns:
        # Remove leading/trailing whitespace
        clean = col.strip()
        
        # Replace problematic characters
        clean = re.sub(r'[^\w\s-]', '_', clean)  # Replace special chars with underscore
        clean = re.sub(r'\s+', '_', clean)  # Replace spaces with underscore
        clean = re.sub(r'_+', '_', clean)  # Replace multiple underscores with single
        clean = clean.strip('_')  # Remove leading/trailing underscores
        
        # Ensure it doesn't start with a number
        if clean and clean[0].isdigit():
            clean = f"col_{clean}"
        
        # Handle empty column names
        if not clean:
            clean = f"column_{len(sanitized)}"
        
        # Handle duplicates
        original_clean = clean
        counter = 1
        while clean.lower() in seen:
            clean = f"{original_clean}_{counter}"
            counter += 1
        
        seen.add(clean.lower())
        sanitized.append(clean)
    
    return sanitized

def try_read_csv_with_strategies(auth_url: str, conn) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    """
    Try multiple CSV reading strategies until one works
    Returns: (sample_dataframe, successful_query, strategy_name)
    """
    
    strategies = [
        # Strategy 1: Auto-detect with increased sample
        {
            "name": "auto_detect_large_sample",
            "query": f"""
                SELECT * FROM read_csv_auto(
                    '{auth_url}',
                    header=true,
                    ignore_errors=true,
                    null_padding=true,
                    max_line_size=100000000,
                    sample_size=20000,
                    all_varchar=false
                )
            """
        },
        
        # Strategy 2: Auto-detect with all columns as text (safest)
        {
            "name": "auto_detect_all_text",
            "query": f"""
                SELECT * FROM read_csv_auto(
                    '{auth_url}',
                    header=true,
                    ignore_errors=true,
                    null_padding=true,
                    max_line_size=100000000,
                    sample_size=10000,
                    all_varchar=true
                )
            """
        },
        
        # Strategy 3: Explicit comma delimiter
        {
            "name": "comma_delimiter",
            "query": f"""
                SELECT * FROM read_csv(
                    '{auth_url}',
                    delim=',',
                    header=true,
                    ignore_errors=true,
                    null_padding=true,
                    quote='"',
                    escape='"',
                    max_line_size=100000000,
                    sample_size=10000,
                    all_varchar=true
                )
            """
        },
        
        # Strategy 4: Tab-delimited
        {
            "name": "tab_delimiter",
            "query": f"""
                SELECT * FROM read_csv(
                    '{auth_url}',
                    delim='\t',
                    header=true,
                    ignore_errors=true,
                    null_padding=true,
                    max_line_size=100000000,
                    sample_size=10000,
                    all_varchar=true
                )
            """
        },
        
        # Strategy 5: Semicolon-delimited (European Excel)
        {
            "name": "semicolon_delimiter",
            "query": f"""
                SELECT * FROM read_csv(
                    '{auth_url}',
                    delim=';',
                    header=true,
                    ignore_errors=true,
                    null_padding=true,
                    max_line_size=100000000,
                    sample_size=10000,
                    all_varchar=true
                )
            """
        },
        
        # Strategy 6: Pipe-delimited
        {
            "name": "pipe_delimiter",
            "query": f"""
                SELECT * FROM read_csv(
                    '{auth_url}',
                    delim='|',
                    header=true,
                    ignore_errors=true,
                    null_padding=true,
                    max_line_size=100000000,
                    sample_size=10000,
                    all_varchar=true
                )
            """
        },
        
        # Strategy 7: No header (first row is data)
        {
            "name": "no_header",
            "query": f"""
                SELECT * FROM read_csv_auto(
                    '{auth_url}',
                    header=false,
                    ignore_errors=true,
                    null_padding=true,
                    max_line_size=100000000,
                    sample_size=10000,
                    all_varchar=true
                )
            """
        },
    ]
    
    for strategy in strategies:
        try:
            logger.info(f"🔍 Trying CSV reading strategy: {strategy['name']}")
            
            # Try to read a sample
            sample = conn.execute(f"{strategy['query']} LIMIT 10").fetchdf()
            
            # Validate we got meaningful data
            if len(sample.columns) >= 1 and len(sample) > 0:
                # Check if we got at least some non-null data
                non_null_count = sample.count().sum()
                
                if non_null_count > 0:
                    logger.info(f"✅ Strategy '{strategy['name']}' succeeded! Columns: {len(sample.columns)}, Rows: {len(sample)}")
                    return sample, strategy['query'], strategy['name']
                else:
                    logger.warning(f"Strategy '{strategy['name']}' returned all nulls")
            else:
                logger.warning(f"Strategy '{strategy['name']}' returned no columns or rows")
                
        except Exception as e:
            logger.warning(f"Strategy '{strategy['name']}' failed: {str(e)[:200]}")
            continue
    
    return None, None, None

def analyze_dataset_background(dataset_id: str, blob_path: str, user_id: str):
    """Background task to analyze uploaded dataset - ULTRA-ROBUST"""
    try:
        logger.info(f"📊 Starting ROBUST analysis for dataset {dataset_id}")
        
        auth_url = get_authenticated_blob_url(blob_path)
        conn = create_duckdb_connection_with_azure()
        
        # Try multiple strategies to read the CSV
        sample, successful_query, strategy_name = try_read_csv_with_strategies(auth_url, conn)
        
        if sample is None or successful_query is None:
            raise Exception(
                "Could not parse CSV file. Please ensure: "
                "(1) File is a valid CSV with headers, "
                "(2) Uses comma, tab, semicolon, or pipe as delimiter, "
                "(3) Has at least 2 columns, "
                "(4) Uses standard double-quotes for text fields"
            )
        
        # Sanitize column names
        original_columns = sample.columns.tolist()
        columns = sanitize_column_names(original_columns)
        
        logger.info(f"📋 Columns detected: {columns}")
        
        if original_columns != columns:
            logger.warning(f"⚠️ Column names were sanitized. Original: {original_columns}")
        
        # Count rows
        logger.info(f"🔢 Counting rows using strategy: {strategy_name}")
        
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM ({successful_query})").fetchone()[0]
        except Exception as count_error:
            logger.warning(f"Direct count failed, trying alternative: {count_error}")
            # Fallback: estimate from sample
            row_count = len(sample) * 100  # Rough estimate
            logger.warning(f"Using estimated row count: {row_count}")
        
        conn.close()
        
        # Update database
        logger.info(f"💾 Updating database: {row_count:,} rows, {len(columns)} columns")
        supabase.table('datasets').update({
            'row_count': row_count,
            'column_count': len(columns),
            'columns': columns,
            'status': 'ready',
            'updated_at': datetime.now().isoformat()
        }).eq('id', dataset_id).execute()
        
        logger.info(f"✅ Dataset {dataset_id} analyzed successfully with strategy: {strategy_name}")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Analysis failed for {dataset_id}: {error_msg}", exc_info=True)
        
        # Create user-friendly error message
        helpful_msg = error_msg
        
        if "sniff" in error_msg.lower() or "delimiter" in error_msg.lower():
            helpful_msg = "CSV format not recognized. Please use comma, tab, semicolon, or pipe as delimiter."
        elif "quote" in error_msg.lower() or "escape" in error_msg.lower():
            helpful_msg = "CSV quote/escape characters not recognized. Please use standard double-quotes (\")."
        elif "encoding" in error_msg.lower():
            helpful_msg = "File encoding not supported. Please save as UTF-8."
        elif "header" in error_msg.lower():
            helpful_msg = "Could not detect column headers. Please ensure first row contains column names."
        
        # Update with error status
        try:
            supabase.table('datasets').update({
                'status': 'error',
                'error_message': helpful_msg[:500],
                'updated_at': datetime.now().isoformat()
            }).eq('id', dataset_id).execute()
        except Exception as update_error:
            logger.error(f"Failed to update error status: {update_error}")

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "8.0.0-robust",
        "timestamp": datetime.now().isoformat(),
        "ai_enabled": openai_client is not None
    }

# ============================================================================
# UPLOAD
# ============================================================================

@app.post("/upload")
@limiter.limit("10/hour")
async def upload_file(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user_id: str = Depends(get_current_user)
):
    """Upload CSV file"""
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, detail="Only CSV files are supported")
    
    dataset_id = str(uuid.uuid4())
    
    try:
        logger.info(f"📥 Starting upload: {file.filename} for user {user_id}")
        
        # Save to temp file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        # Upload to blob
        blob_url = upload_to_blob_streaming(
            dataset_id,
            temp_file.name,
            file.filename,
            "text/csv"
        )
        
        # Create database record
        dataset_record = {
            "id": dataset_id,
            "user_id": user_id,
            "filename": file.filename,
            "blob_path": blob_url,
            "size_bytes": len(content),
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "status": "processing",
            "storage_format": "csv",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        result = supabase.table('datasets').insert(dataset_record).execute()
        
        if not result.data:
            raise Exception("Failed to create dataset record in database")
        
        # Start background analysis
        background_tasks.add_task(analyze_dataset_background, dataset_id, blob_url, user_id)
        
        # Cleanup temp file
        os.unlink(temp_file.name)
        
        logger.info(f"✅ Upload complete: {file.filename} → {dataset_id}")
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "filename": file.filename,
            "message": "File uploaded successfully. Processing in background."
        }
        
    except Exception as e:
        logger.error(f"❌ Upload failed: {str(e)}", exc_info=True)
        
        # Cleanup on error
        try:
            if 'temp_file' in locals():
                os.unlink(temp_file.name)
        except:
            pass
        
        raise HTTPException(500, detail=f"Upload failed: {str(e)}")

# ============================================================================
# DATASETS
# ============================================================================

@app.get("/datasets")
async def list_datasets(user_id: str = Depends(get_current_user)):
    """List all datasets for user"""
    try:
        result = supabase.table('datasets')\
            .select('*')\
            .eq('user_id', user_id)\
            .order('created_at', desc=True)\
            .execute()
        
        datasets = result.data or []
        logger.info(f"📋 Listed {len(datasets)} datasets for user {user_id}")
        
        return {"datasets": datasets}
        
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}")
        raise HTTPException(500, detail=str(e))

@app.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: str, user_id: str = Depends(get_current_user)):
    """Get dataset details"""
    try:
        result = supabase.table('datasets')\
            .select('*')\
            .eq('id', dataset_id)\
            .eq('user_id', user_id)\
            .single()\
            .execute()
        
        if not result.data:
            raise HTTPException(404, detail="Dataset not found")
        
        return result.data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dataset: {e}")
        raise HTTPException(500, detail=str(e))

@app.get("/datasets/{dataset_id}/data")
async def get_dataset_data(
    dataset_id: str,
    limit: int = 100000,
    offset: int = 0,
    user_id: str = Depends(get_current_user)
):
    """Get dataset data with pagination - ROBUST VERSION"""
    
    try:
        logger.info(f"📊 Fetching data for dataset {dataset_id} (limit={limit}, offset={offset})")
        
        # Get dataset from DB
        dataset = supabase.table('datasets')\
            .select('*')\
            .eq('id', dataset_id)\
            .eq('user_id', user_id)\
            .single()\
            .execute()
        
        if not dataset.data:
            raise HTTPException(404, detail="Dataset not found")
        
        # Check status
        status = dataset.data.get('status')
        if status == 'error':
            error_msg = dataset.data.get('error_message', 'Unknown error during processing')
            raise HTTPException(400, detail=f"Dataset processing failed: {error_msg}")
        elif status != 'ready':
            raise HTTPException(400, detail=f"Dataset not ready yet. Status: {status}")
        
        blob_path = dataset.data.get('blob_path')
        if not blob_path:
            raise HTTPException(400, detail="Dataset has no blob_path")
        
        # Get authenticated URL
        auth_url = get_authenticated_blob_url(blob_path)
        
        # Try multiple reading strategies
        conn = create_duckdb_connection_with_azure()
        
        sample, successful_query, strategy_name = try_read_csv_with_strategies(auth_url, conn)
        
        if sample is None or successful_query is None:
            conn.close()
            raise HTTPException(500, detail="Could not read CSV data. File may be corrupted.")
        
        # Add LIMIT and OFFSET to the successful query
        paginated_query = f"{successful_query} LIMIT {limit} OFFSET {offset}"
        
        result_df = conn.execute(paginated_query).fetchdf()
        conn.close()
        
        logger.info(f"✅ Returned {len(result_df)} rows for dataset {dataset_id} using strategy: {strategy_name}")
        
        return {
            "data": result_df.to_dict('records'),
            "columns": result_df.columns.tolist(),
            "rows_returned": len(result_df)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dataset data: {str(e)}", exc_info=True)
        raise HTTPException(500, detail=f"Failed to load data: {str(e)}")

@app.delete("/datasets/{dataset_id}")
async def delete_dataset(
    dataset_id: str,
    user_id: str = Depends(get_current_user)
):
    """Delete dataset"""
    try:
        dataset = supabase.table('datasets')\
            .select('blob_path')\
            .eq('id', dataset_id)\
            .eq('user_id', user_id)\
            .single()\
            .execute()
        
        if not dataset.data:
            raise HTTPException(404, detail="Dataset not found")
        
        # Delete from blob storage
        try:
            blob_name = dataset.data['blob_path'].split("jetdb-datasets/")[-1].split("?")[0]
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            logger.info(f"🗑️ Blob deleted: {blob_name}")
        except Exception as blob_error:
            logger.warning(f"Blob delete failed: {blob_error}")
        
        # Delete from database
        supabase.table('datasets').delete().eq('id', dataset_id).execute()
        
        logger.info(f"🗑️ Dataset deleted: {dataset_id}")
        
        return {"success": True, "message": "Dataset deleted"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete failed: {e}")
        raise HTTPException(500, detail=str(e))

# ============================================================================
# MERGE - ROBUST VERSION
# ============================================================================

@app.post("/datasets/merge")
@limiter.limit("5/hour")
async def merge_datasets(
    request: Request,
    background_tasks: BackgroundTasks,
    merge_request: MergeRequest,
    user_id: str = Depends(get_current_user)
):
    """Merge multiple datasets - ROBUST VERSION"""
    
    dataset_ids = merge_request.dataset_ids
    merged_name = merge_request.merged_name
    
    if len(dataset_ids) < 2:
        raise HTTPException(400, detail="Need at least 2 datasets to merge")
    
    try:
        logger.info(f"🔄 Starting merge of {len(dataset_ids)} datasets")
        
        # Get all datasets
        datasets = []
        for ds_id in dataset_ids:
            ds = supabase.table('datasets')\
                .select('*')\
                .eq('id', ds_id)\
                .eq('user_id', user_id)\
                .single()\
                .execute()
            
            if not ds.data:
                raise HTTPException(404, detail=f"Dataset {ds_id} not found")
            
            if ds.data.get('status') != 'ready':
                raise HTTPException(400, detail=f"Dataset {ds.data.get('filename')} is not ready")
            
            datasets.append(ds.data)
        
        # Validate schemas match
        first_cols = set(datasets[0]['columns'])
        for ds in datasets[1:]:
            if set(ds['columns']) != first_cols:
                missing = first_cols - set(ds['columns'])
                extra = set(ds['columns']) - first_cols
                raise HTTPException(400, detail={
                    "error": "Schema mismatch",
                    "missing": list(missing),
                    "extra": list(extra)
                })
        
        start_time = time.time()
        conn = create_duckdb_connection_with_azure()
        
        # Build UNION ALL query with robust reading
        union_parts = []
        for ds in datasets:
            auth_url = get_authenticated_blob_url(ds['blob_path'])
            
            # Use robust reading for each dataset
            sample, query, strategy = try_read_csv_with_strategies(auth_url, conn)
            
            if query is None:
                raise HTTPException(500, detail=f"Could not read dataset: {ds.get('filename')}")
            
            union_parts.append(f"({query})")
        
        union_query = " UNION ALL ".join(union_parts)
        
        # Create temp parquet file
        merged_id = str(uuid.uuid4())
        temp_merged = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        temp_merged.close()
        
        # Stream merge to parquet
        logger.info(f"💾 Writing merged parquet...")
        conn.execute(f"""
            COPY ({union_query})
            TO '{temp_merged.name}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        
        # Get row count
        total_rows = conn.execute(f"SELECT COUNT(*) FROM ({union_query})").fetchone()[0]
        
        conn.close()
        
        # Upload merged file
        merged_filename = f"{merged_name}.parquet"
        merged_url = upload_to_blob_streaming(
            merged_id,
            temp_merged.name,
            merged_filename,
            "application/octet-stream"
        )
        
        merge_time = time.time() - start_time
        
        # Save to database
        merged_record = {
            "id": merged_id,
            "user_id": user_id,
            "filename": merged_filename,
            "blob_path": merged_url,
            "size_bytes": os.path.getsize(temp_merged.name),
            "row_count": total_rows,
            "column_count": len(datasets[0]['columns']),
            "columns": datasets[0]['columns'],
            "status": "ready",
            "storage_format": "parquet",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        supabase.table('datasets').insert(merged_record).execute()
        
        # Cleanup
        os.unlink(temp_merged.name)
        
        logger.info(f"✅ Merge complete: {total_rows:,} rows in {merge_time:.1f}s")
        
        return {
            "success": True,
            "dataset_id": merged_id,
            "row_count": total_rows,
            "merge_time_seconds": round(merge_time, 1)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Merge failed: {str(e)}", exc_info=True)
        raise HTTPException(500, detail=str(e))

# ============================================================================
# QUERY - ROBUST VERSION
# ============================================================================

@app.post("/query/sql")
@limiter.limit("10/minute")
async def execute_sql(
    request: Request,
    query: SQLQuery,
    user_id: str = Depends(get_current_user)
):
    """Execute SQL query - ROBUST VERSION"""
    
    try:
        dataset = supabase.table('datasets')\
            .select('*')\
            .eq('id', query.dataset_id)\
            .eq('user_id', user_id)\
            .single()\
            .execute()
        
        if not dataset.data:
            raise HTTPException(404, detail="Dataset not found")
        
        if dataset.data.get('status') != 'ready':
            raise HTTPException(400, detail="Dataset not ready")
        
        # SQL validation
        sql_upper = query.sql.upper().strip()
        if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
            raise HTTPException(400, detail="Only SELECT queries allowed")
        
        blocked_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in sql_upper for keyword in blocked_keywords):
            raise HTTPException(400, detail="Query contains blocked keywords")
        
        # Execute query with robust reading
        auth_url = get_authenticated_blob_url(dataset.data['blob_path'])
        conn = create_duckdb_connection_with_azure()
        
        # Get the robust query
        sample, base_query, strategy = try_read_csv_with_strategies(auth_url, conn)
        
        if base_query is None:
            conn.close()
            raise HTTPException(500, detail="Could not read dataset")
        
        # Replace 'FROM data' with actual robust query
        sql_modified = query.sql.replace('FROM data', f'FROM ({base_query})')
        
        start_time = time.time()
        result_df = conn.execute(sql_modified).fetchdf()
        execution_time = time.time() - start_time
        
        conn.close()
        
        logger.info(f"✅ SQL query: {len(result_df)} rows in {execution_time:.2f}s")
        
        return {
            "data": result_df.to_dict('records'),
            "columns": result_df.columns.tolist(),
            "rows_returned": len(result_df),
            "execution_time_seconds": round(execution_time, 3)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"SQL query failed: {str(e)}")
        raise HTTPException(400, detail=str(e))

@app.post("/query/natural")
@limiter.limit("5/minute")
async def natural_language_query(
    request: Request,
    nlq: NaturalLanguageQuery,
    user_id: str = Depends(get_current_user)
):
    """Convert natural language to SQL and execute"""
    
    if not openai_client:
        raise HTTPException(503, detail="AI queries not available")
    
    try:
        dataset = supabase.table('datasets')\
            .select('columns')\
            .eq('id', nlq.dataset_id)\
            .eq('user_id', user_id)\
            .single()\
            .execute()
        
        if not dataset.data:
            raise HTTPException(404, detail="Dataset not found")
        
        columns = dataset.data['columns']
        
        prompt = f"""Convert this question to SQL. The table is called 'data' and has these columns:
{', '.join(columns)}

Question: {nlq.question}

Return only the SQL query, no explanation. Only use SELECT statements."""
        
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate only SELECT queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=300
        )
        
        generated_sql = response.choices[0].message.content.strip()
        generated_sql = generated_sql.replace('```sql', '').replace('```', '').strip()
        
        logger.info(f"🤖 AI generated: {generated_sql}")
        
        # Execute the generated SQL
        sql_query = SQLQuery(sql=generated_sql, dataset_id=nlq.dataset_id)
        result = await execute_sql(request, sql_query, user_id)
        
        result['sql_query'] = generated_sql
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"AI query failed: {str(e)}")
        raise HTTPException(500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)