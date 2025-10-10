"""
JetDB v8.0.0 - Data Persistence Fixed
=====================================
Matches YOUR exact Supabase schema
NO MORE IN-MEMORY DICTIONARY!
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from supabase import create_client, Client
import duckdb
import os
import io
import uuid
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Version
VERSION = "8.0.0"

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="JetDB API", version=VERSION)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")
    raise ValueError("Missing required environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logger.info("✅ Supabase connected")

# Azure connection string (we'll use local files for now if not set)
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
USE_AZURE = bool(AZURE_CONNECTION_STRING)

# Local upload directory (fallback if no Azure)
UPLOAD_DIR = os.path.abspath("uploads")
if not USE_AZURE:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    logger.warning(f"⚠️  No Azure connection string - using local file storage at: {UPLOAD_DIR}")

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class SQLQuery(BaseModel):
    sql: str
    dataset_id: Optional[str] = None

class NaturalLanguageQuery(BaseModel):
    question: str
    dataset_id: str

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def save_file_locally(dataset_id: str, file_content: bytes) -> str:
    """Save CSV to local uploads directory"""
    filepath = os.path.join(UPLOAD_DIR, f"{dataset_id}.csv")
    with open(filepath, "wb") as f:
        f.write(file_content)
    logger.info(f"✅ Saved locally: {filepath}")
    return filepath

def get_dataset_from_db(dataset_id: str) -> dict:
    """Get dataset metadata from Supabase"""
    try:
        response = supabase.table('datasets').select("*").eq('id', dataset_id).execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return response.data[0]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching dataset: {e}")
        raise HTTPException(status_code=500, detail="Database error")

def list_datasets_from_db() -> List[dict]:
    """List all datasets from Supabase"""
    try:
        response = supabase.table('datasets').select("*").order('created_at', desc=True).execute()
        return response.data
    except Exception as e:
        logger.error(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail="Database error")

def analyze_dataset_background(dataset_id: str, file_path: str):
    """Analyze dataset in background - gets exact row count"""
    try:
        logger.info(f"📊 Analyzing {dataset_id}")
        start_time = datetime.now()
        
        # Get exact row count
        conn = duckdb.connect(':memory:')
        row_count = conn.execute(
            f"SELECT COUNT(*) as count FROM '{file_path}'"
        ).fetchone()[0]
        
        # Get column info
        result = conn.execute(f"SELECT * FROM '{file_path}' LIMIT 1").fetchdf()
        conn.close()
        
        analysis_time = (datetime.now() - start_time).total_seconds()
        
        # Update in Supabase
        supabase.table('datasets').update({
            "row_count": row_count,
            "column_count": len(result.columns)
        }).eq('id', dataset_id).execute()
        
        logger.info(f"✅ Analysis complete: {dataset_id} | Rows: {row_count:,}")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")

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
        "persistence": "✅ Supabase + Local Storage",
        "storage_backend": "Azure Blob" if USE_AZURE else "Local Files",
        "endpoints": {
            "upload": "POST /upload",
            "datasets": "GET /datasets",
            "preview": "GET /datasets/{id}/preview",
            "query": "POST /query/sql"
        }
    }

@app.get("/health")
def health_check():
    """Health check"""
    return {
        "status": "healthy",
        "version": VERSION,
        "timestamp": datetime.now().isoformat(),
        "supabase": "connected",
        "storage": "Azure Blob" if USE_AZURE else "Local Files"
    }

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None
):
    """Upload CSV file - now saves to Supabase"""
    
    # Validate file
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files supported")
    
    try:
        # Read file content
        content = await file.read()
        file_size_bytes = len(content)
        
        if file_size_bytes > 10 * 1024 * 1024 * 1024:  # 10GB limit
            raise HTTPException(status_code=400, detail="File too large (max 10GB)")
        
        if file_size_bytes == 0:
            raise HTTPException(status_code=400, detail="File is empty")
        
        # Generate unique dataset ID
        dataset_id = str(uuid.uuid4())
        
        # Save file (locally for now)
        file_path = save_file_locally(dataset_id, content)
        
        # Save metadata to Supabase (matching YOUR schema exactly)
        dataset_record = {
            "id": dataset_id,
            "user_id": "bb98228c-2266-4510-91ad-d81fcbc74b93",  # TODO: Get from auth
            "filename": file.filename,
            "blob_path": file_path,  # YOUR column name
            "size_bytes": file_size_bytes,  # YOUR column name
            "row_count": 0,  # Will be filled by background task
            "column_count": 0,  # Will be filled by background task
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        supabase.table('datasets').insert(dataset_record).execute()
        
        # Trigger background analysis
        if background_tasks:
            background_tasks.add_task(analyze_dataset_background, dataset_id, file_path)
        
        logger.info(f"⚡ Uploaded: {file.filename} | {file_size_bytes} bytes | ID: {dataset_id}")
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "message": f"Uploaded {file.filename} - analyzing in background",
            "metadata": dataset_record
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/datasets")
def list_datasets():
    """List all datasets - now reads from Supabase"""
    datasets_list = list_datasets_from_db()
    
    return {
        "count": len(datasets_list),
        "datasets": datasets_list
    }

@app.get("/datasets/{dataset_id}")
def get_dataset(dataset_id: str):
    """Get specific dataset info - now reads from Supabase"""
    dataset = get_dataset_from_db(dataset_id)
    return dataset

@app.get("/datasets/{dataset_id}/preview")
def preview_dataset(dataset_id: str, limit: int = 100):
    """Get a preview of the dataset"""
    dataset = get_dataset_from_db(dataset_id)
    file_path = dataset["blob_path"]  # YOUR column name
    
    try:
        conn = duckdb.connect(':memory:')
        result = conn.execute(f"SELECT * FROM '{file_path}' LIMIT {limit}").fetchdf()
        conn.close()
        
        return {
            "dataset_id": dataset_id,
            "preview_rows": limit,
            "data": result.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")

@app.get("/datasets/{dataset_id}/rows")
def get_dataset_rows(
    dataset_id: str,
    offset: int = 0,
    limit: int = 1000
):
    """Get paginated rows from dataset"""
    dataset = get_dataset_from_db(dataset_id)
    file_path = dataset["blob_path"]  # YOUR column name
    
    try:
        conn = duckdb.connect(':memory:')
        result = conn.execute(
            f"SELECT * FROM '{file_path}' LIMIT {limit} OFFSET {offset}"
        ).fetchdf()
        conn.close()
        
        return {
            "dataset_id": dataset_id,
            "offset": offset,
            "limit": limit,
            "rows_returned": len(result),
            "data": result.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch rows: {str(e)}")

@app.post("/query/sql")
def execute_sql(query: SQLQuery):
    """Execute SQL query against a dataset"""
    if not query.dataset_id:
        raise HTTPException(status_code=400, detail="dataset_id is required")
    
    # Get dataset from Supabase
    dataset = get_dataset_from_db(query.dataset_id)
    file_path = dataset["blob_path"]  # YOUR column name
    
    try:
        conn = duckdb.connect(':memory:')
        
        # Create a view for easier querying
        conn.execute(f"CREATE VIEW data AS SELECT * FROM '{file_path}'")
        
        # Execute the user's query
        result = conn.execute(query.sql).fetchdf()
        conn.close()
        
        return {
            "success": True,
            "rows_returned": len(result),
            "columns": list(result.columns),
            "data": result.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query failed: {str(e)}")

@app.delete("/datasets/{dataset_id}")
def delete_dataset(dataset_id: str):
    """Delete a dataset"""
    dataset = get_dataset_from_db(dataset_id)
    
    try:
        # Delete file
        file_path = dataset["blob_path"]
        if os.path.exists(file_path):
            os.remove(file_path)
        
        # Delete from Supabase
        supabase.table('datasets').delete().eq('id', dataset_id).execute()
        
        logger.info(f"🗑️  Deleted dataset: {dataset_id}")
        
        return {
            "success": True,
            "message": f"Dataset {dataset_id} deleted successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@app.get("/export/{dataset_id}")
def export_dataset(dataset_id: str, format: str = "csv"):
    """Export dataset"""
    dataset = get_dataset_from_db(dataset_id)
    file_path = dataset["blob_path"]
    
    try:
        conn = duckdb.connect(':memory:')
        result = conn.execute(f"SELECT * FROM '{file_path}'").fetchdf()
        conn.close()
        
        if format == "csv":
            csv_buffer = io.StringIO()
            result.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return StreamingResponse(
                io.BytesIO(csv_buffer.getvalue().encode()),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={dataset['filename']}"
                }
            )
        
        elif format == "json":
            return result.to_dict(orient="records")
        
        else:
            raise HTTPException(status_code=400, detail="Unsupported export format. Use 'csv' or 'json'")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

# ============================================================================
# NLQ ENDPOINT - SECURED & HARDENED
# ============================================================================

# Security constants
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
    
    # Check for dangerous keywords
    for keyword in DANGEROUS_SQL_KEYWORDS:
        if keyword in sql_upper:
            return False, f"Dangerous SQL keyword detected: {keyword}"
    
    # Must be a SELECT query
    if not sql_upper.strip().startswith("SELECT"):
        return False, "Only SELECT queries are allowed"
    
    return True, ""

@app.post("/query/natural")
async def natural_language_query(nlq: NaturalLanguageQuery):
    """Convert natural language to SQL and execute - SECURED VERSION"""
    start_time = datetime.now()
    
    # Check if OpenAI key exists
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    if not OPENAI_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="OpenAI API key not configured. Please set OPENAI_API_KEY in .env"
        )
    
    try:
        # Get dataset from Supabase
        dataset = get_dataset_from_db(nlq.dataset_id)
        file_path = dataset["blob_path"]
        
        # STEP 1: Get schema with types
        conn = duckdb.connect(':memory:')
        
        try:
            # Get column types
            schema_query = f"DESCRIBE SELECT * FROM '{file_path}'"
            schema_info = conn.execute(schema_query).fetchdf()
            
            # Get sample data (3 rows)
            sample_query = f"SELECT * FROM '{file_path}' LIMIT 3"
            sample_data = conn.execute(sample_query).fetchdf()
            
            # Format schema
            schema_text = "\n".join([
                f"  - {row['column_name']} ({row['column_type']})"
                for _, row in schema_info.iterrows()
            ])
            
            sample_text = sample_data.to_string(index=False, max_rows=3)
            
        except Exception as e:
            logger.warning(f"Failed to get schema: {e}")
            schema_text = f"Columns: {', '.join(dataset.get('columns', []))}"
            sample_text = "(Sample data unavailable)"
        
        # STEP 2: Enhanced prompt with safety rules
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
        
        # STEP 3: Call OpenAI
        logger.info(f"Calling OpenAI for question: {nlq.question}")
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Faster and cheaper than gpt-4
                messages=[
                    {"role": "system", "content": "You are a SQL expert. Generate ONLY the SQL query - no markdown, no explanation."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=500
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up response (remove markdown if present)
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
            
            logger.info(f"Generated SQL: {sql_query}")
            
        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise HTTPException(status_code=500, detail=f"AI service error: {str(e)}")
        
        # STEP 4: Validate SQL safety
        is_safe, error_msg = validate_sql_safety(sql_query)
        if not is_safe:
            logger.warning(f"Blocked unsafe SQL: {sql_query}. Reason: {error_msg}")
            raise HTTPException(status_code=400, detail=f"Security violation: {error_msg}")
        
        # STEP 5: Set timeout and execute
        try:
            conn.execute(f"SET statement_timeout='{QUERY_TIMEOUT_SECONDS}s'")
            
            # Create view for easier querying
            conn.execute(f"CREATE VIEW data AS SELECT * FROM '{file_path}'")
            
            # Execute query
            result = conn.execute(sql_query).fetchdf()
            
            # STEP 6: Limit results
            truncated = len(result) > MAX_RESULT_ROWS
            if truncated:
                logger.warning(f"Results truncated: {len(result)} rows -> {MAX_RESULT_ROWS} rows")
                result = result[:MAX_RESULT_ROWS]
            
            conn.close()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "success": True,
                "sql_query": sql_query,
                "rows_returned": len(result),
                "truncated": truncated,
                "execution_time_seconds": round(execution_time, 2),
                "columns": list(result.columns),
                "data": result.to_dict(orient="records"),
                "truncated_message": f"Results limited to {MAX_RESULT_ROWS} rows" if truncated else None
            }
            
        except Exception as e:
            error_str = str(e)
            if "timeout" in error_str.lower():
                raise HTTPException(
                    status_code=408,
                    detail=f"Query timeout after {QUERY_TIMEOUT_SECONDS} seconds. Try a simpler query."
                )
            else:
                raise HTTPException(status_code=400, detail=f"Query execution failed: {error_str}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"NLQ request failed: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
