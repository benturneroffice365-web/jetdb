# ============================================================================
# FILE 1: backend/supabase_helpers.py
# All Supabase database operations with error handling
# ============================================================================

from supabase import create_client, Client
from typing import Optional, Dict, Any, List
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def get_supabase_client() -> Client:
    """Get Supabase client instance"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase credentials not configured")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================================
# DATASET OPERATIONS
# ============================================================================

async def create_dataset(
    user_id: str,
    filename: str,
    blob_path: str,
    size_bytes: int,
    row_count: int,
    column_count: int
) -> Dict[str, Any]:
    """
    Create a new dataset record in Supabase
    
    Args:
        user_id: User who owns the dataset
        filename: Original filename
        blob_path: Path in Azure Blob Storage
        size_bytes: File size in bytes
        row_count: Number of rows in dataset
        column_count: Number of columns in dataset
    
    Returns:
        Created dataset record
    """
    try:
        supabase = get_supabase_client()
        
        data = {
            "user_id": user_id,
            "filename": filename,
            "blob_path": blob_path,
            "size_bytes": size_bytes,
            "row_count": row_count,
            "column_count": column_count,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = supabase.table("datasets").insert(data).execute()
        
        if result.data and len(result.data) > 0:
            logger.info(f"Created dataset: {result.data[0]['id']} for user {user_id}")
            return result.data[0]
        else:
            raise Exception("Failed to create dataset: no data returned")
            
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        raise

async def get_dataset(dataset_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get dataset by ID with ownership verification
    
    Args:
        dataset_id: Dataset UUID
        user_id: User requesting the dataset
    
    Returns:
        Dataset record or None if not found/not owned
    """
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("datasets")\
            .select("*")\
            .eq("id", dataset_id)\
            .eq("user_id", user_id)\
            .execute()
        
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
        
    except Exception as e:
        logger.error(f"Error getting dataset {dataset_id}: {str(e)}")
        raise

async def list_datasets(user_id: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    List all datasets for a user
    
    Args:
        user_id: User ID
        limit: Max results to return
        offset: Pagination offset
    
    Returns:
        List of dataset records
    """
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("datasets")\
            .select("*")\
            .eq("user_id", user_id)\
            .order("created_at", desc=True)\
            .range(offset, offset + limit - 1)\
            .execute()
        
        return result.data if result.data else []
        
    except Exception as e:
        logger.error(f"Error listing datasets for user {user_id}: {str(e)}")
        raise

async def delete_dataset(dataset_id: str, user_id: str) -> bool:
    """
    Delete a dataset (with ownership verification)
    
    Args:
        dataset_id: Dataset UUID
        user_id: User requesting deletion
    
    Returns:
        True if deleted, False if not found
    """
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("datasets")\
            .delete()\
            .eq("id", dataset_id)\
            .eq("user_id", user_id)\
            .execute()
        
        deleted = result.data and len(result.data) > 0
        if deleted:
            logger.info(f"Deleted dataset {dataset_id}")
        return deleted
        
    except Exception as e:
        logger.error(f"Error deleting dataset {dataset_id}: {str(e)}")
        raise

async def verify_dataset_ownership(dataset_id: str, user_id: str) -> bool:
    """
    Verify that a user owns a dataset
    
    Args:
        dataset_id: Dataset UUID
        user_id: User ID to verify
    
    Returns:
        True if user owns dataset, False otherwise
    """
    dataset = await get_dataset(dataset_id, user_id)
    return dataset is not None

# ============================================================================
# SPREADSHEET STATE OPERATIONS
# ============================================================================

async def save_spreadsheet_state(
    dataset_id: str,
    user_id: str,
    state_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Save spreadsheet state (cell edits, formulas, formatting)
    
    Args:
        dataset_id: Dataset UUID
        user_id: User ID
        state_data: JSON object with spreadsheet state
    
    Returns:
        Saved state record
    """
    try:
        supabase = get_supabase_client()
        
        # Verify dataset ownership first
        if not await verify_dataset_ownership(dataset_id, user_id):
            raise PermissionError(f"User {user_id} does not own dataset {dataset_id}")
        
        data = {
            "dataset_id": dataset_id,
            "user_id": user_id,
            "state_data": state_data,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Upsert (insert or update if exists)
        result = supabase.table("spreadsheet_states")\
            .upsert(data, on_conflict="dataset_id,user_id")\
            .execute()
        
        if result.data and len(result.data) > 0:
            logger.info(f"Saved spreadsheet state for dataset {dataset_id}")
            return result.data[0]
        else:
            raise Exception("Failed to save spreadsheet state")
            
    except Exception as e:
        logger.error(f"Error saving spreadsheet state: {str(e)}")
        raise

async def load_spreadsheet_state(
    dataset_id: str,
    user_id: str
) -> Optional[Dict[str, Any]]:
    """
    Load spreadsheet state for a dataset
    
    Args:
        dataset_id: Dataset UUID
        user_id: User ID
    
    Returns:
        State data or None if not found
    """
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("spreadsheet_states")\
            .select("state_data, updated_at")\
            .eq("dataset_id", dataset_id)\
            .eq("user_id", user_id)\
            .execute()
        
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
        
    except Exception as e:
        logger.error(f"Error loading spreadsheet state: {str(e)}")
        raise

async def clear_spreadsheet_state(dataset_id: str, user_id: str) -> bool:
    """
    Clear spreadsheet state for a dataset
    
    Args:
        dataset_id: Dataset UUID
        user_id: User ID
    
    Returns:
        True if cleared, False if not found
    """
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("spreadsheet_states")\
            .delete()\
            .eq("dataset_id", dataset_id)\
            .eq("user_id", user_id)\
            .execute()
        
        cleared = result.data and len(result.data) > 0
        if cleared:
            logger.info(f"Cleared spreadsheet state for dataset {dataset_id}")
        return cleared
        
    except Exception as e:
        logger.error(f"Error clearing spreadsheet state: {str(e)}")
        raise

# ============================================================================
# USAGE TRACKING (for rate limiting)
# ============================================================================

async def increment_upload_count(user_id: str) -> int:
    """
    Increment user's upload count for rate limiting
    
    Args:
        user_id: User ID
    
    Returns:
        Current upload count
    """
    try:
        supabase = get_supabase_client()
        
        # Get current count
        result = supabase.table("user_usage")\
            .select("upload_count")\
            .eq("user_id", user_id)\
            .execute()
        
        if result.data and len(result.data) > 0:
            current_count = result.data[0].get("upload_count", 0)
            new_count = current_count + 1
            
            # Update count
            supabase.table("user_usage")\
                .update({"upload_count": new_count, "updated_at": datetime.utcnow().isoformat()})\
                .eq("user_id", user_id)\
                .execute()
        else:
            # Create new record
            new_count = 1
            supabase.table("user_usage")\
                .insert({"user_id": user_id, "upload_count": new_count})\
                .execute()
        
        return new_count
        
    except Exception as e:
        logger.error(f"Error incrementing upload count: {str(e)}")
        # Don't fail the request if usage tracking fails
        return 0