# ============================================================================
# FILE 4: backend/state_endpoints.py
# Spreadsheet state persistence endpoints
# ============================================================================

from fastapi import APIRouter, Request, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any, Optional
import logging
from supabase_helpers import (
    save_spreadsheet_state,
    load_spreadsheet_state,
    clear_spreadsheet_state,
    verify_dataset_ownership
)
from error_handlers import NotFoundError, AuthorizationError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datasets", tags=["spreadsheet_state"])

# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class SaveStateRequest(BaseModel):
    """Request model for saving spreadsheet state"""
    state_data: Dict[str, Any]
    
    class Config:
        json_schema_extra = {
            "example": {
                "state_data": {
                    "cells": {
                        "A1": {"value": "Updated Value", "formula": None},
                        "B2": {"value": 150, "formula": "=A1*2"}
                    },
                    "formatting": {
                        "A1": {"bold": True, "color": "#FF0000"}
                    },
                    "columnWidths": {
                        "A": 120,
                        "B": 100
                    }
                }
            }
        }

class StateResponse(BaseModel):
    """Response model for spreadsheet state"""
    dataset_id: str
    state_data: Dict[str, Any]
    updated_at: str

class StateMetadata(BaseModel):
    """Response model for state metadata"""
    has_state: bool
    updated_at: Optional[str] = None

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_user_id(request: Request) -> str:
    """
    Extract user ID from authenticated request
    TODO: Implement actual auth token parsing
    """
    # For now, this is a placeholder
    # In production, you'd extract this from JWT token
    user_id = getattr(request.state, "user_id", None)
    
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required"
        )
    
    return user_id

# ============================================================================
# ENDPOINTS
# ============================================================================

@router.post("/{dataset_id}/save-state", response_model=StateResponse)
async def save_state(
    dataset_id: str,
    request: Request,
    body: SaveStateRequest
):
    """
    Save spreadsheet state (cell edits, formulas, formatting)
    
    **Rate Limit:** 100 saves per hour per user
    **Auth Required:** Yes
    
    **Request Body:**
    ```json
    {
        "state_data": {
            "cells": {...},
            "formatting": {...},
            "columnWidths": {...}
        }
    }
    ```
    
    **Response:**
    - 200: State saved successfully
    - 401: Authentication required
    - 403: User doesn't own dataset
    - 404: Dataset not found
    - 500: Database error
    """
    try:
        user_id = get_user_id(request)
        request_id = getattr(request.state, "request_id", "unknown")
        
        logger.info(
            f"Saving spreadsheet state for dataset {dataset_id}",
            extra={
                "request_id": request_id,
                "user_id": user_id,
                "dataset_id": dataset_id
            }
        )
        
        # Save state
        result = await save_spreadsheet_state(
            dataset_id=dataset_id,
            user_id=user_id,
            state_data=body.state_data
        )
        
        return StateResponse(
            dataset_id=dataset_id,
            state_data=result["state_data"],
            updated_at=result["updated_at"]
        )
        
    except PermissionError as e:
        logger.warning(f"Permission denied: {str(e)}")
        raise AuthorizationError("FORBIDDEN_DATASET_ACCESS")
    except Exception as e:
        logger.error(f"Error saving state: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to save spreadsheet state")

@router.get("/{dataset_id}/load-state", response_model=StateResponse)
async def load_state(
    dataset_id: str,
    request: Request
):
    """
    Load spreadsheet state for a dataset
    
    **Auth Required:** Yes
    
    **Response:**
    - 200: State loaded successfully
    - 401: Authentication required
    - 403: User doesn't own dataset
    - 404: No saved state found
    - 500: Database error
    """
    try:
        user_id = get_user_id(request)
        request_id = getattr(request.state, "request_id", "unknown")
        
        logger.info(
            f"Loading spreadsheet state for dataset {dataset_id}",
            extra={
                "request_id": request_id,
                "user_id": user_id,
                "dataset_id": dataset_id
            }
        )
        
        # Verify ownership first
        has_access = await verify_dataset_ownership(dataset_id, user_id)
        if not has_access:
            raise AuthorizationError("FORBIDDEN_DATASET_ACCESS")
        
        # Load state
        result = await load_spreadsheet_state(
            dataset_id=dataset_id,
            user_id=user_id
        )
        
        if not result:
            raise NotFoundError("STATE_NOT_FOUND")
        
        return StateResponse(
            dataset_id=dataset_id,
            state_data=result["state_data"],
            updated_at=result["updated_at"]
        )
        
    except (AuthorizationError, NotFoundError):
        raise
    except Exception as e:
        logger.error(f"Error loading state: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to load spreadsheet state")

@router.delete("/{dataset_id}/clear-state")
async def clear_state(
    dataset_id: str,
    request: Request
):
    """
    Clear spreadsheet state (reset to original CSV data)
    
    **Auth Required:** Yes
    
    **Response:**
    - 200: State cleared successfully
    - 401: Authentication required
    - 403: User doesn't own dataset
    - 404: No saved state found
    - 500: Database error
    """
    try:
        user_id = get_user_id(request)
        request_id = getattr(request.state, "request_id", "unknown")
        
        logger.info(
            f"Clearing spreadsheet state for dataset {dataset_id}",
            extra={
                "request_id": request_id,
                "user_id": user_id,
                "dataset_id": dataset_id
            }
        )
        
        # Clear state
        cleared = await clear_spreadsheet_state(
            dataset_id=dataset_id,
            user_id=user_id
        )
        
        if not cleared:
            raise NotFoundError("STATE_NOT_FOUND")
        
        return {
            "success": True,
            "message": "Spreadsheet state cleared successfully",
            "dataset_id": dataset_id
        }
        
    except NotFoundError:
        raise
    except Exception as e:
        logger.error(f"Error clearing state: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to clear spreadsheet state")

@router.get("/{dataset_id}/state-metadata", response_model=StateMetadata)
async def get_state_metadata(
    dataset_id: str,
    request: Request
):
    """
    Check if a dataset has saved state (without loading the full state)
    
    **Auth Required:** Yes
    
    **Response:**
    - 200: Metadata returned
    - 401: Authentication required
    - 403: User doesn't own dataset
    """
    try:
        user_id = get_user_id(request)
        
        # Verify ownership
        has_access = await verify_dataset_ownership(dataset_id, user_id)
        if not has_access:
            raise AuthorizationError("FORBIDDEN_DATASET_ACCESS")
        
        # Load state (just to check if exists)
        result = await load_spreadsheet_state(dataset_id, user_id)
        
        if result:
            return StateMetadata(
                has_state=True,
                updated_at=result["updated_at"]
            )
        else:
            return StateMetadata(has_state=False)
        
    except AuthorizationError:
        raise
    except Exception as e:
        logger.error(f"Error getting state metadata: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get state metadata")