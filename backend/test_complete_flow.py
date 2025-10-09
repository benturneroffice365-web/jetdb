import requests
import json

API_BASE = "http://localhost:8000"
TOKEN = "eyJhbGciOiJIUzI1NiIsImtpZCI6IlJHeHRPU0cxZnV0VlVnUEciLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2xod21uamhkcXJxcm91eGp0cGxuLnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiJiYjk4MjI4Yy0yMjY2LTQ1MTAtOTFhZC1kODFmY2JjNzRiOTMiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzYwMDE5NDY2LCJpYXQiOjE3NjAwMTU4NjYsImVtYWlsIjoidGVzdEBqZXRkYi5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp7ImVtYWlsX3ZlcmlmaWVkIjp0cnVlfSwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJhYWwiOiJhYWwxIiwiYW1yIjpbeyJtZXRob2QiOiJwYXNzd29yZCIsInRpbWVzdGFtcCI6MTc2MDAxNTg2Nn1dLCJzZXNzaW9uX2lkIjoiNGZhMzA5ZDYtM2U4Ni00MmRkLTkyYzQtYzQ1N2NjYWU5MzZjIiwiaXNfYW5vbnltb3VzIjpmYWxzZX0.fWOBPKSGB3tz5Ifjpo26dUYmaae0pIhfR-RKupRUqaM"

# Use the dataset we created manually in Supabase
DATASET_ID = "36a629f3-00a0-487a-83af-0cd189826b16"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

print("\n" + "="*70)
print("JetDB Spreadsheet State - Complete End-to-End Test")
print("="*70)

print(f"\nUsing existing dataset: {DATASET_ID}")

# Step 1: Save spreadsheet state
print("\n[Step 1] Saving spreadsheet state...")
state_data = {
    "cells": {
        "A1": {"value": "Hello", "formula": None},
        "A2": {"value": "World", "formula": None},
        "B1": {"value": 100, "formula": None},
        "B2": {"value": 200, "formula": "=B1*2"}
    },
    "formatting": {
        "A1": {"bold": True, "color": "#FF0000"},
        "B1": {"bold": True, "color": "#0000FF"}
    },
    "columnWidths": {
        "A": 150,
        "B": 120
    }
}

save_response = requests.post(
    f"{API_BASE}/datasets/{DATASET_ID}/save-state",
    headers=headers,
    json={"state_data": state_data}
)

print(f"Status: {save_response.status_code}")
if save_response.status_code == 200:
    print("SUCCESS: State saved successfully!")
    print(f"Response: {json.dumps(save_response.json(), indent=2)}")
else:
    print(f"FAILED: Save failed!")
    print(f"Response: {save_response.text}")

# Step 2: Load spreadsheet state
print("\n[Step 2] Loading spreadsheet state...")
load_response = requests.get(
    f"{API_BASE}/datasets/{DATASET_ID}/load-state",
    headers=headers
)

print(f"Status: {load_response.status_code}")
if load_response.status_code == 200:
    print("SUCCESS: State loaded successfully!")
    loaded_data = load_response.json()
    cells_count = len(loaded_data.get('state_data', {}).get('cells', {}))
    print(f"Cells loaded: {cells_count}")
    print(f"Response: {json.dumps(loaded_data, indent=2)}")
else:
    print(f"FAILED: Load failed!")
    print(f"Response: {load_response.text}")

# Step 3: Verify data matches
if save_response.status_code == 200 and load_response.status_code == 200:
    print("\n[Step 3] Verifying data integrity...")
    saved_cells = state_data["cells"]
    loaded_cells = load_response.json()["state_data"]["cells"]
    
    if saved_cells == loaded_cells:
        print("SUCCESS: Data integrity verified! Saved and loaded data match.")
    else:
        print("WARNING: Data mismatch detected!")

# Step 4: Clear state
print("\n[Step 4] Clearing spreadsheet state...")
clear_response = requests.delete(
    f"{API_BASE}/datasets/{DATASET_ID}/clear-state",
    headers=headers
)

print(f"Status: {clear_response.status_code}")
if clear_response.status_code == 200:
    print("SUCCESS: State cleared successfully!")
else:
    print(f"FAILED: Clear failed!")
    print(f"Response: {clear_response.text}")

# Step 5: Try to load again (should fail or return empty)
print("\n[Step 5] Verifying state was cleared...")
verify_response = requests.get(
    f"{API_BASE}/datasets/{DATASET_ID}/load-state",
    headers=headers
)

print(f"Status: {verify_response.status_code}")
if verify_response.status_code == 404:
    print("SUCCESS: Confirmed! State no longer exists.")
elif verify_response.status_code == 200:
    print("WARNING: State still exists after clear!")
else:
    print(f"Response: {verify_response.text}")

print("\n" + "="*70)
print("All tests completed!")
print("="*70 + "\n")