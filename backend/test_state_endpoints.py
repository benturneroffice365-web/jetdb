import requests
import json

API_BASE = "http://localhost:8000"
TOKEN = "eyJhbGciOiJIUzI1NiIsImtpZCI6IlJHeHRPU0cxZnV0VlVnUEciLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2xod21uamhkcXJxcm91eGp0cGxuLnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiJiYjk4MjI4Yy0yMjY2LTQ1MTAtOTFhZC1kODFmY2JjNzRiOTMiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzYwMDE5NDY2LCJpYXQiOjE3NjAwMTU4NjYsImVtYWlsIjoidGVzdEBqZXRkYi5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp6eyJlbWFpbF92ZXJpZmllZCI6dHJ1ZX0sInJvbGUiOiJhdXRoZW50aWNhdGVkIiwiYWFsIjoiYWFsMSIsImFtciI6W3sibWV0aG9kIjoicGFzc3dvcmQiLCJ0aW1lc3RhbXAiOjE3NjAwMTU4NjZ9XSwic2Vzc2lvbl9pZCI6IjRmYTMwOWQ2LTNlODYtNDJkZC05MmM0LWM0NTdjY2FlOTM2YyIsImlzX2Fub255bW91cyI6ZmFsc2V9.fWOBPKSGB3tz5Ifjpo26dUYmaae0pIhfR-RKupRUqaM"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Test dataset ID (we'll need a real one from your Supabase)
DATASET_ID = "test-dataset-123"

print("\n" + "="*60)
print("Testing JetDB Spreadsheet State Endpoints")
print("="*60)

# Test 1: Save State
print("\n📝 Test 1: Save spreadsheet state...")
state_data = {
    "cells": {
        "A1": {"value": "Hello", "formula": None},
        "B1": {"value": 100, "formula": "=A1*2"}
    },
    "formatting": {
        "A1": {"bold": True, "color": "#FF0000"}
    }
}

response = requests.post(
    f"{API_BASE}/datasets/{DATASET_ID}/save-state",
    headers=headers,
    json={"state_data": state_data}
)

print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}")

# Test 2: Load State
print("\n📖 Test 2: Load spreadsheet state...")
response = requests.get(
    f"{API_BASE}/datasets/{DATASET_ID}/load-state",
    headers=headers
)

print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}")

# Test 3: Clear State
print("\n🗑️  Test 3: Clear spreadsheet state...")
response = requests.delete(
    f"{API_BASE}/datasets/{DATASET_ID}/clear-state",
    headers=headers
)

print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}")

print("\n" + "="*60)
print("✅ All tests completed!")
print("="*60 + "\n")