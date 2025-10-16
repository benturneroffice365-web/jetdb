#backend/
#  tests/
#    __init__.py  (empty file)
#   test_security.py

pythonimport pytest
from fastapi.testclient import TestClient
from main import app, validate_sql_safety

client = TestClient(app)

def test_sql_injection_blocked():
    """Test that SQL injection attempts are blocked"""
    malicious_queries = [
        "SELECT * FROM data; DROP TABLE users;",
        "SELECT * FROM data WHERE id = 1 OR 1=1",
        "SELECT * FROM data; DELETE FROM users;",
        "SELECT * FROM data UNION SELECT * FROM users"
    ]
    
    for query in malicious_queries:
        is_safe, _ = validate_sql_safety(query)
        assert not is_safe, f"Should block: {query}"

def test_cors_policy():
    """Test CORS is restricted to allowed origins"""
    response = client.get("/health", headers={"Origin": "https://evil.com"})
    assert "access-control-allow-origin" not in response.headers or \
           response.headers.get("access-control-allow-origin") != "https://evil.com"

def test_rate_limiting():
    """Test rate limits are enforced"""
    # Make 11 rapid requests (limit is 10/minute)
    for i in range(11):
        response = client.post("/query/sql", json={
            "sql": "SELECT 1",
            "dataset_id": "test"
        })
    
    assert response.status_code == 429
    assert "too many requests" in response.json()["detail"].lower()

def test_file_upload_validation():
    """Test malicious files are blocked"""
    files = {
        "file": ("malware.exe", b"MZ\x90\x00", "application/x-msdownload")
    }
    response = client.post("/upload", files=files)
    assert response.status_code == 400
    assert "invalid file type" in response.json()["detail"].lower()

def test_legitimate_sql_allowed():
    """Test that valid SELECT queries work"""
    valid_queries = [
        "SELECT * FROM data LIMIT 100",
        "SELECT COUNT(*) FROM data",
        "SELECT revenue, COUNT(*) FROM data GROUP BY revenue",
        "WITH cte AS (SELECT * FROM data) SELECT * FROM cte"
    ]
    
    for query in valid_queries:
        is_safe, _ = validate_sql_safety(query)
        assert is_safe, f"Should allow: {query}"

# Run with: pytest backend/tests/test_security.py -v