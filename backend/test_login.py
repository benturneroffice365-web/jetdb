import requests

SUPABASE_URL = "https://lhwmnjhdqrqrouxjtpln.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxod21uamhkcXJxcm91eGp0cGxuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTk4MDcxNjUsImV4cCI6MjA3NTM4MzE2NX0.TKxyN27JPHXE27kI2CnKHSeGU9fLSlxqfJHKE66THwo"

response = requests.post(
    f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
    headers={
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json"
    },
    json={
        "email": "test@jetdb.com",
        "password": "password"
    }
)

if response.status_code == 200:
    data = response.json()
    print("\n✅ Login successful!")
    print(f"\nAccess Token:")
    print(data['access_token'])
    print(f"\n\nUser ID: {data['user']['id']}")
else:
    print(f"❌ Login failed: {response.status_code}")
    print(response.text)