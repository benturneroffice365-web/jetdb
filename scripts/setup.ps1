14. scripts/setup.ps1 (Windows PowerShell)
powershellWrite-Host "ðŸš€ JetDB v8.0 Setup Script" -ForegroundColor Cyan
Write-Host "================================`n" -ForegroundColor Cyan

# Backend setup
Write-Host "ðŸ“¦ Setting up backend..." -ForegroundColor Yellow
cd backend
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
Write-Host "âœ… Backend setup complete!`n" -ForegroundColor Green

# Frontend setup
Write-Host "ðŸ“¦ Setting up frontend..." -ForegroundColor Yellow
cd ..\frontend
npm install
Copy-Item .env.local.example .env.local
Write-Host "âœ… Frontend setup complete!`n" -ForegroundColor Green

Write-Host "ðŸŽ‰ Setup complete!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "1. Edit backend/.env with your API keys"
Write-Host "2. Edit frontend/.env.local with your config"
Write-Host "3. Run backend: cd backend && uvicorn main:app --reload"
Write-Host "4. Run frontend: cd frontend && npm start"