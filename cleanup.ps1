Write-Host "🧹 Cleaning up JetDB repository..." -ForegroundColor Cyan

# 1. Rename Dockerfiles (remove .txt extension)
if (Test-Path "backend\Dockerfile.txt") {
    Rename-Item "backend\Dockerfile.txt" "backend\Dockerfile"
    Write-Host "✅ Renamed backend\Dockerfile" -ForegroundColor Green
}

if (Test-Path "frontend\Dockerfile.txt") {
    Rename-Item "frontend\Dockerfile.txt" "frontend\Dockerfile"
    Write-Host "✅ Renamed frontend\Dockerfile" -ForegroundColor Green
}

# 2. Move misplaced .env.example
if (Test-Path "env.example") {
    Move-Item "env.example" "frontend\.env.example" -Force
    Write-Host "✅ Moved env.example to frontend\" -ForegroundColor Green
}

# 3. Remove sensitive test files from git
Write-Host "🔐 Removing sensitive test files..." -ForegroundColor Yellow
git rm --cached backend\test_complete_flow.py 2>$null
git rm --cached backend\test_login.py 2>$null
git rm --cached backend\test_state_endpoints.py 2>$null

# 4. Update .gitignore
Add-Content .gitignore "`nbackend/test_*.py"
Add-Content .gitignore "backend/uploads/"
Write-Host "✅ Updated .gitignore" -ForegroundColor Green

# 5. Remove unnecessary directories
if (Test-Path "backend\uploads") {
    git rm -rf backend\uploads 2>$null
    Write-Host "✅ Removed backend\uploads\" -ForegroundColor Green
}

if (Test-Path "scripts") {
    git rm -rf scripts 2>$null
    Write-Host "✅ Removed scripts\" -ForegroundColor Green
}

# 6. Verify critical files exist
Write-Host "`n📋 Verifying files..." -ForegroundColor Cyan
$criticalFiles = @(
    "backend\Dockerfile",
    "frontend\Dockerfile",
    "docker-compose.yml",
    "backend\.env.example",
    "frontend\.env.example",
    "backend\requirements.txt"
)

foreach ($file in $criticalFiles) {
    if (Test-Path $file) {
        Write-Host "✅ $file" -ForegroundColor Green
    } else {
        Write-Host "❌ $file MISSING" -ForegroundColor Red
    }
}

Write-Host "`n🎉 Cleanup complete!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "1. git add ."
Write-Host "2. git commit -m 'Fix: Remove tokens, rename Dockerfiles, clean structure'"
Write-Host "3. git push origin main"
Write-Host "4. ⚠️  IMMEDIATELY rotate Supabase keys!" -ForegroundColor Yellow