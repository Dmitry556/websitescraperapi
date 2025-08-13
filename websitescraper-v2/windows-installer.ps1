# WebsiteScraper v2 - Windows One-Click Installer
# This script does EVERYTHING automatically

Write-Host "🚀 WebsiteScraper v2 Windows Installer" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green

# Check if running as administrator
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator"))
{
    Write-Host "❌ This script needs to run as Administrator for desktop shortcut creation." -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator', then run this script again." -ForegroundColor Yellow
    pause
    exit 1
}

try {
    # 1. Navigate to the project directory
    Write-Host "📂 Setting up project directory..." -ForegroundColor Yellow
    $ProjectPath = "C:\Users\$env:USERNAME\websitescraperapi\websitescraper-v2"
    
    if (!(Test-Path $ProjectPath)) {
        Write-Host "❌ Project not found at $ProjectPath" -ForegroundColor Red
        Write-Host "Please run: git clone https://github.com/Dmitry556/websitescraperapi.git" -ForegroundColor Yellow
        pause
        exit 1
    }
    
    Set-Location $ProjectPath
    
    # 2. Install dependencies if not already installed
    Write-Host "📦 Checking dependencies..." -ForegroundColor Yellow
    if (!(Test-Path "node_modules")) {
        Write-Host "Installing Node.js dependencies..." -ForegroundColor Yellow
        npm install
        npm install playwright @playwright/test dotenv
        npx playwright install chromium
    }
    
    # 3. Build the project
    Write-Host "🔨 Building project..." -ForegroundColor Yellow
    npm run build
    
    # 4. Create environment file with Redis URL
    Write-Host "⚙️ Setting up environment..." -ForegroundColor Yellow
    $EnvContent = @"
# Redis connection (Upstash Redis)
REDIS_URL=rediss://default:AXrwAAIjcDFlYjhlNzM0YWY5ZWE0NzYxYTNiM2ZmZWQxYjUxZmYwOXAxMA@needed-dingo-31472.upstash.io:6379

# Worker configuration (optimized for Lenovo Legion 5 Pro)
MAX_CONCURRENT_JOBS=20
PROXY_FILE=webshare-proxies.txt

# Timeouts and retries
DEFAULT_TIMEOUT_MS=12000
MAX_RETRIES=3
RETRY_DELAY_MS=2000

# Reliability settings
HEARTBEAT_INTERVAL_MS=30000
PROXY_ROTATION_THRESHOLD=3
MEMORY_LIMIT_MB=8000

# Monitoring
LOG_LEVEL=info
STATS_INTERVAL_MS=60000
"@
    $EnvContent | Out-File -FilePath ".env" -Encoding UTF8
    
    # 5. Create the batch file launcher
    Write-Host "🎯 Creating worker launcher..." -ForegroundColor Yellow
    $BatchContent = @"
@echo off
title WebsiteScraper v2 Worker - Production Ready
color 0A
echo.
echo  ██╗    ██╗███████╗██████╗ ███████╗██╗████████╗███████╗
echo  ██║    ██║██╔════╝██╔══██╗██╔════╝██║╚══██╔══╝██╔════╝
echo  ██║ █╗ ██║█████╗  ██████╔╝███████╗██║   ██║   █████╗  
echo  ██║███╗██║██╔══╝  ██╔══██╗╚════██║██║   ██║   ██╔══╝  
echo  ╚███╔███╔╝███████╗██████╔╝███████║██║   ██║   ███████╗
echo   ╚══╝╚══╝ ╚══════╝╚═════╝ ╚══════╝╚═╝   ╚═╝   ╚══════╝
echo.
echo                 SCRAPER v2 - PRODUCTION WORKER
echo                      Ready for 5,000 URLs
echo.
cd /d "$ProjectPath"
set REDIS_URL=rediss://default:AXrwAAIjcDFlYjhlNzM0YWY5ZWE0NzYxYTNiM2ZmZWQxYjUxZmYwOXAxMA@needed-dingo-31472.upstash.io:6379
echo 🚀 Starting WebsiteScraper v2 Worker...
echo ⚡ Connecting to 20 US proxies...
echo 📡 Redis queue ready...
echo.
npm run start:worker
if errorlevel 1 (
    echo.
    echo ❌ Worker failed to start. Check the error above.
    echo.
    pause
) else (
    echo.
    echo ✅ Worker stopped normally.
    pause
)
"@
    $BatchContent | Out-File -FilePath "WebsiteScraper-Worker.bat" -Encoding ASCII
    
    # 6. Create desktop shortcut with custom icon
    Write-Host "🖥️ Creating desktop shortcut..." -ForegroundColor Yellow
    $DesktopPath = [Environment]::GetFolderPath("Desktop")
    $WshShell = New-Object -comObject WScript.Shell
    $Shortcut = $WshShell.CreateShortcut("$DesktopPath\🚀 WebsiteScraper Worker.lnk")
    $Shortcut.TargetPath = "$ProjectPath\WebsiteScraper-Worker.bat"
    $Shortcut.WorkingDirectory = $ProjectPath
    $Shortcut.Description = "WebsiteScraper v2 Worker - Click to start processing Clay jobs"
    $Shortcut.WindowStyle = 1  # Normal window
    $Shortcut.Save()
    
    # 7. Create a quick status checker script
    Write-Host "📊 Creating status checker..." -ForegroundColor Yellow
    $StatusContent = @"
@echo off
title WebsiteScraper v2 - System Status
color 0B
echo.
echo 📊 WebsiteScraper v2 System Status
echo ===================================
echo.
cd /d "$ProjectPath"
set REDIS_URL=rediss://default:AXrwAAIjcDFlYjhlNzM0YWY5ZWE0NzYxYTNiM2ZmZWQxYjUxZmYwOXAxMA@needed-dingo-31472.upstash.io:6379
echo Checking system health...
npm run health 2>nul
if errorlevel 1 (
    echo ❌ System check failed - worker may not be running
) else (
    echo ✅ System is healthy
)
echo.
pause
"@
    $StatusContent | Out-File -FilePath "WebsiteScraper-Status.bat" -Encoding ASCII
    
    # 8. Create another desktop shortcut for status checker
    $StatusShortcut = $WshShell.CreateShortcut("$DesktopPath\📊 WebsiteScraper Status.lnk")
    $StatusShortcut.TargetPath = "$ProjectPath\WebsiteScraper-Status.bat"
    $StatusShortcut.WorkingDirectory = $ProjectPath
    $StatusShortcut.Description = "Check WebsiteScraper v2 system status"
    $StatusShortcut.Save()
    
    Write-Host ""
    Write-Host "🎉 INSTALLATION COMPLETE!" -ForegroundColor Green
    Write-Host "=========================" -ForegroundColor Green
    Write-Host ""
    Write-Host "✅ Two shortcuts created on your desktop:" -ForegroundColor White
    Write-Host "   🚀 WebsiteScraper Worker  - Click to START worker" -ForegroundColor Yellow
    Write-Host "   📊 WebsiteScraper Status  - Click to CHECK status" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "✅ Your Vercel API is ready at:" -ForegroundColor White
    Write-Host "   https://api-k5n618zpj-dmytropinchuk1-gmailcoms-projects.vercel.app" -ForegroundColor Blue
    Write-Host ""
    Write-Host "✅ Auth token for Clay:" -ForegroundColor White
    Write-Host "   WSCRAPER_PROD_6q2dVNYxJ4" -ForegroundColor Green
    Write-Host ""
    Write-Host "🎯 Ready to process 5,000 URLs in Clay!" -ForegroundColor Magenta
    Write-Host ""
    Write-Host "Press any key to exit..." -ForegroundColor Gray
    
} catch {
    Write-Host "❌ Installation failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Please try running as Administrator" -ForegroundColor Yellow
    pause
    exit 1
}

pause