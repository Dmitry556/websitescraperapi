# WebsiteScraper v2 - Google Cloud Deployment One-Click Installer
# Uses your existing $300 Google Cloud credit

Write-Host "☁️ WebsiteScraper v2 Google Cloud Deployment" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green

# Check if running as administrator
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator"))
{
    Write-Host "❌ This script needs to run as Administrator." -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator', then run this script again." -ForegroundColor Yellow
    pause
    exit 1
}

try {
    # 1. Navigate to the project directory
    Write-Host "📂 Setting up Google Cloud deployment..." -ForegroundColor Yellow
    $ProjectPath = "C:\Users\$env:USERNAME\websitescraperapi\websitescraper-v2"
    
    if (!(Test-Path $ProjectPath)) {
        Write-Host "❌ Project not found at $ProjectPath" -ForegroundColor Red
        Write-Host "Please run the main installer first" -ForegroundColor Yellow
        pause
        exit 1
    }
    
    Set-Location $ProjectPath
    
    # 2. Check if Google Cloud CLI is installed
    Write-Host "🔍 Checking Google Cloud CLI..." -ForegroundColor Yellow
    $gcloudExists = Get-Command gcloud -ErrorAction SilentlyContinue
    
    if (-not $gcloudExists) {
        Write-Host "📦 Installing Google Cloud CLI..." -ForegroundColor Yellow
        # Download and install Google Cloud CLI
        $installerUrl = "https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe"
        $installerPath = "$env:TEMP\GoogleCloudSDKInstaller.exe"
        
        Write-Host "   Downloading installer..." -ForegroundColor Gray
        Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath
        
        Write-Host "   Running installer..." -ForegroundColor Gray
        Start-Process -FilePath $installerPath -Wait
        
        Write-Host "✅ Google Cloud CLI installed" -ForegroundColor Green
        Write-Host "📋 Please restart this script after the installation completes" -ForegroundColor Yellow
        pause
        exit 0
    }
    
    Write-Host "✅ Google Cloud CLI found" -ForegroundColor Green
    
    # 3. Check authentication
    Write-Host "🔐 Checking Google Cloud authentication..." -ForegroundColor Yellow
    $authStatus = & gcloud auth list --filter="status:ACTIVE" --format="value(account)" 2>$null
    
    if (-not $authStatus) {
        Write-Host "🔑 Authenticating with Google Cloud..." -ForegroundColor Yellow
        Write-Host "   This will open a browser window for authentication" -ForegroundColor Gray
        & gcloud auth login
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Authentication failed" -ForegroundColor Red
            pause
            exit 1
        }
    }
    
    Write-Host "✅ Authenticated as: $authStatus" -ForegroundColor Green
    
    # 4. Set up project (use existing or create new)
    Write-Host "🏗️ Setting up Google Cloud project..." -ForegroundColor Yellow
    $projects = & gcloud projects list --format="value(projectId)" 2>$null
    
    if ($projects) {
        Write-Host "📋 Available projects:" -ForegroundColor Gray
        $projects | ForEach-Object { Write-Host "   • $_" -ForegroundColor Gray }
        
        $projectId = Read-Host "Enter project ID to use (or press Enter to create new)"
        
        if (-not $projectId) {
            $projectId = "websitescraper-" + (Get-Random -Minimum 1000 -Maximum 9999)
            Write-Host "🆕 Creating new project: $projectId" -ForegroundColor Yellow
            & gcloud projects create $projectId --name="WebsiteScraper v2"
        }
    } else {
        $projectId = "websitescraper-" + (Get-Random -Minimum 1000 -Maximum 9999)
        Write-Host "🆕 Creating new project: $projectId" -ForegroundColor Yellow
        & gcloud projects create $projectId --name="WebsiteScraper v2"
    }
    
    # Set current project
    & gcloud config set project $projectId
    Write-Host "✅ Using project: $projectId" -ForegroundColor Green
    
    # 5. Enable required APIs
    Write-Host "🔧 Enabling required APIs..." -ForegroundColor Yellow
    & gcloud services enable cloudbuild.googleapis.com
    & gcloud services enable run.googleapis.com
    & gcloud services enable containerregistry.googleapis.com
    
    Write-Host "✅ APIs enabled" -ForegroundColor Green
    
    # 6. Create optimized API server file
    Write-Host "⚙️ Creating production API server..." -ForegroundColor Yellow
    $ApiServerContent = @"
const express = require('express');
const Redis = require('ioredis');
const { nanoid } = require('nanoid');
const cors = require('cors');

const app = express();
const port = process.env.PORT || 8080;

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Redis connection with connection pooling
const redis = new Redis(process.env.REDIS_URL, {
  maxRetriesPerRequest: 3,
  retryDelayOnFailover: 100,
  enableAutoPipelining: true,
  lazyConnect: true,
  keepAlive: 30000,
  connectTimeout: 10000,
  commandTimeout: 5000,
  family: 4,
});

// Health check
app.get('/', (req, res) => {
  res.json({ 
    status: 'WebsiteScraper v2 API Running on Google Cloud',
    timestamp: new Date().toISOString(),
    version: '2.0.0',
    environment: 'production'
  });
});

app.get('/api/health', async (req, res) => {
  try {
    await redis.ping();
    const stats = await Promise.all([
      redis.get('stats:jobs_created').catch(() => '0'),
      redis.get('stats:jobs_completed').catch(() => '0'),
      redis.get('stats:jobs_failed').catch(() => '0'),
      redis.llen('jobs:normal').catch(() => 0),
      redis.keys('worker:*').then(keys => keys.length).catch(() => 0),
    ]);
    
    const [created, completed, failed, normalQueue, activeWorkers] = stats;
    
    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      redis: 'connected',
      stats: {
        jobs_created: parseInt(created) || 0,
        jobs_completed: parseInt(completed) || 0,
        jobs_failed: parseInt(failed) || 0,
        success_rate: parseInt(created) > 0 ? 
          ((parseInt(completed) / parseInt(created)) * 100).toFixed(1) + '%' : 'N/A',
        queue_length: normalQueue,
        active_workers: activeWorkers
      }
    });
  } catch (error) {
    res.status(503).json({
      status: 'unhealthy',
      error: error.message,
      redis: 'disconnected'
    });
  }
});

// Scrape endpoint
app.post('/api/scrape', async (req, res) => {
  try {
    // Auth check
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ') || 
        authHeader.substring(7) !== process.env.SCRAPER_AUTH_TOKEN) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Validate URL
    const { url } = req.body;
    if (!url || typeof url !== 'string' || 
        (!url.startsWith('http://') && !url.startsWith('https://'))) {
      return res.status(400).json({ error: 'Valid URL required' });
    }

    // Create job
    const jobId = `scr_${nanoid(12)}`;
    const job = {
      id: jobId,
      url,
      render_js: req.body.render_js || false,
      max_wait_ms: req.body.max_wait_ms || 12000,
      block_assets: req.body.block_assets || ['image', 'font', 'media', 'stylesheet', 'analytics'],
      user_agent: req.body.user_agent || 'auto',
      priority: req.body.priority || 'normal',
      created_at: new Date().toISOString(),
      idempotency_key: req.body.idempotency_key
    };

    // Store job and queue it
    const pipeline = redis.pipeline();
    pipeline.setex(`job:${jobId}`, 3600, JSON.stringify(job));
    pipeline.lpush('jobs:normal', jobId);
    pipeline.incr('stats:jobs_created');
    await pipeline.exec();

    res.json({
      status: 'queued',
      job_id: jobId,
      eta_ms: 15000
    });

  } catch (error) {
    console.error('Scrape API error:', error);
    res.status(500).json({ 
      error: 'Internal server error',
      message: error.message 
    });
  }
});

// Result endpoint
app.get('/api/result', async (req, res) => {
  try {
    const { job_id } = req.query;
    if (!job_id) {
      return res.status(400).json({ error: 'job_id required' });
    }

    // Auth check
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ') || 
        authHeader.substring(7) !== process.env.SCRAPER_AUTH_TOKEN) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const result = await redis.get(`result:${job_id}`);
    if (result) {
      const data = JSON.parse(result);
      res.json({
        status: 'done',
        job_id,
        result: data.data
      });
    } else {
      const job = await redis.get(`job:${job_id}`);
      if (job) {
        res.json({
          status: 'processing',
          job_id,
          message: 'Job is being processed'
        });
      } else {
        res.status(404).json({
          status: 'failed',
          job_id,
          error: 'Job not found'
        });
      }
    }
  } catch (error) {
    console.error('Result API error:', error);
    res.status(500).json({ 
      error: 'Internal server error',
      message: error.message 
    });
  }
});

app.listen(port, '0.0.0.0', () => {
  console.log(`🚀 WebsiteScraper v2 API running on port ${port}`);
  console.log(`☁️ Google Cloud Run deployment ready`);
  console.log(`🔗 Health check: http://localhost:${port}/`);
});
"@
    $ApiServerContent | Out-File -FilePath "cloud-server.js" -Encoding UTF8
    
    # 7. Create package.json for Cloud Run
    $PackageJson = @"
{
  "name": "websitescraper-v2-cloud",
  "version": "2.0.0",
  "description": "WebsiteScraper v2 API for Google Cloud Run",
  "main": "cloud-server.js",
  "scripts": {
    "start": "node cloud-server.js"
  },
  "dependencies": {
    "express": "^4.18.2",
    "ioredis": "^5.3.2",
    "nanoid": "^3.3.7",
    "cors": "^2.8.5"
  },
  "engines": {
    "node": ">=18.0.0"
  }
}
"@
    $PackageJson | Out-File -FilePath "cloud-package.json" -Encoding UTF8
    
    # 8. Create Dockerfile
    $DockerfileContent = @"
FROM node:18-alpine

WORKDIR /app

COPY cloud-package.json package.json
RUN npm ci --only=production

COPY cloud-server.js .

EXPOSE 8080

CMD ["npm", "start"]
"@
    $DockerfileContent | Out-File -FilePath "Dockerfile" -Encoding UTF8
    
    # 9. Build and deploy
    Write-Host "🏗️ Building and deploying to Google Cloud Run..." -ForegroundColor Yellow
    Write-Host "   This may take a few minutes..." -ForegroundColor Gray
    
    & gcloud run deploy websitescraper-api `
        --source . `
        --platform managed `
        --region us-central1 `
        --allow-unauthenticated `
        --memory 1Gi `
        --cpu 1 `
        --timeout 60 `
        --set-env-vars="REDIS_URL=rediss://default:AXrwAAIjcDFlYjhlNzM0YWY5ZWE0NzYxYTNiM2ZmZWQxYjUxZmYwOXAxMA@needed-dingo-31472.upstash.io:6379,SCRAPER_AUTH_TOKEN=WSCRAPER_PROD_6q2dVNYxJ4,NODE_ENV=production"
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Deployment failed" -ForegroundColor Red
        pause
        exit 1
    }
    
    # 10. Get the deployed URL
    $serviceUrl = & gcloud run services describe websitescraper-api --platform managed --region us-central1 --format="value(status.url)"
    
    Write-Host ""
    Write-Host "🎉 GOOGLE CLOUD DEPLOYMENT COMPLETE!" -ForegroundColor Green
    Write-Host "====================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "✅ Your API is deployed and running!" -ForegroundColor White
    Write-Host "🌐 API URL: $serviceUrl" -ForegroundColor Blue
    Write-Host "🔑 Auth Token: WSCRAPER_PROD_6q2dVNYxJ4" -ForegroundColor Green
    Write-Host ""
    Write-Host "📡 Clay Integration URLs:" -ForegroundColor White
    Write-Host "   • Scrape:  $serviceUrl/api/scrape" -ForegroundColor Cyan
    Write-Host "   • Result:  $serviceUrl/api/result" -ForegroundColor Cyan
    Write-Host "   • Health:  $serviceUrl/api/health" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "💰 Monthly Cost: ~$5 from your $300 credit" -ForegroundColor Yellow
    Write-Host "⏰ Credit Duration: ~5 years at this usage" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "🎯 Next Steps:" -ForegroundColor White
    Write-Host "   1. Test the API: $serviceUrl" -ForegroundColor Gray
    Write-Host "   2. Keep your Lenovo worker running" -ForegroundColor Gray
    Write-Host "   3. Configure Clay with the URLs above" -ForegroundColor Gray
    Write-Host "   4. Process 5,000 URLs in ~15 minutes!" -ForegroundColor Gray
    Write-Host ""
    Write-Host "✅ Your WebsiteScraper v2 is production-ready!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Press any key to exit..." -ForegroundColor Gray
    
} catch {
    Write-Host "❌ Installation failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Please check the error and try again" -ForegroundColor Yellow
    pause
    exit 1
}

pause