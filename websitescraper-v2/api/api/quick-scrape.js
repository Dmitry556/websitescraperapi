import Redis from 'ioredis';
import { nanoid } from 'nanoid';

// Ultra-lightweight scrape endpoint for Clay - no validation, just speed
export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    // Quick auth check
    const authHeader = req.headers.authorization;
    if (!authHeader?.startsWith('Bearer ') || 
        authHeader.substring(7) !== process.env.SCRAPER_AUTH_TOKEN) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Get URL from request (minimal parsing)
    const { url } = req.body;
    if (!url) {
      return res.status(400).json({ error: 'URL required' });
    }

    // Create job ID and minimal job object
    const jobId = `scr_${nanoid(12)}`;
    const job = {
      id: jobId,
      url,
      render_js: false,
      max_wait_ms: 12000,
      block_assets: ['image', 'font', 'media', 'stylesheet', 'analytics'],
      priority: 'normal',
      created_at: new Date().toISOString()
    };

    // Ultra-fast Redis connection (no retries, fail fast)
    const redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 1,
      connectTimeout: 2000,
      lazyConnect: true,
      enableAutoPipelining: true
    });

    // Store job and queue it (pipeline for speed)
    const pipeline = redis.pipeline();
    pipeline.setex(`job:${jobId}`, 3600, JSON.stringify(job));
    pipeline.lpush('jobs:normal', jobId);
    await pipeline.exec();
    
    await redis.quit();

    // Instant response
    return res.status(200).json({
      status: 'queued',
      job_id: jobId,
      eta_ms: 15000
    });

  } catch (error) {
    console.error('Quick scrape error:', error);
    return res.status(500).json({ 
      error: 'Service temporarily unavailable',
      retry_in_seconds: 5
    });
  }
}