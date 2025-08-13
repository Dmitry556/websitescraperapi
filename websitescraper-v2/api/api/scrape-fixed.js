import { nanoid } from 'nanoid';

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
    // Fast auth check
    const authHeader = req.headers.authorization;
    if (!authHeader?.startsWith('Bearer ') || 
        authHeader.substring(7) !== process.env.SCRAPER_AUTH_TOKEN) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Fast input validation
    const { url } = req.body;
    if (!url || typeof url !== 'string' || 
        (!url.startsWith('http://') && !url.startsWith('https://'))) {
      return res.status(400).json({ error: 'Valid URL required' });
    }

    // Generate job
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

    // Use fetch to Redis REST API instead of ioredis for speed
    const redisUrl = process.env.REDIS_URL;
    const [, , auth, host] = redisUrl.match(/rediss?:\/\/([^:]*):([^@]*)@([^:]*)/);
    
    // Direct Redis HTTP API calls (much faster than ioredis connection)
    const redisApiBase = `https://${host}`;
    const headers = {
      'Authorization': `Bearer ${auth}`,
      'Content-Type': 'application/json'
    };

    // Store job and queue it using Redis REST API
    const promises = [
      fetch(`${redisApiBase}/setex/job:${jobId}/3600`, {
        method: 'POST',
        headers,
        body: JSON.stringify(job)
      }),
      fetch(`${redisApiBase}/lpush/jobs:normal`, {
        method: 'POST',
        headers,
        body: JSON.stringify([jobId])
      }),
      fetch(`${redisApiBase}/incr/stats:jobs_created`, {
        method: 'POST',
        headers
      })
    ];

    await Promise.all(promises);

    // Immediate response
    return res.status(200).json({
      status: 'queued',
      job_id: jobId,
      eta_ms: 15000
    });

  } catch (error) {
    console.error('Scrape API error:', error);
    return res.status(500).json({ 
      error: 'Internal server error',
      message: error.message 
    });
  }
}