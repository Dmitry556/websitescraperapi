import Redis from 'ioredis';

export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }
  
  try {
    const startTime = Date.now();
    
    // Check Redis connection
    const redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 1,
      connectTimeout: 3000,
      lazyConnect: true,
    });
    
    await redis.ping();
    
    // Get basic stats
    const stats = await Promise.all([
      redis.get('stats:jobs_created') || '0',
      redis.get('stats:jobs_completed') || '0',
      redis.get('stats:jobs_failed') || '0',
      redis.llen('jobs:high'),
      redis.llen('jobs:normal'),
      redis.llen('jobs:low'),
      redis.keys('worker:*').then(keys => keys.length),
    ]);
    
    await redis.quit();
    
    const [created, completed, failed, highQueue, normalQueue, lowQueue, activeWorkers] = stats;
    
    const responseTime = Date.now() - startTime;
    
    res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      response_time_ms: responseTime,
      redis: 'connected',
      stats: {
        jobs_created: parseInt(created),
        jobs_completed: parseInt(completed),
        jobs_failed: parseInt(failed),
        success_rate: parseInt(created) > 0 ? 
          ((parseInt(completed) / parseInt(created)) * 100).toFixed(1) + '%' : 'N/A',
        queue_lengths: {
          high: highQueue,
          normal: normalQueue,
          low: lowQueue,
          total: highQueue + normalQueue + lowQueue
        },
        active_workers: activeWorkers
      }
    });
    
  } catch (error) {
    console.error('Health check error:', error);
    
    res.status(503).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      error: error.message,
      redis: 'disconnected'
    });
  }
}