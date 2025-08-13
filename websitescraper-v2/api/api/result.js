import Redis from 'ioredis';
import { z } from 'zod';

// Input validation
const resultSchema = z.object({
  job_id: z.string().min(1, 'job_id is required'),
});

// Auth middleware
function authenticate(req) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    throw new Error('Missing or invalid Authorization header');
  }
  
  const token = authHeader.substring(7);
  if (token !== process.env.SCRAPER_AUTH_TOKEN) {
    throw new Error('Invalid token');
  }
}

export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }
  
  try {
    // Authentication
    authenticate(req);
    
    // Validate input
    const { job_id } = resultSchema.parse(req.query);
    
    // Connect to Redis
    const redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 3,
      retryDelayOnFailover: 100,
      lazyConnect: true,
      connectTimeout: 5000,
    });
    
    // Get job status
    const jobData = await redis.get(`job:${job_id}`);
    
    if (!jobData) {
      await redis.quit();
      return res.status(404).json({
        error: 'Job not found',
        message: `Job ${job_id} does not exist or has expired`
      });
    }
    
    const job = JSON.parse(jobData);
    
    // Check for result
    const resultData = await redis.get(`result:${job_id}`);
    
    let response;
    
    if (resultData) {
      // Job completed
      const result = JSON.parse(resultData);
      
      if (result.error) {
        response = {
          status: 'failed',
          job_id,
          error: result.error,
          meta: {
            duration_ms: Date.now() - new Date(job.created_at).getTime(),
            worker_id: result.worker_id
          }
        };
      } else {
        response = {
          status: 'done',
          job_id,
          result: result.data,
          meta: {
            duration_ms: Date.now() - new Date(job.created_at).getTime(),
            worker_id: result.worker_id
          }
        };
      }
    } else {
      // Job still in progress
      const isRunning = await redis.get(`running:${job_id}`);
      const queuePosition = await Promise.all([
        redis.lpos('jobs:high', job_id),
        redis.lpos('jobs:normal', job_id),
        redis.lpos('jobs:low', job_id),
      ]);
      
      const position = queuePosition.find(pos => pos !== null);
      
      if (isRunning) {
        const runningData = JSON.parse(isRunning);
        response = {
          status: 'running',
          job_id,
          progress: runningData.progress || 0,
          eta_ms: Math.max(5000, job.max_wait_ms - runningData.elapsed_ms),
          worker_id: runningData.worker_id
        };
      } else if (position !== null) {
        response = {
          status: 'queued',
          job_id,
          queue_position: position + 1,
          eta_ms: (position + 1) * 15000, // Estimate 15s per job ahead
        };
      } else {
        // Job might be lost or expired
        response = {
          status: 'timeout',
          job_id,
          error: {
            code: 'TIMEOUT',
            message: 'Job timed out or was lost'
          }
        };
      }
    }
    
    await redis.quit();
    
    res.status(200).json(response);
    
  } catch (error) {
    console.error('Result API error:', error);
    
    if (error.message.includes('Authorization') || error.message.includes('token')) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Invalid or missing authentication token'
      });
    }
    
    if (error.message.includes('Invalid') || error.name === 'ZodError') {
      return res.status(400).json({
        error: 'Bad Request',
        message: error.message
      });
    }
    
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to get job result'
    });
  }
}