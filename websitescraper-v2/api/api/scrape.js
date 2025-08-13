import Redis from 'ioredis';
import { nanoid } from 'nanoid';
import { z } from 'zod';

// Input validation schema
const scrapeSchema = z.object({
  url: z.string().url().refine((url) => {
    const parsed = new URL(url);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  }, 'URL must use http or https protocol'),
  render_js: z.boolean().optional().default(false),
  max_wait_ms: z.number().min(1000).max(30000).optional().default(12000),
  block_assets: z.array(z.enum(['image', 'font', 'media', 'stylesheet', 'analytics', 'xhr']))
    .optional()
    .default(['image', 'font', 'media', 'stylesheet', 'analytics']),
  user_agent: z.string().optional().default('auto'),
  priority: z.enum(['low', 'normal', 'high']).optional().default('normal'),
  idempotency_key: z.string().optional(),
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

// Rate limiting (simple in-memory for Vercel)
const rateLimitMap = new Map();
function checkRateLimit(ip) {
  const now = Date.now();
  const windowMs = 60000; // 1 minute
  const maxRequests = 100; // 100 requests per minute per IP
  
  const requests = rateLimitMap.get(ip) || [];
  const recentRequests = requests.filter(time => now - time < windowMs);
  
  if (recentRequests.length >= maxRequests) {
    throw new Error('Rate limit exceeded');
  }
  
  recentRequests.push(now);
  rateLimitMap.set(ip, recentRequests);
  
  // Cleanup old entries
  if (rateLimitMap.size > 1000) {
    for (const [key, times] of rateLimitMap.entries()) {
      if (times.every(time => now - time > windowMs)) {
        rateLimitMap.delete(key);
      }
    }
  }
}

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
    // Rate limiting
    const clientIP = req.headers['x-forwarded-for']?.split(',')[0] || 
                     req.headers['x-real-ip'] || 
                     req.connection?.remoteAddress || 
                     'unknown';
    checkRateLimit(clientIP);
    
    // Authentication
    authenticate(req);
    
    // Validate input
    const input = scrapeSchema.parse(req.body);
    
    // Generate job ID
    const jobId = `scr_${nanoid(12)}`;
    
    // Connect to Redis
    const redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 3,
      retryDelayOnFailover: 100,
      lazyConnect: true,
      connectTimeout: 5000,
    });
    
    // Create job
    const job = {
      id: jobId,
      url: input.url,
      render_js: input.render_js,
      max_wait_ms: input.max_wait_ms,
      block_assets: input.block_assets,
      user_agent: input.user_agent,
      priority: input.priority,
      idempotency_key: input.idempotency_key,
      created_at: new Date().toISOString(),
      status: 'queued',
      client_ip: clientIP,
    };
    
    // Check for duplicate (idempotency)
    if (input.idempotency_key) {
      const existing = await redis.get(`idem:${input.idempotency_key}`);
      if (existing) {
        await redis.quit();
        return res.status(200).json({
          status: 'queued',
          job_id: existing,
          eta_ms: 30000,
          note: 'Duplicate request, returning existing job ID'
        });
      }
      
      // Store idempotency mapping
      await redis.setex(`idem:${input.idempotency_key}`, 3600, jobId);
    }
    
    // Store job in Redis
    await redis.setex(`job:${jobId}`, 3600, JSON.stringify(job));
    
    // Add to work queue based on priority
    const queueName = input.priority === 'high' ? 'jobs:high' : 
                     input.priority === 'low' ? 'jobs:low' : 'jobs:normal';
    await redis.lpush(queueName, jobId);
    
    // Update stats
    await redis.incr('stats:jobs_created');
    await redis.incr(`stats:jobs_created:${new Date().toISOString().split('T')[0]}`);
    
    await redis.quit();
    
    // Return response
    res.status(200).json({
      status: 'queued',
      job_id: jobId,
      eta_ms: input.priority === 'high' ? 15000 : 
              input.priority === 'low' ? 120000 : 60000,
    });
    
  } catch (error) {
    console.error('Scrape API error:', error);
    
    if (error.message.includes('Rate limit')) {
      return res.status(429).json({
        error: 'Rate limit exceeded',
        message: 'Too many requests, please slow down'
      });
    }
    
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
      message: 'Failed to create scrape job'
    });
  }
}