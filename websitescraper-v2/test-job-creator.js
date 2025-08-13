#!/usr/bin/env node

// Direct test job creator - bypasses Vercel API timeout issues
const Redis = require('ioredis');
const { nanoid } = require('nanoid');

const REDIS_URL = 'rediss://default:AXrwAAIjcDFlYjhlNzM0YWY5ZWE0NzYxYTNiM2ZmZWQxYjUxZmYwOXAxMA@needed-dingo-31472.upstash.io:6379';

async function createTestJob() {
  console.log('🧪 Creating test job directly in Redis...');
  
  const redis = new Redis(REDIS_URL, {
    maxRetriesPerRequest: 3,
    connectTimeout: 10000,
    lazyConnect: true,
  });

  try {
    await redis.connect();
    console.log('✅ Connected to Redis');

    // Create a test job
    const jobId = `scr_${nanoid(12)}`;
    const job = {
      id: jobId,
      url: 'https://httpbin.org/html',
      render_js: false,
      max_wait_ms: 12000,
      block_assets: ['image', 'font', 'media', 'stylesheet', 'analytics'],
      user_agent: 'auto',
      priority: 'normal',
      created_at: new Date().toISOString(),
      status: 'pending'
    };

    // Store job and add to queue
    await redis.setex(`job:${jobId}`, 3600, JSON.stringify(job));
    await redis.lpush('jobs:normal', jobId);
    
    console.log(`✅ Created test job: ${jobId}`);
    console.log(`🎯 URL: ${job.url}`);
    console.log('👀 Watch your worker window - it should process this job now!');
    console.log('⏱️  Should complete in ~10-15 seconds');

    // Wait a bit and check result
    console.log('\n⏳ Waiting 20 seconds for processing...');
    await new Promise(resolve => setTimeout(resolve, 20000));

    const result = await redis.get(`result:${jobId}`);
    if (result) {
      const parsedResult = JSON.parse(result);
      console.log('\n🎉 SUCCESS! Job completed:');
      console.log(`✅ Status: ${parsedResult.data.status_code}`);
      console.log(`✅ Title: ${parsedResult.data.title}`);
      console.log(`✅ HTML length: ${parsedResult.data.html_body.length} characters`);
      console.log(`✅ Processing time: ${parsedResult.data.meta.fetch_ms}ms`);
      console.log(`✅ Proxy used: ${parsedResult.data.meta.proxy_used}`);
    } else {
      console.log('\n⏳ Job still processing or failed. Check worker logs.');
    }

    await redis.quit();
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

createTestJob();