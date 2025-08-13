#!/usr/bin/env node

// Health check script for monitoring the worker

const Redis = require('ioredis');
const pidusage = require('pidusage');

async function healthCheck() {
    console.log('🔍 WebsiteScraper Health Check');
    console.log('=============================');
    
    try {
        // Load environment
        require('dotenv').config();
        
        // Connect to Redis
        console.log('📡 Connecting to Redis...');
        const redis = new Redis(process.env.REDIS_URL, {
            connectTimeout: 5000,
            lazyConnect: true
        });
        
        await redis.ping();
        console.log('✅ Redis connection: OK');
        
        // Check worker status
        const workers = await redis.keys('worker:*');
        console.log(`👷 Active workers: ${workers.length}`);
        
        for (const workerKey of workers) {
            const data = await redis.get(workerKey);
            if (data) {
                const worker = JSON.parse(data);
                const lastHeartbeat = new Date(worker.last_heartbeat);
                const timeSince = Date.now() - lastHeartbeat.getTime();
                const isHealthy = timeSince < 120000; // 2 minutes
                
                console.log(`   ${isHealthy ? '✅' : '❌'} ${worker.worker_id}: ${worker.active_jobs} active jobs, last seen ${Math.round(timeSince/1000)}s ago`);
                
                if (worker.total_completed || worker.total_failed) {
                    const successRate = (worker.total_completed / (worker.total_completed + worker.total_failed) * 100).toFixed(1);
                    console.log(`     📊 Success rate: ${successRate}% (${worker.total_completed} completed, ${worker.total_failed} failed)`);
                }
            }
        }
        
        // Check queue status
        const queues = ['jobs:high', 'jobs:normal', 'jobs:low'];
        let totalQueued = 0;
        
        for (const queue of queues) {
            const length = await redis.llen(queue);
            totalQueued += length;
            if (length > 0) {
                console.log(`📋 ${queue}: ${length} jobs`);
            }
        }
        
        if (totalQueued === 0) {
            console.log('📋 Job queues: Empty');
        } else {
            console.log(`📋 Total queued jobs: ${totalQueued}`);
        }
        
        // Check system resources
        console.log('\n💻 System Resources:');
        
        try {
            const stats = await pidusage(process.pid);
            const memoryMB = Math.round(stats.memory / 1024 / 1024);
            console.log(`   Memory: ${memoryMB}MB`);
            console.log(`   CPU: ${stats.cpu.toFixed(1)}%`);
        } catch (error) {
            console.log('   ⚠️  Could not get system stats');
        }
        
        // Check proxy file
        console.log('\n🌐 Proxy Configuration:');
        const fs = require('fs');
        try {
            const content = fs.readFileSync(process.env.PROXY_FILE || 'webshare-proxies.txt', 'utf-8');
            const lines = content.split('\n').filter(line => line.trim() && !line.startsWith('#'));
            console.log(`   Total proxies: ${lines.length}`);
            
            let validProxies = 0;
            for (const line of lines) {
                const parts = line.trim().split(':');
                if (parts.length === 4) validProxies++;
            }
            console.log(`   Valid format: ${validProxies}/${lines.length}`);
            
        } catch (error) {
            console.log('   ❌ Could not read proxy file');
        }
        
        // Overall health assessment
        console.log('\n🏥 Overall Health:');
        
        const issues = [];
        
        if (workers.length === 0) {
            issues.push('No active workers');
        }
        
        if (totalQueued > 1000) {
            issues.push('High queue backlog');
        }
        
        for (const workerKey of workers) {
            const data = await redis.get(workerKey);
            if (data) {
                const worker = JSON.parse(data);
                const lastHeartbeat = new Date(worker.last_heartbeat);
                const timeSince = Date.now() - lastHeartbeat.getTime();
                
                if (timeSince > 120000) {
                    issues.push(`Worker ${worker.worker_id} not responding`);
                }
            }
        }
        
        if (issues.length === 0) {
            console.log('✅ System is healthy');
        } else {
            console.log('⚠️  Issues detected:');
            issues.forEach(issue => console.log(`   • ${issue}`));
        }
        
        await redis.quit();
        
    } catch (error) {
        console.error('❌ Health check failed:', error.message);
        process.exit(1);
    }
}

healthCheck();