#!/usr/bin/env node

import Redis from 'ioredis';
import * as cron from 'node-cron';
import pidusage from 'pidusage';
import { loadConfig, validateConfig } from './config';
import { BrowserManager } from './browser-manager';
import { ProxyManager } from './proxy-manager';
import { Scraper } from './scraper';
import { ScrapeRequest, ScrapeResult, WorkerStats } from '../../shared/types';

class ProductionWorker {
  private config = loadConfig();
  private redis: Redis;
  private browserManager: BrowserManager;
  private proxyManager: ProxyManager;
  private scraper: Scraper;
  
  private isRunning = false;
  private isShuttingDown = false;
  private activeJobs = new Set<string>();
  private stats = {
    totalCompleted: 0,
    totalFailed: 0,
    startTime: Date.now()
  };
  
  constructor() {
    console.log('🚀 WebsiteScraper Production Worker v2.0');
    console.log('==========================================');
    
    // Validate configuration
    validateConfig();
    
    // Initialize components
    this.redis = new Redis(this.config.redis_url, {
      maxRetriesPerRequest: 5,
      lazyConnect: true,
      connectTimeout: 10000,
    });
    
    this.browserManager = new BrowserManager();
    this.proxyManager = new ProxyManager(this.config.proxy_file);
    this.scraper = new Scraper(this.browserManager, this.proxyManager, this.config.worker_id);
    
    this.setupErrorHandlers();
    this.setupMonitoring();
  }

  private setupErrorHandlers(): void {
    process.on('uncaughtException', (error) => {
      console.error('💥 Uncaught Exception:', error);
      this.gracefulShutdown('uncaughtException');
    });

    process.on('unhandledRejection', (reason, promise) => {
      console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
      this.gracefulShutdown('unhandledRejection');
    });

    process.on('SIGTERM', () => {
      console.log('📡 Received SIGTERM signal');
      this.gracefulShutdown('SIGTERM');
    });

    process.on('SIGINT', () => {
      console.log('📡 Received SIGINT signal');
      this.gracefulShutdown('SIGINT');
    });
  }

  private setupMonitoring(): void {
    // Heartbeat
    setInterval(() => {
      this.sendHeartbeat();
    }, this.config.heartbeat_interval_ms);

    // Stats reporting
    setInterval(() => {
      this.reportStats();
    }, this.config.stats_interval_ms);

    // Memory monitoring
    setInterval(() => {
      this.checkMemoryUsage();
    }, 30000);

    // Daily stats reset
    cron.schedule('0 0 * * *', () => {
      this.resetDailyStats();
    });

    console.log('📊 Monitoring systems active');
  }

  async start(): Promise<void> {
    try {
      console.log('🔌 Connecting to Redis...');
      await this.redis.ping();
      console.log('✅ Redis connected');

      console.log('🎭 Initializing browser...');
      await this.browserManager.initialize();

      // Register worker
      await this.registerWorker();

      this.isRunning = true;
      console.log(`✅ Worker ${this.config.worker_id} started successfully`);
      console.log(`📦 Max concurrent jobs: ${this.config.max_concurrent_jobs}`);
      console.log(`🌐 Proxy stats:`, this.proxyManager.getStats());

      // Start job processing loop
      this.processJobs();

    } catch (error) {
      console.error('❌ Failed to start worker:', (error as Error).message);
      process.exit(1);
    }
  }

  private async registerWorker(): Promise<void> {
    const workerData = {
      worker_id: this.config.worker_id,
      started_at: new Date().toISOString(),
      max_concurrent_jobs: this.config.max_concurrent_jobs,
      proxy_stats: this.proxyManager.getStats(),
      version: '2.0.0'
    };

    await this.redis.setex(
      `worker:${this.config.worker_id}`,
      this.config.heartbeat_interval_ms * 3 / 1000, // 3x heartbeat interval
      JSON.stringify(workerData)
    );

    console.log(`📝 Worker registered: ${this.config.worker_id}`);
  }

  private async processJobs(): Promise<void> {
    while (this.isRunning && !this.isShuttingDown) {
      try {
        // Check if we can take more jobs
        if (this.activeJobs.size >= this.config.max_concurrent_jobs) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }

        // Get next job (priority order: high -> normal -> low)
        const jobId = await this.getNextJob();
        
        if (!jobId) {
          // No jobs available, wait a bit
          await new Promise(resolve => setTimeout(resolve, 2000));
          continue;
        }

        // Process job in background
        this.processJob(jobId).catch((error) => {
          console.error(`❌ Job processing error for ${jobId}:`, (error as Error).message);
        });

      } catch (error) {
        console.error('❌ Error in job processing loop:', (error as Error).message);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
  }

  private async getNextJob(): Promise<string | null> {
    // Try to get job from priority queues
    const queues = ['jobs:high', 'jobs:normal', 'jobs:low'];
    
    for (const queue of queues) {
      const jobId = await this.redis.rpop(queue);
      if (jobId) {
        return jobId;
      }
    }
    
    return null;
  }

  private async processJob(jobId: string): Promise<void> {
    this.activeJobs.add(jobId);
    const startTime = Date.now();

    try {
      console.log(`🎯 Processing job: ${jobId}`);

      // Get job data
      const jobData = await this.redis.get(`job:${jobId}`);
      if (!jobData) {
        throw new Error('Job data not found');
      }

      const job = JSON.parse(jobData);

      // Mark job as running
      await this.redis.setex(`running:${jobId}`, 300, JSON.stringify({
        worker_id: this.config.worker_id,
        started_at: new Date().toISOString(),
        progress: 0,
        elapsed_ms: 0
      }));

      // Create scrape request
      const request: ScrapeRequest = {
        url: job.url,
        render_js: job.render_js,
        max_wait_ms: job.max_wait_ms,
        block_assets: job.block_assets,
        user_agent: job.user_agent,
        priority: job.priority,
        idempotency_key: job.idempotency_key
      };

      // Update progress
      await this.updateJobProgress(jobId, 25);

      // Perform scrape
      const result = await this.scraper.scrape(request);

      // Update progress
      await this.updateJobProgress(jobId, 90);

      // Store result
      await this.redis.setex(`result:${jobId}`, 3600, JSON.stringify({
        data: result,
        worker_id: this.config.worker_id,
        completed_at: new Date().toISOString()
      }));

      // Update progress
      await this.updateJobProgress(jobId, 100);

      // Clean up running status
      await this.redis.del(`running:${jobId}`);

      // Update stats
      this.stats.totalCompleted++;
      await this.redis.incr('stats:jobs_completed');
      await this.redis.incr(`stats:jobs_completed:${new Date().toISOString().split('T')[0]}`);

      const duration = Date.now() - startTime;
      console.log(`✅ Job completed: ${jobId} (${duration}ms)`);

    } catch (error) {
      console.error(`❌ Job failed: ${jobId}:`, (error as Error).message);

      // Store error result
      await this.redis.setex(`result:${jobId}`, 3600, JSON.stringify({
        error: {
          code: this.getErrorCode((error as Error).message),
          message: (error as Error).message,
          details: (error as Error).stack
        },
        worker_id: this.config.worker_id,
        failed_at: new Date().toISOString()
      }));

      // Clean up running status
      await this.redis.del(`running:${jobId}`);

      // Update stats
      this.stats.totalFailed++;
      await this.redis.incr('stats:jobs_failed');
      await this.redis.incr(`stats:jobs_failed:${new Date().toISOString().split('T')[0]}`);

    } finally {
      this.activeJobs.delete(jobId);
    }
  }

  private async updateJobProgress(jobId: string, progress: number): Promise<void> {
    const runningData = await this.redis.get(`running:${jobId}`);
    if (runningData) {
      const data = JSON.parse(runningData);
      data.progress = progress / 100;
      data.elapsed_ms = Date.now() - new Date(data.started_at).getTime();
      await this.redis.setex(`running:${jobId}`, 300, JSON.stringify(data));
    }
  }

  private getErrorCode(errorMessage: string): string {
    if (errorMessage.includes('INVALID_URL')) return 'INVALID_URL';
    if (errorMessage.includes('NETWORK')) return 'NETWORK';
    if (errorMessage.includes('BLOCKED')) return 'BLOCKED';
    if (errorMessage.includes('TIMEOUT')) return 'TIMEOUT';
    return 'INTERNAL';
  }

  private async sendHeartbeat(): Promise<void> {
    try {
      const workerData = {
        worker_id: this.config.worker_id,
        last_heartbeat: new Date().toISOString(),
        active_jobs: this.activeJobs.size,
        total_completed: this.stats.totalCompleted,
        total_failed: this.stats.totalFailed,
        uptime_seconds: Math.floor((Date.now() - this.stats.startTime) / 1000),
        memory_usage: process.memoryUsage(),
        proxy_stats: this.proxyManager.getStats()
      };

      await this.redis.setex(
        `worker:${this.config.worker_id}`,
        this.config.heartbeat_interval_ms * 3 / 1000,
        JSON.stringify(workerData)
      );

    } catch (error) {
      console.error('❌ Failed to send heartbeat:', (error as Error).message);
    }
  }

  private async reportStats(): Promise<void> {
    const uptime = Math.floor((Date.now() - this.stats.startTime) / 1000);
    const successRate = this.stats.totalCompleted + this.stats.totalFailed > 0 ?
      (this.stats.totalCompleted / (this.stats.totalCompleted + this.stats.totalFailed) * 100).toFixed(1) :
      'N/A';

    console.log(`📊 Stats: ${this.stats.totalCompleted} completed, ${this.stats.totalFailed} failed, ${successRate}% success, ${this.activeJobs.size} active, ${uptime}s uptime`);
  }

  private async checkMemoryUsage(): Promise<void> {
    try {
      const stats = await pidusage(process.pid);
      const memoryMB = stats.memory / 1024 / 1024;

      if (memoryMB > this.config.memory_limit_mb) {
        console.warn(`⚠️  High memory usage: ${memoryMB.toFixed(0)}MB (limit: ${this.config.memory_limit_mb}MB)`);
        
        // If severely over limit, restart worker
        if (memoryMB > this.config.memory_limit_mb * 1.5) {
          console.error('💥 Memory usage critical, restarting worker');
          this.gracefulShutdown('memory_limit');
        }
      }
    } catch (error) {
      console.warn('Warning: Could not check memory usage:', (error as Error).message);
    }
  }

  private async resetDailyStats(): Promise<void> {
    console.log('🔄 Resetting daily stats');
    this.stats.totalCompleted = 0;
    this.stats.totalFailed = 0;
    this.stats.startTime = Date.now();
  }

  private async gracefulShutdown(reason: string): Promise<void> {
    if (this.isShuttingDown) return;

    console.log(`🛑 Graceful shutdown initiated (reason: ${reason})`);
    this.isShuttingDown = true;
    this.isRunning = false;

    // Wait for active jobs to complete (with timeout)
    const shutdownTimeout = 30000; // 30 seconds
    const startTime = Date.now();

    while (this.activeJobs.size > 0 && (Date.now() - startTime) < shutdownTimeout) {
      console.log(`⏳ Waiting for ${this.activeJobs.size} active jobs to complete...`);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    if (this.activeJobs.size > 0) {
      console.warn(`⚠️  Shutdown timeout reached, ${this.activeJobs.size} jobs may be incomplete`);
    }

    // Cleanup resources
    try {
      await this.browserManager.cleanup();
      await this.redis.del(`worker:${this.config.worker_id}`);
      await this.redis.quit();
    } catch (error) {
      console.warn('Warning during cleanup:', (error as Error).message);
    }

    console.log('✅ Graceful shutdown complete');
    process.exit(0);
  }
}

// Start the worker
const worker = new ProductionWorker();
worker.start().catch((error) => {
  console.error('💥 Fatal error:', error);
  process.exit(1);
});