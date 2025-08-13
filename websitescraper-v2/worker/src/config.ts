import { Config } from '../../shared/types';
import { readFileSync } from 'fs';
import { join } from 'path';

function getEnvVar(name: string, defaultValue?: string): string {
  const value = process.env[name];
  if (value === undefined) {
    if (defaultValue !== undefined) {
      return defaultValue;
    }
    throw new Error(`Environment variable ${name} is required`);
  }
  return value;
}

function getEnvNumber(name: string, defaultValue?: number): number {
  const value = process.env[name];
  if (value === undefined) {
    if (defaultValue !== undefined) {
      return defaultValue;
    }
    throw new Error(`Environment variable ${name} is required`);
  }
  const parsed = parseInt(value, 10);
  if (isNaN(parsed)) {
    throw new Error(`Environment variable ${name} must be a number`);
  }
  return parsed;
}

export function loadConfig(): Config {
  // Generate unique worker ID
  const workerId = `worker_${require('os').hostname()}_${process.pid}_${Date.now().toString(36)}`;
  
  return {
    redis_url: getEnvVar('REDIS_URL', 'redis://localhost:6379'),
    
    worker_id: workerId,
    max_concurrent_jobs: getEnvNumber('MAX_CONCURRENT_JOBS', 20),
    proxy_file: getEnvVar('PROXY_FILE', 'webshare-proxies.txt'),
    
    default_timeout_ms: getEnvNumber('DEFAULT_TIMEOUT_MS', 12000),
    max_retries: getEnvNumber('MAX_RETRIES', 3),
    retry_delay_ms: getEnvNumber('RETRY_DELAY_MS', 2000),
    
    heartbeat_interval_ms: getEnvNumber('HEARTBEAT_INTERVAL_MS', 30000),
    proxy_rotation_threshold: getEnvNumber('PROXY_ROTATION_THRESHOLD', 3),
    memory_limit_mb: getEnvNumber('MEMORY_LIMIT_MB', 8000),
    
    log_level: (getEnvVar('LOG_LEVEL', 'info') as any),
    stats_interval_ms: getEnvNumber('STATS_INTERVAL_MS', 60000),
  };
}

export function validateConfig(): void {
  const config = loadConfig();
  
  // Check proxy file exists
  try {
    const proxyPath = join(process.cwd(), config.proxy_file);
    const content = readFileSync(proxyPath, 'utf-8');
    const lines = content.split('\n').filter(line => line.trim() && !line.startsWith('#'));
    
    if (lines.length === 0) {
      throw new Error('Proxy file is empty or contains no valid proxies');
    }
    
    console.log(`✅ Loaded ${lines.length} proxies from ${config.proxy_file}`);
  } catch (error) {
    throw new Error(`Failed to load proxy file: ${(error as Error).message}`);
  }
  
  // Validate memory limit
  const totalMemoryMB = require('os').totalmem() / 1024 / 1024;
  if (config.memory_limit_mb > totalMemoryMB * 0.8) {
    console.warn(`⚠️  Memory limit ${config.memory_limit_mb}MB is high (${Math.round(config.memory_limit_mb / totalMemoryMB * 100)}% of total)`);
  }
  
  console.log(`✅ Worker configuration validated`);
  console.log(`   Worker ID: ${config.worker_id}`);
  console.log(`   Max concurrent jobs: ${config.max_concurrent_jobs}`);
  console.log(`   Memory limit: ${config.memory_limit_mb}MB`);
}