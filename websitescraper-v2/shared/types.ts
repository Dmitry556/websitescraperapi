// Shared types between API and Worker

export interface ScrapeRequest {
  url: string;
  render_js?: boolean;
  max_wait_ms?: number;
  block_assets?: string[];
  user_agent?: string;
  priority?: 'low' | 'normal' | 'high';
  idempotency_key?: string;
}

export interface ScrapeResult {
  url: string;
  final_url: string;
  status_code: number;
  title: string;
  headers: Record<string, string>;
  html_body: string;
  pdf_urls: string[];
  meta: {
    fetch_ms: number;
    retries: number;
    proxy_used: string;
    render_js: boolean;
    bytes_rx: number;
    worker_id: string;
  };
}

export interface JobResponse {
  status: 'queued' | 'running' | 'done' | 'failed' | 'timeout';
  job_id: string;
  progress?: number;
  eta_ms?: number;
  result?: ScrapeResult;
  error?: {
    code: 'INVALID_URL' | 'NETWORK' | 'TIMEOUT' | 'BLOCKED' | 'INTERNAL';
    message: string;
    details?: string;
  };
}

export interface WorkerStats {
  worker_id: string;
  active_jobs: number;
  total_completed: number;
  total_failed: number;
  success_rate: number;
  avg_response_time: number;
  memory_usage: number;
  uptime_seconds: number;
  last_heartbeat: string;
}

export interface ProxyEndpoint {
  host: string;
  port: number;
  username: string;
  password: string;
  failures: number;
  last_used: number;
  success_rate: number;
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface Config {
  // Redis connection
  redis_url: string;
  
  // Worker settings
  worker_id: string;
  max_concurrent_jobs: number;
  proxy_file: string;
  
  // Timeouts and retries
  default_timeout_ms: number;
  max_retries: number;
  retry_delay_ms: number;
  
  // Reliability settings
  heartbeat_interval_ms: number;
  proxy_rotation_threshold: number;
  memory_limit_mb: number;
  
  // Monitoring
  log_level: LogLevel;
  stats_interval_ms: number;
}