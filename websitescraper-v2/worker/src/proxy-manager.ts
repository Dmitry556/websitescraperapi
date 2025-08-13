import { ProxyEndpoint } from '../../shared/types';
import { readFileSync } from 'fs';
import { join } from 'path';

export class ProxyManager {
  private proxies: ProxyEndpoint[] = [];
  private currentIndex = 0;
  private readonly proxyFile: string;

  constructor(proxyFile: string) {
    this.proxyFile = proxyFile;
    this.loadProxies();
  }

  private loadProxies(): void {
    try {
      const filePath = join(process.cwd(), this.proxyFile);
      const content = readFileSync(filePath, 'utf-8');
      const lines = content.split('\n').filter(line => line.trim() && !line.startsWith('#'));
      
      this.proxies = [];
      
      for (const line of lines) {
        const parts = line.trim().split(':');
        if (parts.length === 4) {
          const [host, port, username, password] = parts;
          
          this.proxies.push({
            host,
            port: parseInt(port, 10),
            username,
            password,
            failures: 0,
            last_used: 0,
            success_rate: 1.0
          });
        }
      }
      
      if (this.proxies.length === 0) {
        throw new Error('No valid proxies found in file');
      }
      
      console.log(`📡 Loaded ${this.proxies.length} US proxies`);
      
    } catch (error) {
      console.error('❌ Failed to load proxy file:', (error as Error).message);
      throw error;
    }
  }

  getProxy(attempt: number = 1): ProxyEndpoint {
    if (this.proxies.length === 0) {
      throw new Error('No proxies available');
    }

    // For first attempt, prefer proxies with good success rates
    if (attempt === 1) {
      const goodProxies = this.proxies.filter(p => p.success_rate > 0.7 && p.failures < 3);
      if (goodProxies.length > 0) {
        const proxy = goodProxies[this.currentIndex % goodProxies.length];
        this.currentIndex = (this.currentIndex + 1) % goodProxies.length;
        return proxy;
      }
    }

    // For retries, round-robin through all proxies
    const proxy = this.proxies[this.currentIndex % this.proxies.length];
    this.currentIndex = (this.currentIndex + 1) % this.proxies.length;
    
    return proxy;
  }

  markSuccess(proxy: ProxyEndpoint, responseTimeMs: number): void {
    proxy.last_used = Date.now();
    proxy.success_rate = Math.min(1.0, proxy.success_rate * 0.95 + 0.05); // Slowly increase
    
    // Reset failures on success
    if (proxy.failures > 0) {
      proxy.failures = Math.max(0, proxy.failures - 1);
    }
  }

  markFailure(proxy: ProxyEndpoint, error: string): void {
    proxy.failures++;
    proxy.success_rate = Math.max(0.1, proxy.success_rate * 0.9); // Decrease success rate
    
    console.warn(`🚫 Proxy ${proxy.host}:${proxy.port} failed: ${error} (failures: ${proxy.failures})`);
    
    // If proxy is consistently failing, temporarily skip it
    if (proxy.failures >= 5) {
      console.warn(`⚠️  Proxy ${proxy.host}:${proxy.port} has ${proxy.failures} failures, reducing priority`);
    }
  }

  formatProxyUrl(proxy: ProxyEndpoint): string {
    return `http://${proxy.username}:${proxy.password}@${proxy.host}:${proxy.port}`;
  }

  getStats() {
    const totalProxies = this.proxies.length;
    const healthyProxies = this.proxies.filter(p => p.success_rate > 0.5 && p.failures < 3).length;
    const avgSuccessRate = this.proxies.reduce((sum, p) => sum + p.success_rate, 0) / totalProxies;
    
    return {
      total_proxies: totalProxies,
      healthy_proxies: healthyProxies,
      average_success_rate: Math.round(avgSuccessRate * 100) / 100,
      current_index: this.currentIndex
    };
  }

  // Reload proxies from file (useful for updates without restart)
  reload(): void {
    console.log('🔄 Reloading proxy configuration...');
    this.loadProxies();
  }
}