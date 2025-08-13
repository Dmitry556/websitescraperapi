import { chromium, Browser, BrowserContext, Page } from 'playwright';

export class BrowserManager {
  private browser: Browser | null = null;
  private contexts: Set<BrowserContext> = new Set();
  private isShuttingDown = false;

  async initialize(): Promise<void> {
    if (this.browser) return;

    console.log('🚀 Initializing browser...');
    
    this.browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--disable-gpu',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding',
        '--disable-features=TranslateUI',
        '--disable-ipc-flooding-protection',
        '--disable-web-security',
        '--disable-features=VizDisplayCompositor'
      ],
    });

    console.log('✅ Browser initialized');
    
    // Handle browser disconnection
    this.browser.on('disconnected', () => {
      if (!this.isShuttingDown) {
        console.error('❌ Browser disconnected unexpectedly');
        this.browser = null;
        this.contexts.clear();
      }
    });
  }

  async createContext(proxyUrl?: string, userAgent?: string): Promise<BrowserContext> {
    if (!this.browser) {
      await this.initialize();
    }

    const contextOptions: any = {
      userAgent: userAgent || this.getRandomUserAgent(),
      viewport: { width: 1920, height: 1080 },
      ignoreHTTPSErrors: true,
      extraHTTPHeaders: {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
      },
    };

    if (proxyUrl) {
      const url = new URL(proxyUrl);
      contextOptions.proxy = {
        server: `${url.protocol}//${url.host}`,
        username: url.username,
        password: url.password,
      };
    }

    const context = await this.browser!.newContext(contextOptions);
    this.contexts.add(context);

    // Auto-cleanup context when it closes
    context.on('close', () => {
      this.contexts.delete(context);
    });

    return context;
  }

  async createPage(context: BrowserContext, blockAssets: string[] = []): Promise<Page> {
    const page = await context.newPage();

    // Set up request interception for performance
    await page.route('**/*', (route) => {
      const request = route.request();
      const resourceType = request.resourceType();
      const url = request.url();

      // Block specified asset types
      if (blockAssets.includes(resourceType)) {
        route.abort();
        return;
      }

      // Block known tracking/analytics domains
      const blockedDomains = [
        'google-analytics.com',
        'googletagmanager.com',
        'facebook.com/tr',
        'doubleclick.net',
        'googlesyndication.com',
        'amazon-adsystem.com',
        'google.com/ads',
        'hotjar.com',
        'fullstory.com',
        'mixpanel.com',
        'segment.io',
        'quantserve.com',
        'scorecardresearch.com',
        'outbrain.com',
        'taboola.com',
        'adsystem.com',
        'adsystem.net'
      ];

      if (blockAssets.includes('analytics') && 
          blockedDomains.some(domain => url.includes(domain))) {
        route.abort();
        return;
      }

      // Block large files
      if (blockAssets.includes('media')) {
        const ext = url.split('.').pop()?.toLowerCase();
        if (ext && ['mp4', 'webm', 'avi', 'mov', 'wmv', 'flv'].includes(ext)) {
          route.abort();
          return;
        }
      }

      route.continue();
    });

    // Set reasonable timeouts
    page.setDefaultTimeout(30000);
    page.setDefaultNavigationTimeout(30000);

    return page;
  }

  private getRandomUserAgent(): string {
    const userAgents = [
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
    ];
    
    return userAgents[Math.floor(Math.random() * userAgents.length)];
  }

  async getStats() {
    return {
      browser_connected: !!this.browser?.isConnected(),
      active_contexts: this.contexts.size,
      memory_usage: process.memoryUsage()
    };
  }

  async cleanup(): Promise<void> {
    console.log('🧹 Cleaning up browser...');
    this.isShuttingDown = true;

    // Close all contexts
    for (const context of this.contexts) {
      try {
        await context.close();
      } catch (error) {
        console.warn('Warning: Failed to close context:', (error as Error).message);
      }
    }
    this.contexts.clear();

    // Close browser
    if (this.browser) {
      try {
        await this.browser.close();
      } catch (error) {
        console.warn('Warning: Failed to close browser:', (error as Error).message);
      }
      this.browser = null;
    }

    console.log('✅ Browser cleanup complete');
  }
}