#!/usr/bin/env node

// Quick system test without Redis - just test proxy loading and scraping

const { ProxyManager } = require('../dist/worker/src/proxy-manager');
const { BrowserManager } = require('../dist/worker/src/browser-manager');
const { Scraper } = require('../dist/worker/src/scraper');

require('dotenv').config();

async function testSystem() {
    console.log('🧪 WebsiteScraper v2 System Test');
    console.log('=================================');
    
    try {
        // Test 1: Proxy loading
        console.log('1️⃣  Testing proxy loading...');
        const proxyManager = new ProxyManager(process.env.PROXY_FILE || 'webshare-proxies.txt');
        const proxyStats = proxyManager.getStats();
        console.log(`✅ Loaded ${proxyStats.total_proxies} proxies`);
        
        // Test a proxy
        const testProxy = proxyManager.getProxy(1);
        console.log(`✅ Test proxy: ${testProxy.host}:${testProxy.port} (${testProxy.username})`);
        
        // Test 2: Browser manager
        console.log('\n2️⃣  Testing browser initialization...');
        const browserManager = new BrowserManager();
        await browserManager.initialize();
        console.log('✅ Browser initialized');
        
        // Test 3: Simple scrape
        console.log('\n3️⃣  Testing scrape functionality...');
        const scraper = new Scraper(browserManager, proxyManager, 'test-worker');
        
        const testUrl = 'https://httpbin.org/html';
        console.log(`🎯 Scraping: ${testUrl}`);
        
        const startTime = Date.now();
        const result = await scraper.scrape({
            url: testUrl,
            render_js: false,
            max_wait_ms: 10000,
            block_assets: ['image', 'font', 'media'],
            user_agent: 'auto'
        });
        
        const duration = Date.now() - startTime;
        
        console.log(`✅ Scrape completed in ${duration}ms`);
        console.log(`📄 HTML length: ${result.html_body.length} characters`);
        console.log(`🌐 Final URL: ${result.final_url}`);
        console.log(`📊 Status code: ${result.status_code}`);
        console.log(`🔗 Proxy used: ${result.meta.proxy_used}`);
        console.log(`🔄 Retries: ${result.meta.retries}`);
        
        // Test 4: Cleanup
        console.log('\n4️⃣  Testing cleanup...');
        await browserManager.cleanup();
        console.log('✅ Cleanup completed');
        
        console.log('\n🎉 All tests passed!');
        console.log('✅ Your system is ready for production');
        console.log('\nNext steps:');
        console.log('1. Set up Redis (free Upstash account recommended)');
        console.log('2. Update REDIS_URL in .env');
        console.log('3. Deploy API to Vercel');
        console.log('4. Start worker: npm run start:worker');
        
    } catch (error) {
        console.error('❌ Test failed:', error.message);
        console.error('\nTroubleshooting:');
        console.error('• Make sure webshare-proxies.txt exists and has valid proxies');
        console.error('• Check your internet connection');
        console.error('• Ensure Playwright browsers are installed: npx playwright install chromium');
        process.exit(1);
    }
}

testSystem();