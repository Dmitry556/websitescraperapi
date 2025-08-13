export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');
  
  return res.status(200).json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    message: 'WebsiteScraper v2 API is running!',
    environment: {
      NODE_ENV: process.env.NODE_ENV,
      REDIS_URL: process.env.REDIS_URL ? 'SET' : 'NOT SET',
      SCRAPER_AUTH_TOKEN: process.env.SCRAPER_AUTH_TOKEN ? 'SET' : 'NOT SET'
    }
  });
}