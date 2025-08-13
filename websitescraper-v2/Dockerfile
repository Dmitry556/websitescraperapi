# WebsiteScraper v2 - Google Cloud Run Dockerfile
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY api/package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy API source code
COPY api/api ./api/

# Create a simple server.js for Cloud Run
COPY <<EOF ./server.js
const express = require('express');
const cors = require('cors');
const path = require('path');

const app = express();
const port = process.env.PORT || 8080;

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Health check
app.get('/', (req, res) => {
  res.json({ 
    status: 'WebsiteScraper v2 API Running',
    timestamp: new Date().toISOString(),
    version: '2.0.0'
  });
});

// Load API routes
const scrapeHandler = require('./api/scrape.js').default;
const resultHandler = require('./api/result.js').default;
const healthHandler = require('./api/health.js').default;

// Route handlers
app.post('/api/scrape', scrapeHandler);
app.get('/api/result', resultHandler);
app.get('/api/health', healthHandler);

app.listen(port, '0.0.0.0', () => {
  console.log(\`🚀 WebsiteScraper v2 API running on port \${port}\`);
  console.log(\`🌐 Google Cloud Run deployment ready\`);
  console.log(\`🔗 Health check: http://localhost:\${port}/\`);
});
EOF

# Expose port
EXPOSE 8080

# Start the server
CMD ["node", "server.js"]