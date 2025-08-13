#!/bin/bash

# WebsiteScraper v2 Production Setup Script
# This script sets up everything needed on your Lenovo laptop

set -e

echo "🚀 WebsiteScraper v2 Production Setup"
echo "===================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if running on correct platform
if [[ "$OSTYPE" != "linux-gnu"* ]] && [[ "$OSTYPE" != "darwin"* ]]; then
    echo -e "${RED}❌ This script is designed for Linux/macOS${NC}"
    exit 1
fi

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

echo -e "${BLUE}📋 Checking prerequisites...${NC}"

# Check Node.js
if command_exists node; then
    NODE_VERSION=$(node --version)
    echo -e "${GREEN}✅ Node.js found: $NODE_VERSION${NC}"
    
    # Check if Node.js version is 18+
    NODE_MAJOR=$(echo $NODE_VERSION | cut -d'.' -f1 | cut -d'v' -f2)
    if [ "$NODE_MAJOR" -lt 18 ]; then
        echo -e "${RED}❌ Node.js 18+ required. Please update Node.js${NC}"
        exit 1
    fi
else
    echo -e "${RED}❌ Node.js not found${NC}"
    echo "Please install Node.js 18+ from https://nodejs.org/"
    exit 1
fi

# Check npm
if command_exists npm; then
    NPM_VERSION=$(npm --version)
    echo -e "${GREEN}✅ npm found: $NPM_VERSION${NC}"
else
    echo -e "${RED}❌ npm not found${NC}"
    exit 1
fi

# Check Git
if command_exists git; then
    echo -e "${GREEN}✅ Git found${NC}"
else
    echo -e "${YELLOW}⚠️  Git not found (optional for updates)${NC}"
fi

echo

# Install dependencies
echo -e "${BLUE}📦 Installing dependencies...${NC}"
npm install
echo -e "${GREEN}✅ Dependencies installed${NC}"

# Install Playwright browsers
echo -e "${BLUE}🎭 Installing Playwright browsers...${NC}"
npx playwright install chromium
echo -e "${GREEN}✅ Playwright browsers installed${NC}"

# Build TypeScript
echo -e "${BLUE}🔨 Building TypeScript...${NC}"
npm run build
echo -e "${GREEN}✅ TypeScript built${NC}"

# Check proxy file
echo -e "${BLUE}🌐 Checking proxy configuration...${NC}"
if [ -f "webshare-proxies.txt" ]; then
    PROXY_COUNT=$(grep -v '^#' webshare-proxies.txt | grep -v '^$' | wc -l)
    echo -e "${GREEN}✅ Proxy file found with $PROXY_COUNT proxies${NC}"
else
    echo -e "${RED}❌ Proxy file 'webshare-proxies.txt' not found${NC}"
    echo "Please ensure your proxy file is in the project root"
    exit 1
fi

# Check environment file
echo -e "${BLUE}⚙️  Checking environment configuration...${NC}"
if [ -f ".env" ]; then
    echo -e "${GREEN}✅ Environment file found${NC}"
else
    echo -e "${YELLOW}⚠️  Creating .env from template${NC}"
    cp .env.example .env
    echo -e "${YELLOW}📝 Please edit .env file with your Redis URL${NC}"
fi

# Test basic functionality
echo -e "${BLUE}🧪 Running basic tests...${NC}"

# Test proxy loading
node -e "
const fs = require('fs');
try {
  const content = fs.readFileSync('webshare-proxies.txt', 'utf-8');
  const lines = content.split('\n').filter(line => line.trim() && !line.startsWith('#'));
  let valid = 0;
  for (const line of lines) {
    const parts = line.trim().split(':');
    if (parts.length === 4) valid++;
  }
  console.log('✅ Proxy validation: ' + valid + '/' + lines.length + ' proxies valid');
} catch (error) {
  console.error('❌ Proxy test failed:', error.message);
  process.exit(1);
}
"

# Test TypeScript build
if [ -f "dist/worker/index.js" ]; then
    echo -e "${GREEN}✅ Build validation: TypeScript compiled successfully${NC}"
else
    echo -e "${RED}❌ Build validation failed${NC}"
    exit 1
fi

echo
echo -e "${GREEN}🎉 Setup completed successfully!${NC}"
echo
echo -e "${BLUE}📋 Next steps:${NC}"
echo "1. Set up Redis (we recommend free Upstash Redis)"
echo "2. Update REDIS_URL in .env file"
echo "3. Deploy API to Vercel (instructions in README)"
echo "4. Start worker: npm run start:worker"
echo
echo -e "${BLUE}💡 Quick commands:${NC}"
echo "• Start worker: ${GREEN}npm run start:worker${NC}"
echo "• Development mode: ${GREEN}npm run dev:worker${NC}"
echo "• Check health: ${GREEN}npm run health${NC}"
echo "• View logs: ${GREEN}tail -f worker.log${NC}"
echo
echo -e "${YELLOW}⚠️  Important: Your Lenovo needs to stay running during scraping sessions${NC}"
echo -e "${YELLOW}⚠️  Monitor memory usage - worker will auto-restart if needed${NC}"