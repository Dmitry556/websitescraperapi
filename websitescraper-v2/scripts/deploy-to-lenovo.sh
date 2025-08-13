#!/bin/bash

# One-line deployment script for Lenovo laptop
# Usage: curl -sSL https://raw.githubusercontent.com/yourrepo/scripts/deploy-to-lenovo.sh | bash

set -e

echo "🚀 WebsiteScraper v2 - Lenovo Deployment"
echo "======================================="

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Default configuration
PROJECT_DIR="$HOME/websitescraper-v2"
REPO_URL="https://github.com/your-username/websitescraper-v2.git" # You'll need to set this

echo -e "${BLUE}📍 Installation directory: $PROJECT_DIR${NC}"

# Check if directory exists
if [ -d "$PROJECT_DIR" ]; then
    echo -e "${YELLOW}⚠️  Directory exists. Updating...${NC}"
    cd "$PROJECT_DIR"
    git pull origin main || {
        echo -e "${RED}❌ Git pull failed. Manual intervention required.${NC}"
        exit 1
    }
else
    echo -e "${BLUE}📥 Cloning repository...${NC}"
    git clone "$REPO_URL" "$PROJECT_DIR" || {
        echo -e "${RED}❌ Git clone failed. Please check the repository URL.${NC}"
        echo "Manual installation:"
        echo "1. Download the project files to $PROJECT_DIR"
        echo "2. Run the setup script: cd $PROJECT_DIR && ./scripts/setup.sh"
        exit 1
    }
    cd "$PROJECT_DIR"
fi

# Make scripts executable
chmod +x scripts/*.sh
chmod +x scripts/*.js

# Run setup
echo -e "${BLUE}🔧 Running setup...${NC}"
./scripts/setup.sh

echo
echo -e "${GREEN}🎉 Deployment completed!${NC}"
echo
echo -e "${BLUE}📋 Next steps:${NC}"
echo "1. cd $PROJECT_DIR"
echo "2. Update .env with your Redis URL"
echo "3. npm run start:worker"
echo
echo -e "${BLUE}💡 Useful commands:${NC}"
echo "• Health check: ${GREEN}npm run health${NC}"
echo "• View logs: ${GREEN}tail -f worker.log${NC}"
echo "• Stop worker: ${GREEN}Ctrl+C${NC}"
echo
echo -e "${YELLOW}🔗 Don't forget to deploy the API to Vercel!${NC}"