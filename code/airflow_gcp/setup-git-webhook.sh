#!/bin/bash

# Git Webhook Setup Script for Automated DAG Deployment
# This script sets up a webhook to automatically deploy DAGs when changes are pushed to the repository

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
PROJECT_DIR="/opt/airflow"
WEBHOOK_DIR="/opt/airflow/webhook"
WEBHOOK_PORT=9000

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root"
   exit 1
fi

print_status "Setting up Git webhook for automated DAG deployment..."

# Install required packages
print_status "Installing required packages..."
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv nginx

# Create webhook directory
print_status "Creating webhook directory..."
mkdir -p "$WEBHOOK_DIR"
cd "$WEBHOOK_DIR"

# Create virtual environment
print_status "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Flask and other dependencies
print_status "Installing webhook dependencies..."
pip install flask requests

# Create webhook server
print_status "Creating webhook server..."
cat > webhook_server.py << 'EOF'
#!/usr/bin/env python3
"""
Git Webhook Server for Automated DAG Deployment
Listens for GitHub/GitLab webhooks and automatically deploys DAGs
"""

import os
import json
import subprocess
import logging
from flask import Flask, request, jsonify
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/webhook/webhook.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
PROJECT_DIR = "/opt/airflow"
DAG_DEPLOYMENT_SCRIPT = f"{PROJECT_DIR}/deploy-dags.sh"
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', 'your-webhook-secret-here')

def verify_signature(payload, signature, secret):
    """Verify webhook signature for security"""
    import hmac
    import hashlib
    
    if not signature or not secret:
        return False
    
    # Remove 'sha256=' prefix if present
    if signature.startswith('sha256='):
        signature = signature[7:]
    
    # Create expected signature
    expected_signature = hmac.new(
        secret.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(signature, expected_signature)

def deploy_dags(repo_url, branch='main', dag_subdir='dags'):
    """Deploy DAGs using the deployment script"""
    try:
        logger.info(f"Deploying DAGs from {repo_url} (branch: {branch})")
        
        # Run the deployment script
        cmd = [
            DAG_DEPLOYMENT_SCRIPT,
            '-r', repo_url,
            '-b', branch,
            '-d', dag_subdir,
            '-f'  # Force deployment
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=PROJECT_DIR
        )
        
        if result.returncode == 0:
            logger.info("DAG deployment successful")
            return True, result.stdout
        else:
            logger.error(f"DAG deployment failed: {result.stderr}")
            return False, result.stderr
            
    except Exception as e:
        logger.error(f"Error during DAG deployment: {str(e)}")
        return False, str(e)

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle webhook requests"""
    try:
        # Get the raw payload
        payload = request.get_data()
        
        # Get signature header
        signature = request.headers.get('X-Hub-Signature-256') or request.headers.get('X-Hub-Signature')
        
        # Verify signature if secret is configured
        if WEBHOOK_SECRET != 'your-webhook-secret-here':
            if not verify_signature(payload, signature, WEBHOOK_SECRET):
                logger.warning("Invalid webhook signature")
                return jsonify({'error': 'Invalid signature'}), 401
        
        # Parse JSON payload
        try:
            data = json.loads(payload.decode('utf-8'))
        except json.JSONDecodeError:
            logger.error("Invalid JSON payload")
            return jsonify({'error': 'Invalid JSON'}), 400
        
        # Extract repository information
        repo_url = None
        branch = 'main'
        
        # Handle GitHub webhooks
        if 'repository' in data:
            repo_url = data['repository'].get('clone_url') or data['repository'].get('ssh_url')
            if 'ref' in data:
                branch = data['ref'].replace('refs/heads/', '')
        
        # Handle GitLab webhooks
        elif 'project' in data:
            repo_url = data['project'].get('git_http_url') or data['project'].get('git_ssh_url')
            if 'ref' in data:
                branch = data['ref']
        
        if not repo_url:
            logger.error("Could not determine repository URL from webhook")
            return jsonify({'error': 'Repository URL not found'}), 400
        
        # Check if this is a push to the main branch (or configured branch)
        if 'ref' in data and not data['ref'].endswith(f'/{branch}'):
            logger.info(f"Ignoring push to branch {data['ref']} (not {branch})")
            return jsonify({'message': 'Ignored - not target branch'}), 200
        
        # Deploy DAGs
        success, message = deploy_dags(repo_url, branch)
        
        if success:
            logger.info("Webhook processed successfully")
            return jsonify({
                'status': 'success',
                'message': 'DAGs deployed successfully',
                'details': message
            }), 200
        else:
            logger.error(f"Webhook processing failed: {message}")
            return jsonify({
                'status': 'error',
                'message': 'DAG deployment failed',
                'details': message
            }), 500
            
    except Exception as e:
        logger.error(f"Webhook processing error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

@app.route('/', methods=['GET'])
def index():
    """Root endpoint"""
    return jsonify({
        'service': 'Airflow DAG Webhook Server',
        'version': '1.0.0',
        'endpoints': {
            'webhook': '/webhook',
            'health': '/health'
        }
    }), 200

if __name__ == '__main__':
    logger.info("Starting webhook server...")
    app.run(host='0.0.0.0', port=9000, debug=False)
EOF

# Make webhook server executable
chmod +x webhook_server.py

# Create systemd service for webhook
print_status "Creating systemd service for webhook..."
sudo tee /etc/systemd/system/airflow-webhook.service > /dev/null << EOF
[Unit]
Description=Airflow DAG Webhook Server
After=network.target

[Service]
Type=simple
User=$USER
Group=$USER
WorkingDirectory=$WEBHOOK_DIR
Environment=PATH=$WEBHOOK_DIR/venv/bin
Environment=WEBHOOK_SECRET=your-webhook-secret-here
ExecStart=$WEBHOOK_DIR/venv/bin/python webhook_server.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Create nginx configuration for webhook
print_status "Creating nginx configuration..."
sudo tee /etc/nginx/sites-available/airflow-webhook > /dev/null << EOF
server {
    listen 80;
    server_name your-domain.com;  # Replace with your domain or IP

    location /webhook {
        proxy_pass http://localhost:$WEBHOOK_PORT;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        
        # Increase timeout for large payloads
        proxy_read_timeout 60s;
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
    }

    location /health {
        proxy_pass http://localhost:$WEBHOOK_PORT;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

# Enable nginx site
sudo ln -sf /etc/nginx/sites-available/airflow-webhook /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# Enable and start webhook service
print_status "Enabling webhook service..."
sudo systemctl daemon-reload
sudo systemctl enable airflow-webhook.service
sudo systemctl start airflow-webhook.service

# Create webhook configuration file
print_status "Creating webhook configuration..."
cat > webhook_config.json << EOF
{
    "webhook_url": "http://your-domain.com/webhook",
    "github_webhook_url": "https://api.github.com/repos/your-username/your-repo/hooks",
    "gitlab_webhook_url": "https://gitlab.com/api/v4/projects/your-project-id/hooks",
    "secret": "your-webhook-secret-here",
    "target_branch": "main",
    "dag_subdirectory": "dags"
}
EOF

# Create webhook setup script
print_status "Creating webhook setup script..."
cat > setup_github_webhook.py << 'EOF'
#!/usr/bin/env python3
"""
Setup GitHub webhook for automated DAG deployment
"""

import requests
import json
import sys

def setup_github_webhook(repo_owner, repo_name, webhook_url, secret, token):
    """Setup GitHub webhook"""
    
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/hooks"
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    payload = {
        "name": "web",
        "active": True,
        "events": ["push"],
        "config": {
            "url": webhook_url,
            "content_type": "json",
            "secret": secret
        }
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 201:
        print("✅ GitHub webhook created successfully!")
        return True
    else:
        print(f"❌ Failed to create GitHub webhook: {response.text}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python3 setup_github_webhook.py <repo_owner> <repo_name> <webhook_url> <secret> <github_token>")
        sys.exit(1)
    
    repo_owner, repo_name, webhook_url, secret, token = sys.argv[1:6]
    setup_github_webhook(repo_owner, repo_name, webhook_url, secret, token)
EOF

chmod +x setup_github_webhook.py

print_status "✅ Git webhook setup completed!"
print_warning "Next steps:"
echo "1. Update webhook configuration in $WEBHOOK_DIR/webhook_config.json"
echo "2. Set your webhook secret: sudo systemctl edit airflow-webhook.service"
echo "3. Configure your domain in /etc/nginx/sites-available/airflow-webhook"
echo "4. Setup GitHub/GitLab webhook using the provided script"
echo ""
print_warning "Webhook endpoints:"
echo "- Webhook URL: http://your-domain.com/webhook"
echo "- Health check: http://your-domain.com/health"
echo ""
print_warning "To setup GitHub webhook:"
echo "python3 $WEBHOOK_DIR/setup_github_webhook.py <owner> <repo> <webhook_url> <secret> <github_token>"
echo ""
print_warning "To check webhook status:"
echo "sudo systemctl status airflow-webhook.service"
echo "tail -f $WEBHOOK_DIR/webhook.log"
