#!/bin/bash

# GCP Airflow Setup Script
# This script sets up Airflow on a GCP Compute Engine instance

set -e

echo "🚀 Starting GCP Airflow Setup..."

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

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root"
   exit 1
fi

# Update system packages
print_status "Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install Docker
print_status "Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    sudo usermod -aG docker $USER
    rm get-docker.sh
    print_warning "Please log out and back in for Docker group changes to take effect"
else
    print_status "Docker is already installed"
fi

# Install Docker Compose
print_status "Installing Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
else
    print_status "Docker Compose is already installed"
fi

# Install Git
print_status "Installing Git..."
sudo apt-get install -y git

# Install Google Cloud SDK (if not already installed)
print_status "Installing Google Cloud SDK..."
if ! command -v gcloud &> /dev/null; then
    curl https://sdk.cloud.google.com | bash
    source ~/.bashrc
else
    print_status "Google Cloud SDK is already installed"
fi

# Create project directory
PROJECT_DIR="/opt/airflow"
print_status "Creating project directory at $PROJECT_DIR..."
sudo mkdir -p $PROJECT_DIR
sudo chown $USER:$USER $PROJECT_DIR

# Create necessary directories
print_status "Creating Airflow directories..."
mkdir -p $PROJECT_DIR/{dags,logs,plugins,config/secrets}

# Set up environment file
print_status "Setting up environment configuration..."
cat > $PROJECT_DIR/env.gcp << 'EOF'
# GCP Environment Configuration for Airflow
ENV=GCP

# Airflow Configuration
AIRFLOW_VERSION=2.10.5
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=airflow-cre-app
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Project Configuration
AIRFLOW_GID=0
AIRFLOW_PROJ_DIR=.
AIRFLOW_DAGS_PATH=./dags
AIRFLOW_SECRETS_PATH=./config/secrets

# GCP Configuration
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/secrets/airflow-service-account.json

# Database Configuration (Cloud SQL)
DB_HOST=your-cloud-sql-ip
DB_PORT=5432
DB_PASSWORD=your-db-password

# Airflow Security
AIRFLOW_FERNET_KEY=your-fernet-key-here

# GCS Configuration
GCS_LOGS_BUCKET=your-airflow-logs-bucket

# Additional Requirements (if needed)
_PIP_ADDITIONAL_REQUIREMENTS=
EOF

print_warning "Please update the environment variables in $PROJECT_DIR/env.gcp with your actual values"

# Create Dockerfile
print_status "Creating Dockerfile..."
cat > $PROJECT_DIR/Dockerfile << 'EOF'
FROM apache/airflow:2.10.5
WORKDIR /opt/airflow

USER root

# Install Chrome
RUN apt-get update && apt-get install -y wget unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
EOF

# Create requirements.txt
print_status "Creating requirements.txt..."
cat > $PROJECT_DIR/requirements.txt << 'EOF'
google 
requests 
pandas
geopandas
shapely
sqlalchemy
bs4
openai
PyMuPDF
python-dotenv
sodapy
tqdm
selenium
webdriver-manager
lxml
beautifulsoup4
pinecone
langchain
streamlit
ollama
youtube-transcript-api
langchain-openai
langchain-pinecone
langchain-core
apache-airflow-providers-google
apache-airflow-providers-postgres
EOF

# Create docker-compose file
print_status "Creating docker-compose-gcp.yaml..."
cat > $PROJECT_DIR/docker-compose-gcp.yaml << 'EOF'
# GCP-optimized Airflow cluster configuration for LocalExecutor with Cloud SQL PostgreSQL.
---
x-airflow-common:
  &airflow-common
  image: ${AIRFLOW_IMAGE_NAME:-airflow-cre-app}
  build: .
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/airflow
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
    # GCP-specific configurations
    GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
    GOOGLE_CLOUD_PROJECT: ${GOOGLE_CLOUD_PROJECT}
    # Use GCS for logs
    AIRFLOW__LOGGING__REMOTE_LOGGING: 'true'
    AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID: 'gcs_default'
    AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER: 'gs://${GCS_LOGS_BUCKET}/airflow-logs'
    AIRFLOW__LOGGING__DELETE_LOCAL_LOGS: 'true'
  volumes:
    - ${AIRFLOW_DAGS_PATH}:/opt/airflow/dags
    - ${AIRFLOW_SECRETS_PATH}:/keys:ro
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    &airflow-common-depends-on
    airflow-init:
      condition: service_completed_successfully

services:
  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on

  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        if [[ -z "${AIRFLOW_UID}" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
          echo "If you are on Linux, you SHOULD follow the instructions below to set "
          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."
          echo "For other operating systems you can get rid of the warning with manually created .env file:"
          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"
          echo
        fi
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
      _PIP_ADDITIONAL_REQUIREMENTS: ''
    user: "0:0"
    volumes:
      - ${AIRFLOW_PROJ_DIR:-.}:/sources

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    command:
      - bash
      - -c
      - airflow
EOF

# Create startup script
print_status "Creating startup script..."
cat > $PROJECT_DIR/start-airflow.sh << 'EOF'
#!/bin/bash

# Start Airflow on GCP
set -e

echo "🚀 Starting Airflow on GCP..."

# Load environment variables
if [ -f env.gcp ]; then
    export $(cat env.gcp | grep -v '^#' | xargs)
fi

# Build the Docker image
echo "Building Airflow Docker image..."
docker-compose -f docker-compose-gcp.yaml build

# Start Airflow services
echo "Starting Airflow services..."
docker-compose -f docker-compose-gcp.yaml up -d

echo "✅ Airflow is starting up!"
echo "🌐 Web UI will be available at: http://localhost:8080"
echo "👤 Default credentials: airflow / airflow"
echo ""
echo "To view logs: docker-compose -f docker-compose-gcp.yaml logs -f"
echo "To stop: docker-compose -f docker-compose-gcp.yaml down"
EOF

chmod +x $PROJECT_DIR/start-airflow.sh

# Create stop script
print_status "Creating stop script..."
cat > $PROJECT_DIR/stop-airflow.sh << 'EOF'
#!/bin/bash

# Stop Airflow on GCP
echo "🛑 Stopping Airflow services..."
docker-compose -f docker-compose-gcp.yaml down

echo "✅ Airflow stopped!"
EOF

chmod +x $PROJECT_DIR/stop-airflow.sh

# Create systemd service for auto-start
print_status "Creating systemd service..."
sudo tee /etc/systemd/system/airflow.service > /dev/null << EOF
[Unit]
Description=Airflow on GCP
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/start-airflow.sh
ExecStop=$PROJECT_DIR/stop-airflow.sh
User=$USER
Group=$USER

[Install]
WantedBy=multi-user.target
EOF

# Enable the service
sudo systemctl daemon-reload
sudo systemctl enable airflow.service

print_status "Setup complete! 🎉"
print_warning "Next steps:"
echo "1. Update $PROJECT_DIR/env.gcp with your actual values"
echo "2. Place your service account key in $PROJECT_DIR/config/secrets/"
echo "3. Clone your DAGs repository to $PROJECT_DIR/dags/"
echo "4. Run: $PROJECT_DIR/start-airflow.sh"
echo ""
print_warning "To start Airflow automatically on boot:"
echo "sudo systemctl start airflow"
echo ""
print_warning "To check status:"
echo "sudo systemctl status airflow"
