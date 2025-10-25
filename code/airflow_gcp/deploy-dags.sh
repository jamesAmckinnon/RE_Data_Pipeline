#!/bin/bash

# DAG Deployment Script for GCP Airflow
# This script clones DAGs from Git repository and sets up the environment

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
DAGS_DIR="$PROJECT_DIR/dags"
BACKUP_DIR="$PROJECT_DIR/dags-backup"

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root"
   exit 1
fi

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -r, --repo URL          Git repository URL for DAGs"
    echo "  -b, --branch BRANCH     Git branch to deploy (default: main)"
    echo "  -d, --directory DIR     Directory within repo containing DAGs (default: dags)"
    echo "  -f, --force             Force deployment even if DAGs directory exists"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -r https://github.com/username/airflow-dags.git"
    echo "  $0 -r https://github.com/username/airflow-dags.git -b develop -d airflow/dags"
}

# Default values
REPO_URL=""
BRANCH="main"
DAG_SUBDIR="dags"
FORCE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--repo)
            REPO_URL="$2"
            shift 2
            ;;
        -b|--branch)
            BRANCH="$2"
            shift 2
            ;;
        -d|--directory)
            DAG_SUBDIR="$2"
            shift 2
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$REPO_URL" ]]; then
    print_error "Repository URL is required"
    show_usage
    exit 1
fi

# Check if DAGs directory exists and handle accordingly
if [[ -d "$DAGS_DIR" && "$FORCE" == false ]]; then
    print_warning "DAGs directory already exists at $DAGS_DIR"
    echo "Use -f or --force to overwrite, or specify a different repository"
    exit 1
fi

# Create backup if DAGs directory exists
if [[ -d "$DAGS_DIR" ]]; then
    print_status "Creating backup of existing DAGs..."
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    mv "$DAGS_DIR" "$BACKUP_DIR-$TIMESTAMP"
    print_status "Backup created at $BACKUP_DIR-$TIMESTAMP"
fi

# Create temporary directory for cloning
TEMP_DIR=$(mktemp -d)
print_status "Using temporary directory: $TEMP_DIR"

# Clone the repository
print_status "Cloning repository: $REPO_URL"
print_status "Branch: $BRANCH"
print_status "DAG subdirectory: $DAG_SUBDIR"

cd "$TEMP_DIR"
git clone -b "$BRANCH" "$REPO_URL" repo

# Check if DAG subdirectory exists
if [[ ! -d "repo/$DAG_SUBDIR" ]]; then
    print_error "DAG subdirectory '$DAG_SUBDIR' not found in repository"
    print_status "Available directories:"
    ls -la repo/
    exit 1
fi

# Copy DAGs to the target directory
print_status "Copying DAGs to $DAGS_DIR..."
mkdir -p "$DAGS_DIR"
cp -r "repo/$DAG_SUBDIR"/* "$DAGS_DIR/"

# Set proper permissions
print_status "Setting permissions..."
chmod -R 755 "$DAGS_DIR"
chown -R $USER:$USER "$DAGS_DIR"

# Clean up temporary directory
print_status "Cleaning up..."
rm -rf "$TEMP_DIR"

# Verify deployment
print_status "Verifying deployment..."
DAG_COUNT=$(find "$DAGS_DIR" -name "*.py" | wc -l)
print_status "Deployed $DAG_COUNT Python files to $DAGS_DIR"

# List deployed files
print_status "Deployed files:"
find "$DAGS_DIR" -name "*.py" -exec basename {} \; | sort

# Check if Airflow is running and restart if necessary
if docker-compose -f "$PROJECT_DIR/docker-compose-gcp.yaml" ps | grep -q "Up"; then
    print_status "Airflow is running. DAGs will be automatically detected."
    print_status "You can check the Airflow UI at http://localhost:8080"
else
    print_warning "Airflow is not running. Start it with: $PROJECT_DIR/start-airflow.sh"
fi

print_status "✅ DAG deployment completed successfully!"

# Show next steps
echo ""
print_warning "Next steps:"
echo "1. Verify DAGs in the Airflow UI: http://localhost:8080"
echo "2. Check DAG syntax: docker-compose -f $PROJECT_DIR/docker-compose-gcp.yaml exec airflow-webserver airflow dags list"
echo "3. Test DAG execution: docker-compose -f $PROJECT_DIR/docker-compose-gcp.yaml exec airflow-webserver airflow dags trigger <dag_id>"
echo ""
print_warning "To update DAGs in the future, run this script again with the same parameters"
