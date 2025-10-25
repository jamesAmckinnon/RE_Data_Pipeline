# Airflow GCP Deployment Guide

This guide walks you through deploying your Airflow setup to Google Cloud Platform (GCP) Compute Engine.

## Prerequisites

- GCP Compute Engine VM instance running Ubuntu 20.04+ or similar
- Google Cloud SDK installed and configured
- Docker and Docker Compose installed
- Git repository containing your DAGs

## Quick Start

### 1. Initial Setup

Run the setup script on your GCP instance:

```bash
# Download and run the setup script
curl -fsSL https://raw.githubusercontent.com/your-repo/airflow-gcp/main/setup-gcp.sh | bash
```

Or manually:

```bash
# Clone this repository
git clone <your-repo-url> /opt/airflow
cd /opt/airflow

# Make setup script executable
chmod +x setup-gcp.sh

# Run setup
./setup-gcp.sh
```

### 2. Configure Environment

Edit the environment file with your GCP settings:

```bash
nano /opt/airflow/env.gcp
```

Update the following variables:

```bash
# GCP Configuration
GOOGLE_CLOUD_PROJECT=your-actual-project-id
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/secrets/airflow-service-account.json

# Database Configuration (Cloud SQL)
DB_HOST=your-cloud-sql-ip-address
DB_PORT=5432
DB_PASSWORD=your-secure-db-password

# Airflow Security
AIRFLOW_FERNET_KEY=your-generated-fernet-key

# GCS Configuration
GCS_LOGS_BUCKET=your-airflow-logs-bucket
```

### 3. Set Up Cloud SQL Database

Create a Cloud SQL PostgreSQL instance:

```bash
# Create Cloud SQL instance
gcloud sql instances create airflow-db \
    --database-version=POSTGRES_13 \
    --tier=db-f1-micro \
    --region=us-central1 \
    --storage-type=SSD \
    --storage-size=10GB

# Create database
gcloud sql databases create airflow --instance=airflow-db

# Create user
gcloud sql users create airflow \
    --instance=airflow-db \
    --password=your-secure-password
```

### 4. Set Up Service Account

Create a service account for Airflow:

```bash
# Create service account
gcloud iam service-accounts create airflow-service-account \
    --display-name="Airflow Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:airflow-service-account@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:airflow-service-account@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/cloudsql.client"

# Create and download key
gcloud iam service-accounts keys create /opt/airflow/config/secrets/airflow-service-account.json \
    --iam-account=airflow-service-account@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 5. Set Up GCS Buckets

Create GCS buckets for logs and data:

```bash
# Create logs bucket
gsutil mb gs://your-airflow-logs-bucket

# Create data buckets (if not already created)
gsutil mb gs://cre-property-listings
gsutil mb gs://cre-financial-reports
gsutil mb gs://council-transcripts
```

### 6. Deploy DAGs

Deploy your DAGs from your Git repository:

```bash
# Deploy DAGs from Git repository
./deploy-dags.sh -r https://github.com/your-username/your-dags-repo.git -b main -d dags
```

### 7. Start Airflow

Start the Airflow services:

```bash
# Start Airflow
./start-airflow.sh
```

## Configuration Details

### Environment Variables

The `env.gcp` file contains all necessary environment variables:

- **Database**: Cloud SQL PostgreSQL connection
- **GCP**: Project ID and service account credentials
- **Security**: Fernet key for encryption
- **Storage**: GCS buckets for logs and data

### Docker Configuration

The setup uses a custom Docker image with:
- Apache Airflow 2.10.5
- Chrome browser for web scraping
- All required Python packages
- GCP-specific configurations

### File Structure

```
/opt/airflow/
├── dags/                    # Your DAG files
├── logs/                    # Local logs (backed up to GCS)
├── plugins/                 # Airflow plugins
├── config/
│   └── secrets/            # Service account keys
├── docker-compose-gcp.yaml # GCP-optimized compose file
├── Dockerfile              # Custom Airflow image
├── requirements.txt        # Python dependencies
├── env.gcp                 # Environment configuration
├── start-airflow.sh        # Startup script
├── stop-airflow.sh         # Stop script
└── deploy-dags.sh          # DAG deployment script
```

## Management Commands

### Start/Stop Airflow

```bash
# Start Airflow
./start-airflow.sh

# Stop Airflow
./stop-airflow.sh

# Check status
docker-compose -f docker-compose-gcp.yaml ps
```

### View Logs

```bash
# View all logs
docker-compose -f docker-compose-gcp.yaml logs -f

# View specific service logs
docker-compose -f docker-compose-gcp.yaml logs -f airflow-webserver
docker-compose -f docker-compose-gcp.yaml logs -f airflow-scheduler
```

### Update DAGs

```bash
# Update DAGs from Git repository
./deploy-dags.sh -r https://github.com/your-username/your-dags-repo.git -f
```

### Airflow CLI

```bash
# Access Airflow CLI
docker-compose -f docker-compose-gcp.yaml exec airflow-webserver airflow dags list
docker-compose -f docker-compose-gcp.yaml exec airflow-webserver airflow dags trigger <dag_id>
```

## Auto-Start on Boot

Enable Airflow to start automatically on boot:

```bash
# Enable systemd service
sudo systemctl enable airflow.service
sudo systemctl start airflow.service

# Check status
sudo systemctl status airflow.service
```

## Monitoring and Maintenance

### Health Checks

- **Web UI**: http://your-vm-ip:8080
- **Health endpoint**: http://your-vm-ip:8080/health
- **Scheduler health**: http://your-vm-ip:8974/health

### Log Management

- Local logs are automatically synced to GCS
- Logs are stored in: `gs://your-airflow-logs-bucket/airflow-logs/`
- Old local logs are automatically deleted

### Database Maintenance

```bash
# Connect to Cloud SQL
gcloud sql connect airflow-db --user=airflow

# Check database size
gcloud sql instances describe airflow-db
```

## Troubleshooting

### Common Issues

1. **Permission denied errors**
   ```bash
   sudo chown -R $USER:$USER /opt/airflow
   ```

2. **Database connection issues**
   - Check Cloud SQL instance is running
   - Verify firewall rules allow connections
   - Verify credentials in env.gcp

3. **DAG not appearing**
   - Check DAG syntax: `airflow dags list`
   - Verify file permissions
   - Check scheduler logs

4. **Service account issues**
   - Verify service account key is in correct location
   - Check IAM permissions
   - Test with: `gcloud auth activate-service-account`

### Log Locations

- **Airflow logs**: `/opt/airflow/logs/`
- **Docker logs**: `docker-compose -f docker-compose-gcp.yaml logs`
- **System logs**: `journalctl -u airflow.service`

## Security Considerations

1. **Firewall Rules**: Only open necessary ports (8080 for web UI)
2. **Service Account**: Use least privilege principle
3. **Database**: Use strong passwords and enable SSL
4. **Secrets**: Store sensitive data in GCP Secret Manager
5. **Updates**: Regularly update Docker images and dependencies

## Scaling

For production workloads, consider:

1. **Cloud Composer**: Managed Airflow service
2. **Kubernetes**: GKE with Airflow on Kubernetes
3. **Multiple instances**: Load balancer with multiple Airflow instances
4. **Database**: Upgrade to higher-tier Cloud SQL instance

## Backup and Recovery

### Database Backup

```bash
# Create backup
gcloud sql backups create --instance=airflow-db

# Restore from backup
gcloud sql backups restore BACKUP_ID --instance=airflow-db
```

### DAG Backup

DAGs are version-controlled in Git, so recovery is automatic.

### Configuration Backup

```bash
# Backup configuration
tar -czf airflow-config-backup.tar.gz /opt/airflow/config/
```

## Support

For issues and questions:

1. Check the logs first
2. Review this documentation
3. Check Airflow documentation: https://airflow.apache.org/docs/
4. Check GCP documentation: https://cloud.google.com/docs
