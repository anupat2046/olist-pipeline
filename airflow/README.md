# Olist Pipeline - Airflow Configuration

This folder contains Airflow configuration using Astro CLI.

## Quick Start

```bash
# 1. Install Astro CLI (PowerShell as Admin)
winget install -e --id Astronomer.Astro

# 2. Start Airflow
cd airflow
astro dev start

# 3. Open UI
# http://localhost:8080
# Username: admin
# Password: admin

# 4. Stop when done
astro dev stop
```

## Setup GCP Connection

1. Open http://localhost:8080
2. Go to **Admin → Connections**
3. Click **+** to add new connection:
   - **Conn Id**: `google_cloud_default`
   - **Conn Type**: `Google Cloud`
   - **Keyfile Path**: `/opt/airflow/project/.google/service-account.json`
   - **Project Id**: `olist-de`

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `olist_master_pipeline_v1` | Daily 02:00 | Full pipeline (Silver → Gold) |

## Troubleshooting

```bash
# View logs
astro dev logs

# Restart
astro dev restart

# Force rebuild
astro dev start --build
```
