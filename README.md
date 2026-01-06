# Acid Rain Event Generator

A multilingual typing game (English/Korean) that generates behavioral event data for analytics pipelines.

## Features

# AcidRain Event Platform

## Overview
AcidRain Event is a cloud-native event processing and analytics platform built on Google Cloud Platform. It powers a real-time typing game and dashboard, ingesting, processing, and visualizing user events with scalable serverless infrastructure.

## Architecture

```
┌───────────────┐
│   Browser     │
│  (JS App)     │
│ - Typing Game │
│ - Dashboard   │
└───────┬───────┘
  │ HTTP
  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Google Cloud Platform                      │
│                                                                   │
│   ┌─────────────────────────────┐                                │
│   │ Cloud Run                   │                                │
│   │ Event Ingest API            │                                │
│   │                             │                                │
│   │ - validate event            │                                │
│   │ - enrich metadata           │                                │
│   └───────────┬─────────────────┘                                │
│               │ write (JSON)                                      │
│               ▼                                                   │
│   ┌─────────────────────────────┐                                │
│   │ Google Cloud Storage        │                                │
│   │ Raw Events Bucket           │                                │
│   │                             │                                │
│   │ raw/YYYY/MM/DD/*.json       │                                │
│   └───────────┬─────────────────┘                                │
│               │                                                   │
│     (scheduled trigger)                                           │
│               │                                                   │
│   ┌───────────▼───────────┐        ┌─────────────────────────────┐│
│   │ Cloud Scheduler        │ ...... │ Cloud Logging / Monitoring  ││
│   │ (Cron)                 │        └─────────────────────────────┘│
│   └───────────┬───────────┘                                      │
│               │ HTTP                                              │
│               ▼                                                   │
│   ┌─────────────────────────────┐                                │
│   │ Cloud Run / Cloud Function  │                                │
│   │ Batch Trigger API           │                                │
│   │                             │                                │
│   │ - create Dataproc batch     │                                │
│   │ - read watermark            │                                │
│   └───────────┬─────────────────┘                                │
│               │ create batch                                    │
│               ▼                                                   │
│   ┌─────────────────────────────┐                                │
│   │ Dataproc Serverless         │                                │
│   │ (Spark)                     │                                │
│   │                             │                                │
│   │ - read raw events (GCS)     │                                │
│   │ - validate / transform      │                                │
│   │ - watermark-based ETL       │                                │
│   └───────────┬─────────────────┘                                │
│               │ append                                            │
│               ▼                                                   │
│   ┌─────────────────────────────┐                                │
│   │ BigQuery                    │                                │
│   │                             │                                │
│   │ - sessions table            │                                │
│   │ - attempts table            │                                │
│   │ - words dimension           │                                │
│   └───────────┬─────────────────┘                                │
│               │ SQL                                               │
│               ▼                                                   │
│   ┌─────────────────────────────┐                                │
│   │ Cloud Run                   │                                │
│   │ Dashboard API               │                                │
│   │                             │                                │
│   │ - query BigQuery            │                                │
│   │ - aggregate metrics         │                                │
│   └───────────┬─────────────────┘                                │
│               │ JSON                                              │
│               ▼                                                   │
│   ┌─────────────────────────────┐                                │
│   │ Dashboard UI                │                                │
│   │ (HTML / JS)                 │                                │
│   └─────────────────────────────┘                                │
│                                                                   │
└─────────────────────────────────────────────────────────────────────┘

CI / CD
=======
┌────────────┐     ┌──────────────┐     ┌──────────────────────┐
│   GitHub   │ --> │ Cloud Build  │ --> │ Cloud Run / GCS Deploy │
│ (main)     │     │ Triggers     │     │ + Spark Job Upload    │
└────────────┘     └──────────────┘     └──────────────────────┘
```

## Components

- **Frontend (app/):**
  - Typing game and dashboard UI (HTML/JS/CSS)
- **Event Ingest API (main.py):**
  - Validates and enriches incoming events
  - Deployable to Cloud Run
- **Batch Trigger API (acidrain-batch-trigger/):**
  - Schedules and triggers Dataproc Spark jobs
  - Reads watermark for batch processing
- **Dashboard API (acidrain-dashboard-api/):**
  - Aggregates and serves metrics from BigQuery
- **Spark Jobs (spark_jobs/):**
  - ETL pipeline for event validation, transformation, and loading to BigQuery
- **Infrastructure & Deployment:**
  - `Dockerfile`, `app.yaml`, deployment scripts for App Engine and Cloud Run
  - Cloud Scheduler for batch triggers
  - Cloud Build for CI/CD

## Data Flow
1. **User events** are sent from the browser to the Event Ingest API.
2. **Events** are validated and stored as JSON in Google Cloud Storage.
3. **Cloud Scheduler** triggers batch jobs via the Batch Trigger API.
4. **Dataproc Spark jobs** process raw events, validate, transform, and load to BigQuery.
5. **Dashboard API** queries BigQuery for metrics and serves them to the dashboard UI.

## Project Structure

- `main.py`                — Event Ingest API
- `acidrain-batch-trigger/` — Batch Trigger API
- `acidrain-dashboard-api/` — Dashboard API
- `spark_jobs/`            — Spark ETL jobs
- `app/`                   — Frontend (HTML/JS/CSS)
- `Dockerfile`, `app.yaml` — Deployment configuration
- `deploy-cloudrun.sh`, `deploy-appengine.sh` — Deployment scripts

## Architecture

```
Frontend (HTML/JS) → FastAPI → GCS (raw zone)
                                ↓
                    gzip-compressed JSONL
                    raw/YYYY/MM/DD/events_{uuid}.json.gz
```

## Event Schema

### Event Structure
```json
{
  "event_id": "uuid-v4",
  "session_id": "uuid-v4",
  "event_type": "word_typed_correct",
  "timestamp": "2025-12-21T10:30:45.123Z",
  "metadata": {
    "word": "example",
    "time_to_type_ms": 420,
    "current_speed": 1.8,
    "page_url": "https://...",
    "user_agent": "Mozilla/5.0 ..."
  },
  "web_context": {
    "timezone": "Asia/Seoul",
    "language": "en-US",
    "country": "KR",
    "region": "11"
  }
}
```


## API Endpoints

### `POST /events`
Stateless ingestion endpoint - no validation, transformation, or deduplication

**Request**: JSON array of events  
**Response**: Upload confirmation with GCS path  
**Side Effect**: Writes `raw/YYYY/MM/DD/events_{uuid}.json.gz`

### `GET /health`
Health check with GCS status

## Design Principles

✅ **Stateless**: Each request is independent  
✅ **Append-only**: Immutable raw zone  
✅ **No validation**: Accept all events as-is  
✅ **Compression at write**: gzip in-memory before upload  
✅ **Session-level context**: Collect once, reuse for all events  
✅ **UTC partitioning**: Daily date-based partitions
