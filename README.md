# Project Prestige - Phase 2: NFL Validation Engine

## Overview

Project Prestige Phase 2 is a sophisticated, containerized microservices application designed to process and analyze NFL betting data. The system consists of multiple services that work together to ingest, process, and store NFL game data for backtesting and validation purposes.

## Architecture

The system follows a microservices architecture with the following components:

- **PostgreSQL Database**: Persistent storage for NFL game data and betting lines
- **RabbitMQ Message Queue**: Asynchronous communication between services
- **NFL Ingestor Service**: Reads CSV data and publishes to message queue
- **NFL Processor Service**: Consumes messages and stores data in PostgreSQL

## System Requirements

- Docker and Docker Compose
- At least 4GB RAM available for Docker
- NFL dataset in CSV format

## Project Structure

```
project_prestige/
├── nfl_ingestor/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── nfl_processor/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── data/
│   └── nfl_basic.csv          # Place your NFL data file here
├── docker-compose.yml
├── schema.sql
├── .env.example
└── README.md
```

## Setup Instructions

### 1. Prepare the Data Directory

Create a `data` directory in the project root and place your NFL CSV file:

```bash
mkdir data
# Copy your nfl_basic.csv file to the data directory
cp /path/to/your/nfl_basic.csv ./data/nfl_basic.csv
```

### 2. Environment Configuration

Copy the example environment file and customize if needed:

```bash
cp .env.example .env
```

### 3. Build and Start the Services

Run the entire system with a single command:

```bash
docker-compose up --build
```

This will:
- Start PostgreSQL database with the schema automatically loaded
- Start RabbitMQ message queue
- Build and start the NFL ingestor service
- Build and start the NFL processor service

### 4. Monitor the System

You can monitor the system through various interfaces:

- **Application logs**: `docker-compose logs -f`
- **RabbitMQ Management UI**: http://localhost:15672 (guest/guest)
- **PostgreSQL**: Connect on localhost:5432 (postgres/password)

## Data Flow

The complete end-to-end data flow is as follows:

1. **CSV Reading**: The `nfl_ingestor` service reads the NFL CSV file row by row
2. **Data Validation**: Each row is validated and parsed into structured JSON
3. **Message Publishing**: Valid game data is published to RabbitMQ queue
4. **Message Processing**: The `nfl_processor` consumes messages from the queue
5. **Database Storage**: Game data is inserted/updated in PostgreSQL tables

## Database Schema

The system uses a comprehensive PostgreSQL schema designed for NFL analytics:

### Main Tables

- **games**: Primary table storing all game data including:
  - Game metadata (ID, date, teams, scores)
  - Betting lines (spreads, moneylines, totals)
  - Opening and closing odds

### Views

- **game_results**: Analytical view showing:
  - Game winners and point differentials
  - Betting outcomes (spread covers, totals results)
  - Performance metrics

## Service Details

### NFL Ingestor Service

- **Purpose**: Read and validate NFL data from CSV files
- **Features**:
  - Robust CSV parsing with error handling
  - Data type validation and conversion
  - Configurable processing intervals
  - Structured JSON logging

### NFL Processor Service

- **Purpose**: Store validated game data in PostgreSQL
- **Features**:
  - Resilient database connections with retry logic
  - Duplicate handling with UPSERT operations
  - Transaction safety with proper rollback
  - Message acknowledgment handling

## Configuration

### Key Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `INGESTOR_INTERVAL` | Seconds between processing rows | 5 |
| `NFL_DATA_FILE` | Path to NFL CSV data file | `/app/data/nfl_basic.csv` |
| `RABBITMQ_QUEUE` | Message queue name | `game_events` |
| `DB_NAME` | PostgreSQL database name | `nfl_analytics` |
| `LOG_LEVEL` | Logging level (INFO, DEBUG, etc.) | `INFO` |

## Development and Debugging

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f nfl_ingestor
docker-compose logs -f nfl_processor
```

### Database Access

Connect to PostgreSQL to verify data:

```bash
# Connect to database
docker exec -it prestige_postgres psql -U postgres -d nfl_analytics

# Sample queries
SELECT COUNT(*) FROM games;
SELECT * FROM games LIMIT 5;
SELECT * FROM game_results WHERE game_date >= '2009-09-01' LIMIT 10;
```

### RabbitMQ Monitoring

Access the management interface at http://localhost:15672 to:
- Monitor queue status
- View message rates
- Debug connection issues

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Ensure PostgreSQL service is healthy: `docker-compose ps`
   - Check database credentials in environment variables

2. **CSV File Not Found**
   - Verify the data file exists: `ls -la ./data/`
   - Check file permissions and path configuration

3. **RabbitMQ Connection Issues**
   - Verify RabbitMQ is running: `docker-compose logs rabbitmq`
   - Check network connectivity between services

### Health Checks

The system includes comprehensive health checks:

```bash
# Check service status
docker-compose ps

# Verify database connectivity
docker exec prestige_postgres pg_isready -U postgres

# Check RabbitMQ status
docker exec prestige_rabbitmq rabbitmq-diagnostics ping
```

## Data Quality

The system includes robust data validation:

- **Required Fields**: Game ID, Date, Team Names
- **Data Types**: Automatic conversion with error handling
- **Date Validation**: YYYYMMDD format parsing
- **Duplicate Handling**: UPSERT operations prevent data conflicts

## Performance Considerations

- **Processing Rate**: Configurable via `INGESTOR_INTERVAL`
- **Memory Usage**: Services use minimal memory footprint
- **Database Performance**: Optimized with indexes on common query columns
- **Message Durability**: Persistent queues and messages
