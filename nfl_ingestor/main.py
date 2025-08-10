import os
import json
import time
import logging
import csv
import pika
from datetime import datetime, date
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# Load environment variables
load_dotenv()

class JSONFormatter(logging.Formatter):
    """Custom formatter to output logs in JSON format"""
    
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service_name": "nfl_ingestor",
            "message": record.getMessage()
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_entry)

def setup_logging():
    """Configure structured JSON logging"""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
    
    # Remove default handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add console handler with JSON formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JSONFormatter())
    logger.addHandler(console_handler)
    
    return logger

def create_rabbitmq_connection():
    """Create RabbitMQ connection with exponential backoff retry"""
    logger = logging.getLogger()
    
    host = os.getenv('RABBITMQ_HOST', 'localhost')
    port = int(os.getenv('RABBITMQ_PORT', 5672))
    user = os.getenv('RABBITMQ_USER', 'guest')
    password = os.getenv('RABBITMQ_PASS', 'guest')
    
    retry_delay = 1
    max_delay = 60
    
    while True:
        try:
            logger.info(f"Attempting to connect to RabbitMQ at {host}:{port}")
            
            credentials = pika.PlainCredentials(user, password)
            parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            connection = pika.BlockingConnection(parameters)
            logger.info("Successfully connected to RabbitMQ")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            logger.info(f"Retrying in {retry_delay} seconds...")
            
            time.sleep(retry_delay)
            
            # Exponential backoff with max cap
            retry_delay = min(retry_delay * 2, max_delay)

def parse_date(date_str: str) -> Optional[str]:
    """Parse date from YYYYMMDD format to YYYY-MM-DD"""
    try:
        if date_str and len(date_str) == 8:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            # Validate the date
            parsed_date = date(int(year), int(month), int(day))
            return parsed_date.isoformat()
        return None
    except (ValueError, TypeError):
        return None

def safe_int(value: str) -> Optional[int]:
    """Safely convert string to integer"""
    try:
        if value and value.strip():
            return int(value.strip())
        return None
    except (ValueError, TypeError):
        return None

def safe_float(value: str) -> Optional[float]:
    """Safely convert string to float"""
    try:
        if value and value.strip():
            return float(value.strip())
        return None
    except (ValueError, TypeError):
        return None

def validate_and_parse_row(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Validate and parse a CSV row into structured game data"""
    logger = logging.getLogger()
    
    try:
        # Required fields validation
        if not all([row.get('Game ID'), row.get('Date'), row.get('Away Team'), row.get('Home Team')]):
            logger.warning(f"Missing required fields in row: {row.get('Game ID', 'Unknown')}")
            return None
        
        # Parse and validate the game data
        game_data = {
            "game_id": safe_int(row.get('Game ID')),
            "game_date": parse_date(row.get('Date')),
            "away_team": row.get('Away Team', '').strip(),
            "away_score": safe_int(row.get('Away Score')),
            "home_team": row.get('Home Team', '').strip(),
            "home_score": safe_int(row.get('Home Score')),
            
            # Away team betting data
            "away_spread_open": safe_float(row.get('Away Spread Open')),
            "away_spread_open_odds": safe_int(row.get('Away Spread Open Odds')),
            "away_spread_close": safe_float(row.get('Away Spread Close')),
            "away_spread_close_odds": safe_int(row.get('Away Spread Close Odds')),
            "away_moneyline_open": safe_int(row.get('Away MoneyLine Open')),
            "away_moneyline_close": safe_int(row.get('Away MoneyLine Close')),
            
            # Home team betting data
            "home_spread_open": safe_float(row.get('Home Spread Open')),
            "home_spread_open_odds": safe_int(row.get('Home Spread Open Odds')),
            "home_spread_close": safe_float(row.get('Home Spread Close')),
            "home_spread_close_odds": safe_int(row.get('Home Spread Close Odds')),
            "home_moneyline_open": safe_int(row.get('Home MoneyLine Open')),
            "home_moneyline_close": safe_int(row.get('Home MoneyLine Close')),
            
            # Over/Under data
            "over_open": safe_float(row.get('Over Open')),
            "over_open_odds": safe_int(row.get('Over Open Odds')),
            "over_close": safe_float(row.get('Over Close')),
            "over_close_odds": safe_int(row.get('Over Close Odds')),
            "under_open": safe_float(row.get('Under Open')),
            "under_open_odds": safe_int(row.get('Under Open Odds')),
            "under_close": safe_float(row.get('Under Close')),
            "under_close_odds": safe_int(row.get('Under Close Odds')),
            
            # Processing metadata
            "processed_at": datetime.utcnow().isoformat() + "Z"
        }
        
        # Basic validation checks
        if not game_data["game_id"] or not game_data["game_date"]:
            logger.warning(f"Invalid game_id or date for row: {row.get('Game ID', 'Unknown')}")
            return None
            
        if not game_data["away_team"] or not game_data["home_team"]:
            logger.warning(f"Invalid team names for game: {game_data['game_id']}")
            return None
        
        return game_data
        
    except Exception as e:
        logger.error(f"Error parsing row {row.get('Game ID', 'Unknown')}: {str(e)}")
        return None

def read_nfl_data(file_path: str):
    """Generator to read NFL data from CSV file"""
    logger = logging.getLogger()
    
    if not os.path.exists(file_path):
        logger.error(f"NFL data file not found: {file_path}")
        raise FileNotFoundError(f"NFL data file not found: {file_path}")
    
    logger.info(f"Reading NFL data from: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        # Use DictReader to handle column headers
        reader = csv.DictReader(csvfile)
        
        row_count = 0
        for row in reader:
            row_count += 1
            
            # Validate and parse the row
            parsed_data = validate_and_parse_row(row)
            if parsed_data:
                yield parsed_data
            else:
                logger.warning(f"Skipped invalid row {row_count}")
        
        logger.info(f"Finished reading {row_count} rows from CSV file")

def publish_message(channel, queue_name: str, message: Dict[str, Any]):
    """Publish a message to the specified queue"""
    logger = logging.getLogger()
    
    try:
        # Declare queue (idempotent operation)
        channel.queue_declare(queue=queue_name, durable=True)
        
        # Publish message
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
        )
        
        logger.info(f"Published game data for Game ID {message.get('game_id')} ({message.get('away_team')} @ {message.get('home_team')})")
        
    except Exception as e:
        logger.error(f"Failed to publish message: {str(e)}")
        raise

def main():
    """Main service loop"""
    logger = setup_logging()
    logger.info("NFL Ingestor service starting up")
    
    # Get configuration
    queue_name = os.getenv('RABBITMQ_QUEUE', 'game_events')
    interval = int(os.getenv('INGESTOR_INTERVAL', 5))
    csv_file_path = os.getenv('NFL_DATA_FILE', '/app/data/nfl_basic.csv')
    
    logger.info(f"Configuration: Queue={queue_name}, Interval={interval}s, File={csv_file_path}")
    
    # Create RabbitMQ connection
    connection = create_rabbitmq_connection()
    channel = connection.channel()
    
    try:
        # Read and process NFL data
        games_processed = 0
        
        for game_data in read_nfl_data(csv_file_path):
            # Publish game data
            publish_message(channel, queue_name, game_data)
            games_processed += 1
            
            # Wait between messages to avoid overwhelming the system
            time.sleep(interval)
        
        logger.info(f"Successfully processed {games_processed} games")
        logger.info("All NFL data has been processed. Service will exit.")
        
    except FileNotFoundError as e:
        logger.error(f"Data file error: {str(e)}")
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping service")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {str(e)}")
    finally:
        try:
            connection.close()
            logger.info("RabbitMQ connection closed")
        except:
            pass
        logger.info("NFL Ingestor service shutdown complete")

if __name__ == "__main__":
    main()