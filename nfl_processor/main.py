import os
import json
import logging
import pika
import psycopg2
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables
load_dotenv()

class JSONFormatter(logging.Formatter):
    """Custom formatter to output logs in JSON format"""
    
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service_name": "nfl_processor",
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

def create_database_connection():
    """Create PostgreSQL database connection with retry logic"""
    logger = logging.getLogger()
    
    host = os.getenv('DB_HOST', 'localhost')
    port = int(os.getenv('DB_PORT', 5432))
    database = os.getenv('DB_NAME', 'nfl_analytics')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASS', 'password')
    
    retry_delay = 1
    max_delay = 60
    
    while True:
        try:
            logger.info(f"Attempting to connect to PostgreSQL at {host}:{port}/{database}")
            
            connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            # Set autocommit to False for transaction control
            connection.autocommit = False
            
            logger.info("Successfully connected to PostgreSQL")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            logger.info(f"Retrying in {retry_delay} seconds...")
            
            time.sleep(retry_delay)
            
            # Exponential backoff with max cap
            retry_delay = min(retry_delay * 2, max_delay)

def insert_game_data(db_connection, game_data: Dict[str, Any]) -> bool:
    """Insert game data into the PostgreSQL database"""
    logger = logging.getLogger()
    
    try:
        cursor = db_connection.cursor()
        
        # SQL INSERT statement
        insert_query = """
        INSERT INTO games (
            game_id, game_date, away_team, away_score, home_team, home_score,
            away_spread_open, away_spread_open_odds, away_spread_close, away_spread_close_odds,
            away_moneyline_open, away_moneyline_close,
            home_spread_open, home_spread_open_odds, home_spread_close, home_spread_close_odds,
            home_moneyline_open, home_moneyline_close,
            over_open, over_open_odds, over_close, over_close_odds,
            under_open, under_open_odds, under_close, under_close_odds
        ) VALUES (
            %(game_id)s, %(game_date)s, %(away_team)s, %(away_score)s, %(home_team)s, %(home_score)s,
            %(away_spread_open)s, %(away_spread_open_odds)s, %(away_spread_close)s, %(away_spread_close_odds)s,
            %(away_moneyline_open)s, %(away_moneyline_close)s,
            %(home_spread_open)s, %(home_spread_open_odds)s, %(home_spread_close)s, %(home_spread_close_odds)s,
            %(home_moneyline_open)s, %(home_moneyline_close)s,
            %(over_open)s, %(over_open_odds)s, %(over_close)s, %(over_close_odds)s,
            %(under_open)s, %(under_open_odds)s, %(under_close)s, %(under_close_odds)s
        )
        ON CONFLICT (game_id) DO UPDATE SET
            game_date = EXCLUDED.game_date,
            away_team = EXCLUDED.away_team,
            away_score = EXCLUDED.away_score,
            home_team = EXCLUDED.home_team,
            home_score = EXCLUDED.home_score,
            away_spread_open = EXCLUDED.away_spread_open,
            away_spread_open_odds = EXCLUDED.away_spread_open_odds,
            away_spread_close = EXCLUDED.away_spread_close,
            away_spread_close_odds = EXCLUDED.away_spread_close_odds,
            away_moneyline_open = EXCLUDED.away_moneyline_open,
            away_moneyline_close = EXCLUDED.away_moneyline_close,
            home_spread_open = EXCLUDED.home_spread_open,
            home_spread_open_odds = EXCLUDED.home_spread_open_odds,
            home_spread_close = EXCLUDED.home_spread_close,
            home_spread_close_odds = EXCLUDED.home_spread_close_odds,
            home_moneyline_open = EXCLUDED.home_moneyline_open,
            home_moneyline_close = EXCLUDED.home_moneyline_close,
            over_open = EXCLUDED.over_open,
            over_open_odds = EXCLUDED.over_open_odds,
            over_close = EXCLUDED.over_close,
            over_close_odds = EXCLUDED.over_close_odds,
            under_open = EXCLUDED.under_open,
            under_open_odds = EXCLUDED.under_open_odds,
            under_close = EXCLUDED.under_close,
            under_close_odds = EXCLUDED.under_close_odds,
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Execute the insert
        cursor.execute(insert_query, game_data)
        
        # Commit the transaction
        db_connection.commit()
        
        logger.info(f"Successfully inserted/updated game {game_data['game_id']} ({game_data['away_team']} @ {game_data['home_team']})")
        
        cursor.close()
        return True
        
    except psycopg2.IntegrityError as e:
        logger.error(f"Database integrity error for game {game_data.get('game_id')}: {str(e)}")
        db_connection.rollback()
        return False
        
    except Exception as e:
        logger.error(f"Database error for game {game_data.get('game_id')}: {str(e)}")
        db_connection.rollback()
        return False

def process_message(ch, method, properties, body, db_connection):
    """Process received message from queue and store in database"""
    logger = logging.getLogger()
    
    try:
        # Parse JSON message
        message = json.loads(body.decode('utf-8'))
        
        logger.info(f"Processing message for Game ID: {message.get('game_id')}")
        
        # Insert into database
        success = insert_game_data(db_connection, message)
        
        if success:
            # Acknowledge message processing
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Successfully processed and stored game {message.get('game_id')}")
        else:
            # Reject message but don't requeue (to avoid infinite loops with bad data)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            logger.error(f"Failed to store game {message.get('game_id')}, message rejected")
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON message: {str(e)}")
        # Reject message and don't requeue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
    except Exception as e:
        logger.error(f"Unexpected error processing message: {str(e)}")
        # Reject message and don't requeue to prevent infinite loops
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    """Main service loop"""
    logger = setup_logging()
    logger.info("NFL Processor service starting up")
    
    # Get configuration
    queue_name = os.getenv('RABBITMQ_QUEUE', 'game_events')
    
    # Create connections
    rabbitmq_connection = create_rabbitmq_connection()
    rabbitmq_channel = rabbitmq_connection.channel()
    
    db_connection = create_database_connection()
    
    try:
        # Declare queue (idempotent operation)
        rabbitmq_channel.queue_declare(queue=queue_name, durable=True)
        
        # Set up consumer with database connection
        def message_handler(ch, method, properties, body):
            process_message(ch, method, properties, body, db_connection)
        
        rabbitmq_channel.basic_consume(
            queue=queue_name,
            on_message_callback=message_handler
        )
        
        # Set QoS to process one message at a time for better error handling
        rabbitmq_channel.basic_qos(prefetch_count=1)
        
        logger.info(f"Waiting for messages from queue '{queue_name}'. To exit press CTRL+C")
        
        # Start consuming messages
        rabbitmq_channel.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping service")
        rabbitmq_channel.stop_consuming()
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {str(e)}")
    finally:
        # Clean up connections
        try:
            rabbitmq_connection.close()
            logger.info("RabbitMQ connection closed")
        except:
            pass
        
        try:
            db_connection.close()
            logger.info("PostgreSQL connection closed")
        except:
            pass
            
        logger.info("NFL Processor service shutdown complete")

if __name__ == "__main__":
    main()