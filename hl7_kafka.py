from kafka import KafkaConsumer, KafkaProducer
from typing import Callable, Optional
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HL7KafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        input_topic: str,
        output_topic: str,
        group_id: str = "hl7-parser-group",
        max_workers: int = 10,
        batch_size: int = 100
    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        self.consumer = None
        self.producer = None
    
    def start(self, parser, error_handler: Optional[Callable] = None):
        self.running = True
        
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: m.decode('utf-8'),
            max_poll_records=self.batch_size
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Started consuming from {self.input_topic}")
        
        try:
            for messages in self._batch_messages():
                if not self.running:
                    break
                
                futures = []
                for message in messages:
                    future = self.executor.submit(
                        self._process_message,
                        message,
                        parser,
                        error_handler
                    )
                    futures.append(future)
                
                for future in futures:
                    future.result()
                
                self.consumer.commit()
                
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            raise
        finally:
            self.shutdown()
    
    def _batch_messages(self):
        batch = []
        for message in self.consumer:
            batch.append(message)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        
        if batch:
            yield batch
    
    def _process_message(self, message, parser, error_handler):
        try:
            raw_hl7 = message.value
            
            appointments = parser.parse_file(raw_hl7)
            
            for appointment in appointments:
                output_data = {
                    'source_offset': message.offset,
                    'source_partition': message.partition,
                    'appointment': appointment.to_dict()
                }
                
                self.producer.send(
                    self.output_topic,
                    value=output_data
                )
                
                logger.info(f"Processed appointment: {appointment.appointment_id}")
            
            self.producer.flush()
            
        except Exception as e:
            logger.error(f"Error processing message at offset {message.offset}: {e}")
            
            if error_handler:
                error_handler(message, e)
            else:
                error_data = {
                    'source_offset': message.offset,
                    'source_partition': message.partition,
                    'error': str(e),
                    'raw_message': message.value[:500]
                }
                
                self.producer.send(
                    f"{self.output_topic}-errors",
                    value=error_data
                )
                self.producer.flush()
    
    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def shutdown(self):
        logger.info("Shutting down consumer...")
        self.running = False
        
        if self.executor:
            self.executor.shutdown(wait=True)
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        if self.consumer:
            self.consumer.close()
        
        logger.info("Consumer shut down complete")


class HL7FileProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8'),
            acks='all'
        )
    
    def send_file(self, filepath: str):
        logger.info(f"Sending file {filepath} to Kafka topic {self.topic}")
        
        with open(filepath, 'r') as f:
            content = f.read()
        
        self.producer.send(self.topic, value=content)
        self.producer.flush()
        
        logger.info(f"File {filepath} sent successfully")
    
    def send_files(self, filepaths: list):
        for filepath in filepaths:
            self.send_file(filepath)
    
    def close(self):
        self.producer.close()