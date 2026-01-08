import argparse
import json
import sys
from pathlib import Path
import glob
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HL7CLI:
    def __init__(self, parser):
        self.parser = parser
    
    def process_file(self, filepath: str, output_file: str = None):
        logger.info(f"Processing file: {filepath}")
        
        try:
            with open(filepath, 'r') as f:
                content = f.read()
            
            appointments = self.parser.parse_file(content)
            
            results = [apt.to_dict() for apt in appointments]
            
            if output_file:
                with open(output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                logger.info(f"Results written to {output_file}")
            else:
                print(json.dumps(results, indent=2))
            
            logger.info(f"Successfully processed {len(appointments)} appointments")
            return results
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            sys.exit(1)
    
    def process_directory(self, directory: str, pattern: str = "*.hl7", output_dir: str = None):
        logger.info(f"Processing directory: {directory}")
        
        dir_path = Path(directory)
        if not dir_path.exists():
            logger.error(f"Directory not found: {directory}")
            sys.exit(1)
        
        files = list(dir_path.glob(pattern))
        
        if not files:
            logger.warning(f"No files matching pattern '{pattern}' found in {directory}")
            return
        
        logger.info(f"Found {len(files)} files to process")
        
        all_results = []
        
        for filepath in files:
            try:
                logger.info(f"Processing {filepath.name}...")
                
                with open(filepath, 'r') as f:
                    content = f.read()
                
                appointments = self.parser.parse_file(content)
                results = [apt.to_dict() for apt in appointments]
                all_results.extend(results)
                
                if output_dir:
                    output_path = Path(output_dir)
                    output_path.mkdir(parents=True, exist_ok=True)
                    
                    output_file = output_path / f"{filepath.stem}.json"
                    with open(output_file, 'w') as f:
                        json.dump(results, f, indent=2)
                    
                    logger.info(f"Wrote {len(results)} appointments to {output_file}")
                
            except Exception as e:
                logger.error(f"Error processing {filepath.name}: {e}")
                continue
        
        logger.info(f"Total appointments processed: {len(all_results)}")
        
        if not output_dir:
            print(json.dumps(all_results, indent=2))
        
        return all_results


def main():
    from hl7_parser import HL7Parser
    from hl7_kafka import HL7KafkaConsumer, HL7FileProducer
    
    parser_obj = argparse.ArgumentParser(
        description='HL7 SIU^S12 Appointment Parser',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  Parse a single file:
    python hl7_cli.py parse input.hl7
  
  Parse with output:
    python hl7_cli.py parse input.hl7 -o output.json
  
  Parse directory:
    python hl7_cli.py parse-dir ./hl7_files/ -o ./output/
  
  Send to Kafka:
    python hl7_cli.py kafka-produce input.hl7 --bootstrap localhost:9092 --topic hl7-raw
    
  Send to Kafka as dir:
    python hl7_cli.py kafka-produce-dir ./hl7_files/ --bootstrap localhost:9092 --topic hl7-raw
  
  Start Kafka consumer:
    python hl7_cli.py kafka-consume --bootstrap localhost:9092 --input-topic hl7-raw --output-topic appointments
        '''
    )
    
    subparsers = parser_obj.add_subparsers(dest='command', help='Commands')
    
    parse_parser = subparsers.add_parser('parse', help='Parse a single HL7 file')
    parse_parser.add_argument('file', help='Path to HL7 file')
    parse_parser.add_argument('-o', '--output', help='Output JSON file')
    
    parse_dir_parser = subparsers.add_parser('parse-dir', help='Parse all HL7 files in a directory')
    parse_dir_parser.add_argument('directory', help='Directory containing HL7 files')
    parse_dir_parser.add_argument('-p', '--pattern', default='*.hl7', help='File pattern (default: *.hl7)')
    parse_dir_parser.add_argument('-o', '--output-dir', help='Output directory for JSON files')
    
    kafka_produce_parser = subparsers.add_parser('kafka-produce', help='Send HL7 files to Kafka')
    kafka_produce_parser.add_argument('files', nargs='+', help='HL7 files to send')
    kafka_produce_parser.add_argument('--bootstrap', required=True, help='Kafka bootstrap servers')
    kafka_produce_parser.add_argument('--topic', required=True, help='Kafka topic')

    kafka_produce_dir_parser = subparsers.add_parser('kafka-produce-dir',
                                                     help='Send all HL7 files from a directory to Kafka')
    kafka_produce_dir_parser.add_argument('directory', help='Directory containing HL7 files')
    kafka_produce_dir_parser.add_argument('-p', '--pattern', default='*.hl7', help='File pattern (default: *.hl7)')
    kafka_produce_dir_parser.add_argument('--bootstrap', required=True, help='Kafka bootstrap servers')
    kafka_produce_dir_parser.add_argument('--topic', required=True, help='Kafka topic')

    kafka_consume_parser = subparsers.add_parser('kafka-consume', help='Consume and parse HL7 from Kafka')
    kafka_consume_parser.add_argument('--bootstrap', required=True, help='Kafka bootstrap servers')
    kafka_consume_parser.add_argument('--input-topic', required=True, help='Input Kafka topic')
    kafka_consume_parser.add_argument('--output-topic', required=True, help='Output Kafka topic')
    kafka_consume_parser.add_argument('--group-id', default='hl7-parser-group', help='Consumer group ID')
    kafka_consume_parser.add_argument('--workers', type=int, default=10, help='Number of worker threads')
    kafka_consume_parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    
    args = parser_obj.parse_args()
    
    if not args.command:
        parser_obj.print_help()
        sys.exit(1)
    
    hl7_parser = HL7Parser()
    cli = HL7CLI(hl7_parser)
    
    if args.command == 'parse':
        cli.process_file(args.file, args.output)
    
    elif args.command == 'parse-dir':
        cli.process_directory(args.directory, args.pattern, args.output_dir)
    
    elif args.command == 'kafka-produce':
        producer = HL7FileProducer(args.bootstrap, args.topic)
        producer.send_files(args.files)
        producer.close()
        logger.info("Files sent to Kafka successfully")

    elif args.command == 'kafka-produce-dir':
        dir_path = Path(args.directory)
        if not dir_path.exists():
            logger.error(f"Directory not found: {args.directory}")
            sys.exit(1)

        files = [str(f) for f in dir_path.glob(args.pattern)]

        if not files:
            logger.warning(f"No files matching pattern '{args.pattern}' found")
            sys.exit(0)

        producer = HL7FileProducer(args.bootstrap, args.topic)
        producer.send_files(files)
        producer.close()
        logger.info(f"Sent {len(files)} files to Kafka successfully")
    
    elif args.command == 'kafka-consume':
        consumer = HL7KafkaConsumer(
            bootstrap_servers=args.bootstrap,
            input_topic=args.input_topic,
            output_topic=args.output_topic,
            group_id=args.group_id,
            max_workers=args.workers,
            batch_size=args.batch_size
        )
        consumer.start(hl7_parser)


if __name__ == '__main__':
    main()