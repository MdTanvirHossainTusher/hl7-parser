import sys
import json
import argparse
from pathlib import Path
from hl7_parser import HL7Parser

"""
CLI commands to parse appoinment JSON:

    python hl7_cli.py input.hl7
    python hl7_cli.py input.hl7 -o output.json
    python hl7_cli.py input.hl7 --pretty

"""


def main():
    parser = argparse.ArgumentParser(
        description='Simple HL7 SIU^S12 Appointment Parser',
        usage='%(prog)s input.hl7 [-o OUTPUT] [--pretty]'
    )
    
    parser.add_argument(
        'input_file',
        help='Path to HL7 input file'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output JSON file (if not specified, prints to stdout)'
    )
    
    parser.add_argument(
        '--pretty',
        action='store_true',
        help='Pretty print JSON output with indentation'
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(input_path, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        hl7_parser = HL7Parser()
        appointments = hl7_parser.parse_file(content)
        
        results = [apt.to_dict() for apt in appointments]
        
        indent = 2 if args.pretty else None
        json_output = json.dumps(results, indent=indent)
        
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                f.write(json_output)
            
            print(f"Successfully parsed {len(appointments)} appointment(s)")
            print(f"Output written to: {args.output}")
        else:
            print(json_output)
        
    except Exception as e:
        print(f"Error parsing HL7 file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()