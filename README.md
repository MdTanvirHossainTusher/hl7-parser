# HL7 SIU^S12 Appointment Parser

HL7 parser with Kafka streaming support for high-throughput healthcare data processing.

## Architecture Overview

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌──────────────┐
│  HL7 Files  │─────>│ File Producer│─────>│ Kafka Topic │─────>│   Consumer   │
│  (.hl7)     │      │    (Input)   │      │  (hl7-raw)  │      │  (Parser)    │
└─────────────┘      └──────────────┘      └─────────────┘      └──────────────┘
                                                                         │
                                                                         v
                                                              ┌──────────────────┐
                                                              │  Kafka Topic     │
                                                              │  (appointments)  │
                                                              └──────────────────┘
```

### Design Decisions

**1. Streaming Architecture**
- Kafka-based ingestion handles massive concurrent HL7 file uploads
- Horizontal scaling via consumer groups enables processing millions of messages
- Backpressure handling prevents memory exhaustion

**2. Separation of Concerns**
- **hl7_parser.py**: Core parsing logic (segments, messages, domain models)
- **hl7_kafka.py**: Streaming infrastructure (producers, consumers)
- **hl7_cli.py**: Command-line interface and file operations

**3. Error Handling Strategy**
- Invalid messages sent to dead-letter topic (`{output-topic}-errors`)
- Graceful degradation: missing optional fields don't fail parsing
- Structured exceptions for different failure modes

**4. Performance Optimizations**
- Thread pool executor for parallel message processing
- Batch commits reduce Kafka overhead
- Configurable batch sizes and worker threads

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Parse Single File

```bash
python hl7_cli.py parse input.hl7
python hl7_cli.py parse input.hl7 -o output.json
```

### Parse Directory
```bash
python hl7_cli.py parse-dir ./hl7_files/
python hl7_cli.py parse-dir ./hl7_files/ -o ./output/
```

### Kafka Streaming

**Send files to Kafka:**
```bash
python hl7_cli.py kafka-produce file1.hl7 file2.hl7 \
  --bootstrap localhost:9092 \
  --topic hl7-raw
```

**Start consumer (parse from Kafka):**
```bash
python hl7_cli.py kafka-consume \
  --bootstrap localhost:9092 \
  --input-topic hl7-raw \
  --output-topic appointments \
  --workers 20 \
  --batch-size 200
```

## Sample Input and Output

### Input (Single file contains `multiple message`)
```bash
MSH|^~\&|EPIC|HOSPITAL|||202505051430||SIU^S12|MSG12346|P|2.3|
SCH|234567||||||Cardiology Follow-up|||45|m|^^45^20250515143000|||||||||D12345^Johnson^Emily|||||BOOKED
PID|1||P23456||Smith^Sarah^Anne||19920315|F|||456 Oak Ave^^Boston^MA^02101
PV1|1|O|Cardiology Unit^Room 105||||D12345^Johnson^

MSH|^~\&|MEDITECH|CLINIC|||202505101000||SIU^S12|MSG12347|P|2.3|


MSH|^~\&|CERNER|MEDICAL_CENTER|||202505121545||SIU^S12|MSG12348|P|2.3|
SCH|456789||||||Orthopedic Surgery Consultation|||90|m|^^90^20250525154500|||||||||D45678^Martinez^Carlos|||||PENDING
PID|1||P45678||Davis^Jennifer^Lynn||19780622|F|||321 Elm Rd^^Austin^TX^78701
PV1|1|O|Orthopedics^Room 301||||D45678^Martinez^Carlos
PV1|1|O|Orthopedics^Room 301||||D45678^Martinez^Carlos
PV1|1|O|Orthopedics^Room 301||||D45678^Martinez^Carlos

MSH|^~\&|DENTRIX|DENTAL_CLINIC|||202505081100||SIU^S12|MSG12349|P|2.3|
SCH|567890||||||Routine Dental Cleaning|||60|m|^^60^20250518110000|||||||||D23456^Lee^Susan|||||BOOKED
PV1|1|O|Dental Suite^Chair 3||||D23456^Lee^Susan

MSH|^~\&|ALLSCRIPTS|DERMATOLOGY|||202505141630||SIU^S12|MSG12350|P|2.3|
SCH|678901||||||Annual Skin Cancer Screening|||30|m|^^30^20250528163000|||||||||D89012^Patel^Anjali|||||WAITLISTED
PID|1||P67890||Anderson^Robert^Lee||19650425|M|||987 Cedar Ln^^Miami^FL^33101
XYZ|1|O|Dermatology^Exam Room 2||||D89012^Patel^Anjali
PV1|1|O|Behavioral Health^Room 410||||D34567^Thompson^David
```

### Output 

```bash
[
  {
    "appointment_id": "234567",
    "appointment_datetime": "2025-05-15T14:30:00Z",
    "patient": {
      "id": "P23456",
      "first_name": "Sarah",
      "last_name": "Smith",
      "dob": "1992-03-15",
      "gender": "F"
    },
    "provider": {
      "id": "D12345",
      "name": "Dr. Johnson "
    },
    "location": "Cardiology Unit Room 105",
    "reason": "Cardiology Follow-up"
  },
  {
    "appointment_id": "456789",
    "appointment_datetime": "2025-05-25T15:45:00Z",
    "patient": {
      "id": "P45678",
      "first_name": "Jennifer",
      "last_name": "Davis",
      "dob": "1978-06-22",
      "gender": "F"
    },
    "provider": {
      "id": "D45678",
      "name": "Dr. Martinez Carlos"
    },
    "location": "Orthopedics Room 301",
    "reason": "Orthopedic Surgery Consultation"
  },
  {
    "appointment_id": "567890",
    "appointment_datetime": "2025-05-18T11:00:00Z",
    "patient": null,
    "provider": {
      "id": "D23456",
      "name": "Dr. Lee Susan"
    },
    "location": "Dental Suite Chair 3",
    "reason": "Routine Dental Cleaning"
  },
  {
    "appointment_id": "678901",
    "appointment_datetime": "2025-05-28T16:30:00Z",
    "patient": {
      "id": "P67890",
      "first_name": "Robert",
      "last_name": "Anderson",
      "dob": "1965-04-25",
      "gender": "M"
    },
    "provider": {
      "id": "D34567",
      "name": "Dr. Thompson David"
    },
    "location": "Behavioral Health Room 410",
    "reason": "Annual Skin Cancer Screening"
  }
]
```

## Docker Deployment

### Build Image
```bash
docker build -t hl7-parser .
```

### Run with Docker Compose (Full Stack)
```bash
docker-compose up -d
```

This starts:
- Zookeeper
- Kafka broker
- HL7 parser consumer

## Scaling for High Throughput

### Horizontal Scaling
```bash
# Run multiple consumer instances
docker-compose up --scale hl7-parser=5
```

### Partition Strategy
- Increase Kafka topic partitions for parallelism
- Each consumer group member processes separate partitions
- Linear scaling up to partition count

### Tuning Parameters
```python
# Consumer configuration
max_workers=50          # Thread pool size
batch_size=500          # Messages per batch
max_poll_records=1000   # Kafka poll size
```

## Testing

```bash
python -m unittest hl7_unittest.py -v
```

### Test Coverage
- Valid message parsing
- Missing segment handling
- Timestamp normalization
- Multiple messages per file
- Malformed input handling
- Empty field processing

## HL7 Message Structure

### Required Segments
- **MSH**: Message header (validation, message type)
- **SCH**: Scheduling information (appointment ID, datetime, reason)
- **PID**: Patient demographics (optional but recommended)
- **PV1**: Visit information (provider, location)

### Field Mapping

**SCH Segment:**
- Field 1: Appointment ID
- Field 7: Reason for visit
- Field 11: Appointment datetime (component 4)
- Field 16: Provider information

**PID Segment:**
- Field 3: Patient ID
- Field 5: Patient name (components: last^first)
- Field 7: Date of birth (YYYYMMDD)
- Field 8: Gender

**PV1 Segment:**
- Field 3: Location (components: facility^room)
- Field 7: Attending provider

## Assumptions

### Assumptions
1. Messages follow HL7 v2.x conventions (pipe-delimited, carriage return separators)
2. SIU^S12 is the only supported message type
3. Timestamps are in format YYYYMMDDHHMMSS or YYYYMMDDHHMM

## Error Handling

### Parse Errors
- **InvalidMessageTypeError**: Non-SIU^S12 messages rejected
- **MissingRequiredSegmentError**: MSH or SCH missing
- **HL7ParsingError**: Generic parsing failures

### Kafka Errors
- Failed messages sent to `{output-topic}-errors`
- Includes error message, offset, and truncated raw data