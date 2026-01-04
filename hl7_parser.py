from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import re
import os
from dotenv import load_dotenv

load_dotenv()


class HL7ParsingError(Exception):
    pass


class InvalidMessageTypeError(HL7ParsingError):
    pass


class MissingRequiredSegmentError(HL7ParsingError):
    pass


@dataclass
class Patient:
    id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    dob: Optional[str] = None
    gender: Optional[str] = None


@dataclass
class Provider:
    id: str
    name: Optional[str] = None


@dataclass
class Appointment:
    appointment_id: str
    appointment_datetime: Optional[str] = None
    patient: Optional[Patient] = None
    provider: Optional[Provider] = None
    location: Optional[str] = None
    reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        if self.patient:
            result['patient'] = asdict(self.patient)
        if self.provider:
            result['provider'] = asdict(self.provider)
        return result


class Config:
    """Configuration class to load and store environment variables"""
    
    # Segment Configuration
    DEFAULT_SEGMENT_TYPE = os.getenv('DEFAULT_SEGMENT_TYPE', '')
    FIELD_SEPARATOR = os.getenv('FIELD_SEPARATOR', '|')
    COMPONENT_SEPARATOR = os.getenv('COMPONENT_SEPARATOR', '^')
    SEGMENT_SEPARATOR = '\r'  # Hardcoded as escape sequences don't work in .env
    LINE_SEPARATOR = '\n'  # Hardcoded as escape sequences don't work in .env
    
    # Message Type Configuration
    EXPECTED_MESSAGE_TYPE = os.getenv('EXPECTED_MESSAGE_TYPE', 'SIU^S12')
    
    # Required Segments
    REQUIRED_SEGMENT_MSH = os.getenv('REQUIRED_SEGMENT_MSH', 'MSH')
    REQUIRED_SEGMENT_SCH = os.getenv('REQUIRED_SEGMENT_SCH', 'SCH')
    SEGMENT_PID = os.getenv('SEGMENT_PID', 'PID')
    SEGMENT_PV1 = os.getenv('SEGMENT_PV1', 'PV1')
    
    # Field Indices
    MSH_MESSAGE_TYPE_INDEX = int(os.getenv('MSH_MESSAGE_TYPE_INDEX', '8'))
    SCH_APPOINTMENT_ID_INDEX = int(os.getenv('SCH_APPOINTMENT_ID_INDEX', '1'))
    SCH_DATETIME_INDEX = int(os.getenv('SCH_DATETIME_INDEX', '12'))
    SCH_PROVIDER_INDEX = int(os.getenv('SCH_PROVIDER_INDEX', '16'))
    SCH_REASON_INDEX = int(os.getenv('SCH_REASON_INDEX', '7'))
    PID_PATIENT_ID_INDEX = int(os.getenv('PID_PATIENT_ID_INDEX', '3'))
    PID_NAME_INDEX = int(os.getenv('PID_NAME_INDEX', '5'))
    PID_DOB_INDEX = int(os.getenv('PID_DOB_INDEX', '7'))
    PID_GENDER_INDEX = int(os.getenv('PID_GENDER_INDEX', '8'))
    PV1_PROVIDER_INDEX = int(os.getenv('PV1_PROVIDER_INDEX', '7'))
    PV1_LOCATION_INDEX = int(os.getenv('PV1_LOCATION_INDEX', '3'))
    
    # DateTime Component Index
    DATETIME_COMPONENT_INDEX = int(os.getenv('DATETIME_COMPONENT_INDEX', '3'))
    
    # Name Component Indices
    NAME_LAST_INDEX = int(os.getenv('NAME_LAST_INDEX', '0'))
    NAME_FIRST_INDEX = int(os.getenv('NAME_FIRST_INDEX', '1'))
    
    # Provider Component Indices
    PROVIDER_ID_INDEX = int(os.getenv('PROVIDER_ID_INDEX', '0'))
    PROVIDER_LAST_NAME_INDEX = int(os.getenv('PROVIDER_LAST_NAME_INDEX', '1'))
    PROVIDER_FIRST_NAME_INDEX = int(os.getenv('PROVIDER_FIRST_NAME_INDEX', '2'))
    
    # Date Format Patterns
    DATE_PATTERN_14 = os.getenv('DATE_PATTERN_14', r'^\d{14}$')
    DATE_FORMAT_14 = os.getenv('DATE_FORMAT_14', '%Y%m%d%H%M%S')
    DATE_PATTERN_12 = os.getenv('DATE_PATTERN_12', r'^\d{12}$')
    DATE_FORMAT_12 = os.getenv('DATE_FORMAT_12', '%Y%m%d%H%M')
    DATE_PATTERN_8 = os.getenv('DATE_PATTERN_8', r'^\d{8}$')
    DATE_FORMAT_8 = os.getenv('DATE_FORMAT_8', '%Y%m%d')
    
    # Output Format
    OUTPUT_DATETIME_FORMAT = os.getenv('OUTPUT_DATETIME_FORMAT', '%Y-%m-%dT%H:%M:%SZ')
    OUTPUT_DATE_FORMAT = os.getenv('OUTPUT_DATE_FORMAT', '%Y-%m-%d')
    
    # Provider Name Prefix
    PROVIDER_NAME_PREFIX = os.getenv('PROVIDER_NAME_PREFIX', 'Dr.')
    
    # Date String Indices
    DATE_YEAR_START = int(os.getenv('DATE_YEAR_START', '0'))
    DATE_YEAR_END = int(os.getenv('DATE_YEAR_END', '4'))
    DATE_MONTH_START = int(os.getenv('DATE_MONTH_START', '4'))
    DATE_MONTH_END = int(os.getenv('DATE_MONTH_END', '6'))
    DATE_DAY_START = int(os.getenv('DATE_DAY_START', '6'))
    DATE_DAY_END = int(os.getenv('DATE_DAY_END', '8'))
    MIN_DATE_LENGTH = int(os.getenv('MIN_DATE_LENGTH', '8'))


class HL7Segment:
    def __init__(self, raw_segment: str):
        self.raw = raw_segment.strip()
        self.fields = self._parse_fields()
        self.segment_type = self.fields[0] if self.fields else Config.DEFAULT_SEGMENT_TYPE
    
    def _parse_fields(self) -> List[str]:
        if not self.raw:
            return []
        return self.raw.split(Config.FIELD_SEPARATOR)
    
    def get_field(self, index: int, default: str = "") -> str:
        if index < len(self.fields):
            return self.fields[index].strip()
        return default
    
    def get_component(self, field_index: int, component_index: int, default: str = "") -> str:
        field = self.get_field(field_index)
        if not field:
            return default
        components = field.split(Config.COMPONENT_SEPARATOR)
        if component_index < len(components):
            return components[component_index].strip()
        return default


class HL7Message:
    def __init__(self, raw_message: str):
        self.raw = raw_message
        self.segments = self._parse_segments()
        self.segment_map = self._build_segment_map()
    
    def _parse_segments(self) -> List[HL7Segment]:
        lines = self.raw.split(Config.SEGMENT_SEPARATOR)
        lines = [line for line in lines if line.strip()]
        return [HL7Segment(line) for line in lines]
    
    def _build_segment_map(self) -> Dict[str, List[HL7Segment]]:
        segment_map = {}
        for segment in self.segments:
            seg_type = segment.segment_type
            if seg_type not in segment_map:
                segment_map[seg_type] = []
            segment_map[seg_type].append(segment)
        return segment_map
    
    def get_segment(self, segment_type: str) -> Optional[HL7Segment]:
        segments = self.segment_map.get(segment_type, [])
        return segments[0] if segments else None
    
    def has_segment(self, segment_type: str) -> bool:
        return segment_type in self.segment_map


class HL7Parser:
    DATE_PATTERNS = [
        (Config.DATE_PATTERN_14, Config.DATE_FORMAT_14),
        (Config.DATE_PATTERN_12, Config.DATE_FORMAT_12),
        (Config.DATE_PATTERN_8, Config.DATE_FORMAT_8),
    ]
    
    def parse_file(self, content: str) -> List[Appointment]:
        messages = self._split_messages(content)
        appointments = []
        
        for msg_content in messages:
            try:
                appointment = self.parse_message(msg_content)
                appointments.append(appointment)
            except HL7ParsingError as e:
                print(f"Warning: Failed to parse message: {e}")
                continue
        
        return appointments
    
    def _split_messages(self, content: str) -> List[str]:
        messages = []
        current_message = []
        
        lines = content.split(Config.LINE_SEPARATOR)
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith(Config.REQUIRED_SEGMENT_MSH):
                if current_message:
                    messages.append(Config.SEGMENT_SEPARATOR.join(current_message))
                current_message = [line]
            else:
                if current_message:
                    current_message.append(line)
        
        if current_message:
            messages.append(Config.SEGMENT_SEPARATOR.join(current_message))
        
        return messages
    
    def parse_message(self, raw_message: str) -> Appointment:
        message = HL7Message(raw_message)
        
        self._validate_message(message)
        
        appointment_id = self._extract_appointment_id(message)
        appointment_datetime = self._extract_appointment_datetime(message)
        patient = self._extract_patient(message)
        provider = self._extract_provider(message)
        location = self._extract_location(message)
        reason = self._extract_reason(message)
        
        return Appointment(
            appointment_id=appointment_id,
            appointment_datetime=appointment_datetime,
            patient=patient,
            provider=provider,
            location=location,
            reason=reason
        )
    
    def _validate_message(self, message: HL7Message):
        msh = message.get_segment(Config.REQUIRED_SEGMENT_MSH)
        if not msh:
            raise MissingRequiredSegmentError(f"{Config.REQUIRED_SEGMENT_MSH} segment is required")
        
        message_type = msh.get_field(Config.MSH_MESSAGE_TYPE_INDEX)
        if not message_type.startswith(Config.EXPECTED_MESSAGE_TYPE):
            raise InvalidMessageTypeError(f"Expected {Config.EXPECTED_MESSAGE_TYPE}, got {message_type}")
        
        if not message.has_segment(Config.REQUIRED_SEGMENT_SCH):
            raise MissingRequiredSegmentError(f"{Config.REQUIRED_SEGMENT_SCH} segment is required for appointments")
    
    def _extract_appointment_id(self, message: HL7Message) -> str:
        sch = message.get_segment(Config.REQUIRED_SEGMENT_SCH)
        if not sch:
            raise MissingRequiredSegmentError(f"{Config.REQUIRED_SEGMENT_SCH} segment required for appointment ID")
        
        appointment_id = sch.get_field(Config.SCH_APPOINTMENT_ID_INDEX)
        if not appointment_id:
            raise HL7ParsingError(f"Appointment ID not found in {Config.REQUIRED_SEGMENT_SCH} segment")
        
        return appointment_id
    
    def _extract_appointment_datetime(self, message: HL7Message) -> Optional[str]:
        sch = message.get_segment(Config.REQUIRED_SEGMENT_SCH)
        if not sch:
            return None
        
        timestamp_field = sch.get_field(Config.SCH_DATETIME_INDEX)
        if not timestamp_field:
            return None
        
        components = timestamp_field.split(Config.COMPONENT_SEPARATOR)
        timestamp = components[Config.DATETIME_COMPONENT_INDEX] if len(components) > Config.DATETIME_COMPONENT_INDEX else components[0]
        
        return self._normalize_timestamp(timestamp)
    
    def _extract_patient(self, message: HL7Message) -> Optional[Patient]:
        pid = message.get_segment(Config.SEGMENT_PID)
        if not pid:
            return None
        
        patient_id = pid.get_field(Config.PID_PATIENT_ID_INDEX)
        if not patient_id:
            return None
        
        name_field = pid.get_field(Config.PID_NAME_INDEX)
        name_parts = name_field.split(Config.COMPONENT_SEPARATOR) if name_field else []
        last_name = name_parts[Config.NAME_LAST_INDEX] if len(name_parts) > Config.NAME_LAST_INDEX else None
        first_name = name_parts[Config.NAME_FIRST_INDEX] if len(name_parts) > Config.NAME_FIRST_INDEX else None
        
        dob = pid.get_field(Config.PID_DOB_INDEX)
        dob_formatted = self._format_date(dob) if dob else None
        
        gender = pid.get_field(Config.PID_GENDER_INDEX)
        
        return Patient(
            id=patient_id,
            first_name=first_name,
            last_name=last_name,
            dob=dob_formatted,
            gender=gender if gender else None
        )
    
    def _extract_provider(self, message: HL7Message) -> Optional[Provider]:
        sch = message.get_segment(Config.REQUIRED_SEGMENT_SCH)
        pv1 = message.get_segment(Config.SEGMENT_PV1)
        
        provider_field = None
        if sch:
            provider_field = sch.get_field(Config.SCH_PROVIDER_INDEX)
        
        if not provider_field and pv1:
            provider_field = pv1.get_field(Config.PV1_PROVIDER_INDEX)
        
        if not provider_field:
            return None
        
        parts = provider_field.split(Config.COMPONENT_SEPARATOR)
        provider_id = parts[Config.PROVIDER_ID_INDEX] if len(parts) > Config.PROVIDER_ID_INDEX else None
        
        if not provider_id:
            return None
        
        name_parts = []
        if len(parts) > Config.PROVIDER_LAST_NAME_INDEX:
            name_parts.append(parts[Config.PROVIDER_LAST_NAME_INDEX])
        if len(parts) > Config.PROVIDER_FIRST_NAME_INDEX:
            name_parts.append(parts[Config.PROVIDER_FIRST_NAME_INDEX])
        
        provider_name = ' '.join(name_parts) if name_parts else None
        if provider_name:
            provider_name = f"{Config.PROVIDER_NAME_PREFIX} {provider_name}"
        
        return Provider(id=provider_id, name=provider_name)
    
    def _extract_location(self, message: HL7Message) -> Optional[str]:
        pv1 = message.get_segment(Config.SEGMENT_PV1)
        if not pv1:
            return None
        
        location_field = pv1.get_field(Config.PV1_LOCATION_INDEX)
        if not location_field:
            return None
        
        parts = location_field.split(Config.COMPONENT_SEPARATOR)
        location_parts = [p for p in parts if p.strip()]
        
        return ' '.join(location_parts) if location_parts else None
    
    def _extract_reason(self, message: HL7Message) -> Optional[str]:
        sch = message.get_segment(Config.REQUIRED_SEGMENT_SCH)
        if not sch:
            return None
        
        reason = sch.get_field(Config.SCH_REASON_INDEX)
        return reason if reason else None
    
    def _normalize_timestamp(self, timestamp: str) -> Optional[str]:
        if not timestamp:
            return None
        
        timestamp = timestamp.strip()
        
        for pattern, date_format in self.DATE_PATTERNS:
            if re.match(pattern, timestamp):
                try:
                    dt = datetime.strptime(timestamp, date_format)
                    return dt.strftime(Config.OUTPUT_DATETIME_FORMAT)
                except ValueError:
                    continue
        
        return None
    
    def _format_date(self, date_str: str) -> Optional[str]:
        if not date_str or len(date_str) < Config.MIN_DATE_LENGTH:
            return None
        
        try:
            year = date_str[Config.DATE_YEAR_START:Config.DATE_YEAR_END]
            month = date_str[Config.DATE_MONTH_START:Config.DATE_MONTH_END]
            day = date_str[Config.DATE_DAY_START:Config.DATE_DAY_END]
            return f"{year}-{month}-{day}"
        except (ValueError, IndexError):
            return None