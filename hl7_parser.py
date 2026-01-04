from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import re


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


class HL7Segment:
    def __init__(self, raw_segment: str):
        self.raw = raw_segment.strip()
        self.fields = self._parse_fields()
        self.segment_type = self.fields[0] if self.fields else ""
    
    def _parse_fields(self) -> List[str]:
        if not self.raw:
            return []
        return self.raw.split('|')
    
    def get_field(self, index: int, default: str = "") -> str:
        if index < len(self.fields):
            return self.fields[index].strip()
        return default
    
    def get_component(self, field_index: int, component_index: int, default: str = "") -> str:
        field = self.get_field(field_index)
        if not field:
            return default
        components = field.split('^')
        if component_index < len(components):
            return components[component_index].strip()
        return default


class HL7Message:
    def __init__(self, raw_message: str):
        self.raw = raw_message
        self.segments = self._parse_segments()
        self.segment_map = self._build_segment_map()
    
    def _parse_segments(self) -> List[HL7Segment]:
        lines = self.raw.split('\r')
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
        (r'^\d{14}$', '%Y%m%d%H%M%S'),
        (r'^\d{12}$', '%Y%m%d%H%M'),
        (r'^\d{8}$', '%Y%m%d'),
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
        
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith('MSH'):
                if current_message:
                    messages.append('\r'.join(current_message))
                current_message = [line]
            else:
                if current_message:
                    current_message.append(line)
        
        if current_message:
            messages.append('\r'.join(current_message))
        
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
        msh = message.get_segment('MSH')
        if not msh:
            raise MissingRequiredSegmentError("MSH segment is required")
        
        message_type = msh.get_field(8)
        if not message_type.startswith('SIU^S12'):
            raise InvalidMessageTypeError(f"Expected SIU^S12, got {message_type}")
        
        if not message.has_segment('SCH'):
            raise MissingRequiredSegmentError("SCH segment is required for appointments")
    
    def _extract_appointment_id(self, message: HL7Message) -> str:
        sch = message.get_segment('SCH')
        if not sch:
            raise MissingRequiredSegmentError("SCH segment required for appointment ID")
        
        appointment_id = sch.get_field(1)
        if not appointment_id:
            raise HL7ParsingError("Appointment ID not found in SCH segment")
        
        return appointment_id
    
    def _extract_appointment_datetime(self, message: HL7Message) -> Optional[str]:
        sch = message.get_segment('SCH')
        if not sch:
            return None
        
        timestamp_field = sch.get_field(12)
        if not timestamp_field:
            return None
        
        components = timestamp_field.split('^')
        timestamp = components[3] if len(components) > 3 else components[0]
        
        return self._normalize_timestamp(timestamp)
    
    def _extract_patient(self, message: HL7Message) -> Optional[Patient]:
        pid = message.get_segment('PID')
        if not pid:
            return None
        
        patient_id = pid.get_field(3)
        if not patient_id:
            return None
        
        name_field = pid.get_field(5)
        name_parts = name_field.split('^') if name_field else []
        last_name = name_parts[0] if len(name_parts) > 0 else None
        first_name = name_parts[1] if len(name_parts) > 1 else None
        
        dob = pid.get_field(7)
        dob_formatted = self._format_date(dob) if dob else None
        
        gender = pid.get_field(8)
        
        return Patient(
            id=patient_id,
            first_name=first_name,
            last_name=last_name,
            dob=dob_formatted,
            gender=gender if gender else None
        )
    
    def _extract_provider(self, message: HL7Message) -> Optional[Provider]:
        sch = message.get_segment('SCH')
        pv1 = message.get_segment('PV1')
        
        provider_field = None
        if sch:
            provider_field = sch.get_field(16)
        
        if not provider_field and pv1:
            provider_field = pv1.get_field(7)
        
        if not provider_field:
            return None
        
        parts = provider_field.split('^')
        provider_id = parts[0] if len(parts) > 0 else None
        
        if not provider_id:
            return None
        
        name_parts = []
        if len(parts) > 1:
            name_parts.append(parts[1])
        if len(parts) > 2:
            name_parts.append(parts[2])
        
        provider_name = ' '.join(name_parts) if name_parts else None
        if provider_name:
            provider_name = f"Dr. {provider_name}"
        
        return Provider(id=provider_id, name=provider_name)
    
    def _extract_location(self, message: HL7Message) -> Optional[str]:
        pv1 = message.get_segment('PV1')
        if not pv1:
            return None
        
        location_field = pv1.get_field(3)
        if not location_field:
            return None
        
        parts = location_field.split('^')
        location_parts = [p for p in parts if p.strip()]
        
        return ' '.join(location_parts) if location_parts else None
    
    def _extract_reason(self, message: HL7Message) -> Optional[str]:
        sch = message.get_segment('SCH')
        if not sch:
            return None
        
        reason = sch.get_field(7)
        return reason if reason else None
    
    def _normalize_timestamp(self, timestamp: str) -> Optional[str]:
        if not timestamp:
            return None
        
        timestamp = timestamp.strip()
        
        for pattern, date_format in self.DATE_PATTERNS:
            if re.match(pattern, timestamp):
                try:
                    dt = datetime.strptime(timestamp, date_format)
                    return dt.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
                except ValueError:
                    continue
        
        return None
    
    def _format_date(self, date_str: str) -> Optional[str]:
        if not date_str or len(date_str) < 8:
            return None
        
        try:
            year = date_str[0:4]
            month = date_str[4:6]
            day = date_str[6:8]
            return f"{year}-{month}-{day}"
        except (ValueError, IndexError):
            return None