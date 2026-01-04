import unittest
from hl7_parser import (
    HL7Parser, HL7Segment, HL7Message,
    HL7ParsingError, InvalidMessageTypeError, MissingRequiredSegmentError
)


class TestHL7Segment(unittest.TestCase):
    
    def test_parse_basic_segment(self):
        segment = HL7Segment("PID|1||P12345||Doe^John||19850210|M")
        self.assertEqual(segment.segment_type, "PID")
        self.assertEqual(segment.get_field(3), "P12345")
    
    def test_get_component(self):
        segment = HL7Segment("PID|1||P12345||Doe^John||19850210|M")
        self.assertEqual(segment.get_component(5, 0), "Doe")
        self.assertEqual(segment.get_component(5, 1), "John")
    
    def test_missing_field_returns_default(self):
        segment = HL7Segment("PID|1||P12345")
        self.assertEqual(segment.get_field(10, "default"), "default")
    
    def test_empty_field(self):
        segment = HL7Segment("PID|1|||")
        self.assertEqual(segment.get_field(3), "")


class TestHL7Message(unittest.TestCase):
    
    def test_parse_multiple_segments(self):
        raw = "MSH|^~\\&|EPIC||||\r\nPID|1||P12345\r\nPV1|1|O"
        message = HL7Message(raw)
        self.assertEqual(len(message.segments), 3)
        self.assertTrue(message.has_segment('MSH'))
        self.assertTrue(message.has_segment('PID'))
        self.assertTrue(message.has_segment('PV1'))
    
    def test_get_segment(self):
        raw = "MSH|^~\\&|EPIC||||\r\nPID|1||P12345"
        message = HL7Message(raw)
        pid = message.get_segment('PID')
        self.assertIsNotNone(pid)
        self.assertEqual(pid.segment_type, 'PID')
    
    def test_missing_segment(self):
        raw = "MSH|^~\\&|EPIC||||\r\nPID|1||P12345"
        message = HL7Message(raw)
        sch = message.get_segment('SCH')
        self.assertIsNone(sch)


class TestHL7Parser(unittest.TestCase):
    
    def setUp(self):
        self.parser = HL7Parser()
        self.valid_message = """MSH|^~\\&|EPIC|HOSPITAL|||20250502130000||SIU^S12|MSG12345|P|2.3|
                                SCH|123456||||||General Consultation|||60|m|^^60^20250502130000|||||||||D67890^Smith^John|||||BOOKED
                                PID|1||P12345||Doe^John||19850210|M|||123 Main St^^Springfield^IL^62704
                                PV1|1|O|Clinic A^Room 203||||D67890^Smith^John"""
    
    def test_parse_valid_message(self):
        appointments = self.parser.parse_file(self.valid_message)
        self.assertEqual(len(appointments), 1)
        
        apt = appointments[0]
        self.assertEqual(apt.appointment_id, "123456")
        self.assertEqual(apt.reason, "General Consultation")
        self.assertIsNotNone(apt.patient)
        self.assertEqual(apt.patient.id, "P12345")
        self.assertEqual(apt.patient.first_name, "John")
        self.assertEqual(apt.patient.last_name, "Doe")
    
    def test_invalid_message_type(self):
        invalid_msg = """MSH|^~\\&|EPIC||||20250502130000||ADT^A01|MSG123|P|2.3|
                        PID|1||P12345||Doe^John"""
        
        with self.assertRaises(InvalidMessageTypeError):
            self.parser.parse_message(invalid_msg)
    
    def test_missing_sch_segment(self):
        missing_sch = """MSH|^~\\&|EPIC||||20250502130000||SIU^S12|MSG123|P|2.3|
                        PID|1||P12345||Doe^John"""
        
        with self.assertRaises(MissingRequiredSegmentError):
            self.parser.parse_message(missing_sch)
    
    def test_missing_patient_gracefully(self):
        no_patient = """MSH|^~\\&|EPIC||||20250502130000||SIU^S12|MSG123|P|2.3|
                        SCH|123456||||||Consultation"""
        
        appointments = self.parser.parse_file(no_patient)
        self.assertEqual(len(appointments), 1)
        self.assertIsNone(appointments[0].patient)
    
    def test_normalize_timestamp(self):
        timestamp = "20250502130000"
        normalized = self.parser._normalize_timestamp(timestamp)
        self.assertEqual(normalized, "2025-05-02T13:00:00Z")
    
    def test_normalize_timestamp_short_format(self):
        timestamp = "202505021300"
        normalized = self.parser._normalize_timestamp(timestamp)
        self.assertEqual(normalized, "2025-05-02T13:00:00Z")
    
    def test_format_date(self):
        date = "19850210"
        formatted = self.parser._format_date(date)
        self.assertEqual(formatted, "1985-02-10")
    
    def test_multiple_messages_in_file(self):
        multiple = """MSH|^~\\&|EPIC||||20250502130000||SIU^S12|MSG1|P|2.3|
                    SCH|123456||||||Consultation1
                    PID|1||P12345||Doe^John
                    MSH|^~\\&|EPIC||||20250502140000||SIU^S12|MSG2|P|2.3|
                    SCH|789012||||||Consultation2
                    PID|1||P67890||Smith^Jane"""
        
        appointments = self.parser.parse_file(multiple)
        self.assertEqual(len(appointments), 2)
        self.assertEqual(appointments[0].appointment_id, "123456")
        self.assertEqual(appointments[1].appointment_id, "789012")
    
    def test_extract_provider_from_pv1(self):
        msg = """MSH|^~\\&|EPIC||||20250502130000||SIU^S12|MSG1|P|2.3|
                SCH|123456
                PV1|1|O|||||D67890^Smith^Jane"""
        
        appointments = self.parser.parse_file(msg)
        self.assertIsNotNone(appointments[0].provider)
        self.assertEqual(appointments[0].provider.id, "D67890")
    
    def test_extract_location(self):
        appointments = self.parser.parse_file(self.valid_message)
        self.assertEqual(appointments[0].location, "Clinic A Room 203")
    
    def test_empty_fields_handled(self):
        msg = """MSH|^~\\&|EPIC||||20250502130000||SIU^S12|MSG1|P|2.3|
                SCH|123456||||||
                PID|1||P12345||||"""
        
        appointments = self.parser.parse_file(msg)
        self.assertEqual(len(appointments), 1)
        self.assertIsNone(appointments[0].reason)
        self.assertIsNone(appointments[0].patient.dob)


class TestHL7ParserEdgeCases(unittest.TestCase):
    
    def setUp(self):
        self.parser = HL7Parser()
    
    def test_malformed_timestamp(self):
        timestamp = "invalid"
        normalized = self.parser._normalize_timestamp(timestamp)
        self.assertIsNone(normalized)
    
    def test_extra_segments_ignored(self):
        msg = """MSH|^~\\&|EPIC||||20250502130000||SIU^S12|MSG1|P|2.3|
                SCH|123456
                ZZZ|custom|segment
                PID|1||P12345||Doe^John"""
        
        appointments = self.parser.parse_file(msg)
        self.assertEqual(len(appointments), 1)
        self.assertEqual(appointments[0].patient.last_name, "Doe")
    
    def test_truncated_segment(self):
        segment = HL7Segment("PID|1||P12345")
        self.assertEqual(segment.get_field(10), "")
    
    def test_appointment_to_dict(self):
        msg = """MSH|^~\\&|EPIC||||20250502130000||SIU^S12|MSG1|P|2.3|
                SCH|123456||||||Consultation|||60|m|^^60^20250502130000
                PID|1||P12345||Doe^John||19850210|M"""
        
        appointments = self.parser.parse_file(msg)
        apt_dict = appointments[0].to_dict()
        
        self.assertIsInstance(apt_dict, dict)
        self.assertEqual(apt_dict['appointment_id'], '123456')
        self.assertIsInstance(apt_dict['patient'], dict)
        self.assertEqual(apt_dict['patient']['first_name'], 'John')


if __name__ == '__main__':
    unittest.main()