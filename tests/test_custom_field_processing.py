"""
Tests for enhanced custom field processing
"""

import pytest
from datetime import datetime
from amocrm_exporter.utils.custom_field_processor import CustomFieldProcessor, CustomFieldMetadataExtractor
from amocrm_exporter.utils.data_formatter import DataFormatter


class TestCustomFieldProcessor:
    """Test the CustomFieldProcessor class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.processor = CustomFieldProcessor()

    def test_process_text_field(self):
        """Test processing of text fields"""
        custom_fields_data = [
            {
                'field_id': '123',
                'field_name': 'Company Name',
                'field_type': 'text',
                'values': [{'value': 'Test Company'}]
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data)

        assert result.success
        assert 'cf_123_company_name' in result.processed_fields
        assert result.processed_fields['cf_123_company_name'] == 'Test Company'

    def test_process_numeric_field(self):
        """Test processing of numeric fields"""
        custom_fields_data = [
            {
                'field_id': '456',
                'field_name': 'Revenue',
                'field_type': 'numeric',
                'values': [{'value': '1000.50'}]
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data)

        assert result.success
        assert 'cf_456_revenue' in result.processed_fields
        assert result.processed_fields['cf_456_revenue'] == 1000.5

    def test_process_multiselect_field(self):
        """Test processing of multiselect fields"""
        custom_fields_data = [
            {
                'field_id': '789',
                'field_name': 'Tags',
                'field_type': 'multiselect',
                'values': [
                    {'value': 'Tag1'},
                    {'value': 'Tag2'},
                    {'value': 'Tag3'}
                ]
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data, preserve_field_types=False)

        assert result.success
        assert 'cf_789_tags' in result.processed_fields
        assert result.processed_fields['cf_789_tags'] == 'Tag1, Tag2, Tag3'

    def test_process_date_field(self):
        """Test processing of date fields"""
        custom_fields_data = [
            {
                'field_id': '101',
                'field_name': 'Start Date',
                'field_type': 'date',
                'values': [{'value': 1640995200}]  # 2022-01-01 timestamp
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data)

        assert result.success
        assert 'cf_101_start_date' in result.processed_fields
        # Should be formatted as Google Sheets date formula
        assert '=DATE(' in str(result.processed_fields['cf_101_start_date'])

    def test_process_checkbox_field(self):
        """Test processing of checkbox fields"""
        custom_fields_data = [
            {
                'field_id': '202',
                'field_name': 'Is Active',
                'field_type': 'checkbox',
                'values': [{'value': True}]
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data, preserve_field_types=False)

        assert result.success
        assert 'cf_202_is_active' in result.processed_fields
        assert result.processed_fields['cf_202_is_active'] == 'Yes'

    def test_process_phone_field(self):
        """Test processing of phone fields"""
        custom_fields_data = [
            {
                'field_id': '303',
                'field_name': 'Mobile',
                'field_type': 'phone',
                'values': [{'value': '+1234567890'}]
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data)

        assert result.success
        assert 'cf_303_mobile' in result.processed_fields
        # Phone numbers should be prefixed with quote to preserve formatting
        assert result.processed_fields['cf_303_mobile'] == "'+1234567890"

    def test_process_complex_address_field(self):
        """Test processing of complex address fields"""
        custom_fields_data = [
            {
                'field_id': '404',
                'field_name': 'Address',
                'field_type': 'streetaddress',
                'values': [{
                    'value': {
                        'street': '123 Main St',
                        'city': 'New York',
                        'state': 'NY',
                        'zip': '10001',
                        'country': 'USA'
                    }
                }]
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data)

        assert result.success
        assert 'cf_404_address' in result.processed_fields
        address_value = result.processed_fields['cf_404_address']
        assert '123 Main St' in address_value
        assert 'New York' in address_value
        assert 'NY' in address_value

    def test_graceful_error_handling(self):
        """Test graceful error handling for malformed data"""
        # Malformed custom fields data
        custom_fields_data = [
            {
                'field_id': '505',
                'field_name': 'Bad Field',
                'field_type': 'unknown_type',
                'values': [{'value': None}]
            }
        ]

        result = self.processor.process_custom_fields(custom_fields_data, graceful_degradation=True)

        # Should still succeed with graceful degradation
        assert result.success or len(result.errors) == 0  # Warnings are OK
        assert 'cf_505_bad_field' in result.processed_fields

    def test_empty_custom_fields(self):
        """Test handling of empty custom fields"""
        result = self.processor.process_custom_fields([])

        assert result.success
        assert len(result.processed_fields) == 0
        assert len(result.errors) == 0

    def test_json_string_input(self):
        """Test processing custom fields from JSON string"""
        import json

        custom_fields_data = [
            {
                'field_id': '606',
                'field_name': 'Test Field',
                'field_type': 'text',
                'values': [{'value': 'Test Value'}]
            }
        ]

        json_string = json.dumps(custom_fields_data)
        result = self.processor.process_custom_fields(json_string)

        assert result.success
        assert 'cf_606_test_field' in result.processed_fields
        assert result.processed_fields['cf_606_test_field'] == 'Test Value'


class TestCustomFieldMetadataExtractor:
    """Test the CustomFieldMetadataExtractor class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.extractor = CustomFieldMetadataExtractor()

    def test_extract_field_metadata(self):
        """Test extraction of field metadata"""
        data = [
            {
                'id': '1',
                'custom_fields_values': [
                    {
                        'field_id': '123',
                        'field_name': 'Company Name',
                        'field_type': 'text',
                        'values': [{'value': 'Test Company 1'}]
                    },
                    {
                        'field_id': '456',
                        'field_name': 'Revenue',
                        'field_type': 'numeric',
                        'values': [{'value': 1000}]
                    }
                ]
            },
            {
                'id': '2',
                'custom_fields_values': [
                    {
                        'field_id': '123',
                        'field_name': 'Company Name',
                        'field_type': 'text',
                        'values': [{'value': 'Test Company 2'}]
                    }
                ]
            }
        ]

        metadata = self.extractor.extract_field_metadata(data)

        assert '123' in metadata
        assert '456' in metadata

        # Check field 123 metadata
        field_123 = metadata['123']
        assert field_123['field_name'] == 'Company Name'
        assert field_123['field_type'] == 'text'
        assert len(field_123['value_examples']) == 2
        assert 'Test Company 1' in field_123['value_examples']
        assert 'Test Company 2' in field_123['value_examples']

    def test_complexity_score_calculation(self):
        """Test complexity score calculation"""
        # Simple text field
        simple_metadata = {
            'field_type': 'text',
            'value_types': ['str'],
            'has_complex_values': False,
            'is_multivalue': False
        }

        simple_score = self.extractor.get_field_complexity_score(simple_metadata)
        assert simple_score <= 2

        # Complex field
        complex_metadata = {
            'field_type': 'items',
            'value_types': ['dict', 'str'],
            'has_complex_values': True,
            'is_multivalue': True
        }

        complex_score = self.extractor.get_field_complexity_score(complex_metadata)
        assert complex_score >= 8


class TestDataFormatterIntegration:
    """Test integration between custom field processor and data formatter"""

    def setup_method(self):
        """Set up test fixtures"""
        self.processor = CustomFieldProcessor()
        self.formatter = DataFormatter()

    def test_phone_number_formatting(self):
        """Test that phone numbers are properly formatted"""
        phone_value = "+1-234-567-8900"
        formatted = self.formatter.format_value(phone_value, 'text', 'phone')

        # Should be prefixed with quote to preserve formatting
        assert formatted == "'+1-234-567-8900"

    def test_special_character_escaping(self):
        """Test that special characters are properly escaped"""
        special_value = "=SUM(A1:A10)"
        formatted = self.formatter.format_value(special_value, 'text', 'formula')

        # Should be prefixed with quote to prevent formula interpretation
        assert formatted == "'=SUM(A1:A10)"

    def test_date_formatting(self):
        """Test that dates are formatted as Google Sheets formulas"""
        timestamp = 1640995200  # 2022-01-01
        formatted = self.formatter.format_value(timestamp, 'date', 'date_field')

        # Should be a Google Sheets DATE formula
        assert formatted.startswith('=DATE(')
        assert '2022' in formatted


if __name__ == '__main__':
    pytest.main([__file__])