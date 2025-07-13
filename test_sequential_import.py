#!/usr/bin/env python3
"""
Тестовый скрипт для проверки последовательного импорта с обогащением данных
"""

import sys
import os

# Устанавливаем переменные окружения для локального тестирования
os.environ['MONGODB_URI'] = 'mongodb://localhost:27017'
os.environ['MONGODB_DB'] = 'amocrm_exporter'

# Добавляем путь к модулям
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from amocrm_exporter.exporters.parallel_exporter import ParallelExporter
from amocrm_exporter.core.logger import log_event

def test_sequential_import():
    """Тестирует новый последовательный импорт с обогащением"""

    print("🚀 Starting sequential import test...")
    print("=" * 60)

    try:
        # Initialize exporter
        print("📦 Initializing ParallelExporter...")
        exporter = ParallelExporter()
        print("✅ ParallelExporter initialized successfully")

        # Test the new sequential export order
        print("\n🔄 Testing new sequential export order:")
        print("   users → custom_fields → deals → events → contacts → companies → pipelines")

        # Check if we can initialize DataEnricher properly
        if hasattr(exporter, 'data_enricher'):
            print("✅ DataEnricher initialized successfully")

            # Check if enrichment method exists
            if hasattr(exporter, '_enrich_entities_for_export'):
                print("✅ Data enrichment method available")

                # Test enrichment on sample data
                print("\n🧪 Testing data enrichment...")
                sample_entities = [
                    {
                        'id': 12345,
                        'name': 'Test Deal',
                        'responsible_user_id': 1,
                        'custom_fields_values': [
                            {
                                'field_id': 123,
                                'field_name': 'Budget',
                                'values': [{'value': '100000'}]
                            }
                        ]
                    },
                    {
                        'id': 12346,
                        'name': 'Another Deal',
                        'responsible_user_id': 2,
                        'pipeline_id': 1,
                        'status_id': 123
                    }
                ]

                print(f"   📥 Input: {len(sample_entities)} sample entities")
                enriched = exporter._enrich_entities_for_export(sample_entities, 'deals')
                print(f"   📤 Output: {len(enriched)} enriched entities")

                # Show what fields were added
                if enriched:
                    original_keys = set(sample_entities[0].keys())
                    enriched_keys = set(enriched[0].keys())
                    new_keys = enriched_keys - original_keys

                    if new_keys:
                        print(f"   ✨ New enriched fields added: {len(new_keys)}")
                        for key in sorted(new_keys):
                            print(f"      • {key}")
                    else:
                        print("   ⚠️  No new fields added (enrichment may need configuration)")

                print("✅ Sample enrichment test completed")

            else:
                print("❌ Data enrichment method missing")
                return False
        else:
            print("❌ DataEnricher not initialized")
            return False

        print("\n📋 Available export methods:")
        export_methods = [
            'export_users', 'export_custom_fields', 'export_deals',
            'export_events', 'export_contacts', 'export_companies', 'export_pipelines'
        ]

        for method in export_methods:
            if hasattr(exporter, method):
                print(f"   ✅ {method}")
            else:
                print(f"   ❌ {method}")

        # Check if sequential export_all exists
        if hasattr(exporter, 'export_all'):
            print("\n🔄 Sequential export_all method available")
            print("   Ready for testing real import with proper order and enrichment!")
        else:
            print("\n❌ export_all method missing")
            return False

        print("\n" + "=" * 60)
        print("🎉 Sequential import system ready for testing!")
        print("💡 To test with real data, run:")
        print("   python -m amocrm_exporter.cli --entity all")
        print("   or use the web interface at http://localhost:8000")

        return True

    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_sequential_import()
    sys.exit(0 if success else 1)