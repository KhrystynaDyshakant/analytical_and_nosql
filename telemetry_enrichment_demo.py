"""
Демонстрація збагачення телеметричних даних
"""

import json
import os
from datetime import datetime
from typing import Dict, Any

class TelemetryEnricher:
    """Збагачувач телеметричних даних (локальна версія Azure Function)"""
    
    def __init__(self):
        self.load_metadata_from_files()
    
    def load_metadata_from_files(self):
        """Завантаження метаданих з файлового кешу (з реальної Azure SQL DB)"""
        try:
            # Завантаження метаданих з ваших JSON файлів
            cache_dir = "metadata_cache"
            
            if not os.path.exists(cache_dir):
                print(f"Папка {cache_dir} не знайдена!")
                print("Запустіть спочатку: python load_metadata_sql_to_cache.py")
                self.turbines_metadata = {}
                self.sensors_metadata = {}
                self.turbine_sensors = {}
                return
            
            # Турбіни з реальної SQL бази
            with open(f"{cache_dir}/turbines_lookup.json", 'r', encoding='utf-8') as f:
                turbines_data = json.load(f)
                self.turbines_metadata = turbines_data.get('data', {})
            
            # Сенсори з реальної SQL бази
            with open(f"{cache_dir}/sensors_lookup.json", 'r', encoding='utf-8') as f:
                sensors_data = json.load(f)
                self.sensors_metadata = sensors_data.get('data', {})
            
            # Турбіни-сенсори з реальної SQL бази
            with open(f"{cache_dir}/turbine_sensors_lookup.json", 'r', encoding='utf-8') as f:
                turbine_sensors_data = json.load(f)
                self.turbine_sensors = turbine_sensors_data.get('data', {})
            
            print("метадані завантажені з Azure SQL Database (через файловий кеш)")
            print(f"   Турбіни з SQL: {len(self.turbines_metadata)}")
            print(f"   Сенсори з SQL: {len(self.sensors_metadata)}")
            print(f"   Зв'язки з SQL: {len(self.turbine_sensors)}")
            
            # Показуємо зразок реальних даних
            if self.turbines_metadata:
                sample_turbine = list(self.turbines_metadata.values())[0]
                print(f"   Приклад турбіни: {sample_turbine.get('turbine_name', 'N/A')}")
            
        except FileNotFoundError as e:
            print(f"Файл метаданих не знайдено: {e}")
            print("Запустіть спочатку завантаження метаданих:")
            print("   python load_metadata_sql_to_cache.py")
            self.turbines_metadata = {}
            self.sensors_metadata = {}
            self.turbine_sensors = {}
        except Exception as e:
            print(f"Помилка завантаження метаданих: {e}")
            self.turbines_metadata = {}
            self.sensors_metadata = {}
            self.turbine_sensors = {}
    
    def process_telemetry_message(self, telemetry_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Основна функція обробки телеметрії 
        """
        
        turbine_id = telemetry_data.get('turbine_id', 'UNKNOWN')
        
        print(f"\n Обробка телеметрії для турбіни: {turbine_id}")
        print(f"   Потужність: {telemetry_data.get('output_power', 0)} кВт")
        print(f"   RPM: {telemetry_data.get('rotor_rpm', 0)}")
        
        # Збагачення даних метаданими
        enriched_data = self.enrich_telemetry_data(telemetry_data)
        
        # Збереження у "Delta Lake" (імітація)
        self.save_to_delta_lake(enriched_data)
        
        print(f" Успішно оброблено і збагачено телеметрію для {turbine_id}")
        
        return enriched_data
    
    def enrich_telemetry_data(self, telemetry_data: Dict[str, Any]) -> Dict[str, Any]:
        """Збагачення телеметричних даних метаданими"""
        
        turbine_id = telemetry_data.get('turbine_id')
        
        # Отримання метаданих турбіни з файлового кешу
        turbine_metadata = self.turbines_metadata.get(turbine_id, {})
        
        # Отримання сенсорів турбіни
        turbine_sensors = self.turbine_sensors.get(turbine_id, [])
        
        # Створення збагаченого запису
        enriched_data = {
            # Оригінальні телеметричні дані
            **telemetry_data,
            
            # Збагачені метадані турбіни
            'turbine_metadata': {
                'turbine_name': turbine_metadata.get('turbine_name'),
                'location_lat': turbine_metadata.get('location_lat'),
                'location_lng': turbine_metadata.get('location_lng'),
                'manufacturer': turbine_metadata.get('manufacturer'),
                'model': turbine_metadata.get('model'),
                'nominal_power_kw': turbine_metadata.get('nominal_power_kw'),
                'installation_date': turbine_metadata.get('installation_date'),
                'status': turbine_metadata.get('status')
            },
            
            # Метадані сенсорів
            'sensor_metadata': self._get_sensor_metadata(telemetry_data, turbine_sensors),
            
            # Розрахункові метрики
            'calculated_metrics': self._calculate_derived_metrics(telemetry_data, turbine_metadata),
            
            # Інформація про збагачення
            'enrichment_info': {
                'processing_timestamp': datetime.utcnow().isoformat(),
                'data_quality_score': self._calculate_data_quality(telemetry_data),
                'enrichment_version': '1.0',
                'source': 'Azure SQL Database via File Cache',
                'processed_by': 'Local Telemetry Enricher (замість Azure Functions)'
            }
        }
        
        return enriched_data
    
    def _get_sensor_metadata(self, telemetry_data: Dict[str, Any], turbine_sensors: list) -> Dict[str, Any]:
        """Отримання метаданих сенсорів з Azure SQL DB"""
        sensor_metadata = {}
        
        # Використовуємо РЕАЛЬНІ дані з SQL бази
        for param_name in telemetry_data.keys():
            if param_name in ['turbine_id', 'timestamp']:
                continue
                
            # Знаходимо відповідний сенсор з реальної SQL бази
            for sensor in turbine_sensors:
                # Мапінг параметрів до реальних назв з SQL
                param_mapping = {
                    'output_power': 'output_power',
                    'rotor_rpm': 'rotor_rpm', 
                    'max_power_limit': 'max_power_limit',
                    'voltage': 'voltage',
                    'current': 'current',
                    'power_factor': 'power_factor'
                }
                
                if sensor.get('parameter_name') == param_mapping.get(param_name):
                    sensor_metadata[param_name] = {
                        'sensor_name': sensor.get('sensor_name'),
                        'unit_of_measurement': sensor.get('unit_of_measurement'),
                        'description': sensor.get('description'),
                        'sensor_type_id': sensor.get('sensor_type_id'),
                        'turbine_sensor_id': sensor.get('turbine_sensor_id')
                    }
                    break
                    
            # Якщо не знайшли у реальних даних, шукаємо в lookup сенсорів
            if param_name not in sensor_metadata:
                for sensor_id, sensor_data in self.sensors_metadata.items():
                    if sensor_data.get('parameter_name') == param_mapping.get(param_name):
                        sensor_metadata[param_name] = {
                            'sensor_name': sensor_data.get('sensor_name'),
                            'unit_of_measurement': sensor_data.get('unit_of_measurement'),
                            'description': sensor_data.get('description'),
                            'sensor_type_id': sensor_data.get('sensor_type_id'),
                            'source': 'SQL SensorTypes table'
                        }
                        break
        
        return sensor_metadata
    
    def _calculate_derived_metrics(self, telemetry_data: Dict[str, Any], turbine_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Розрахунок додаткових метрик"""
        derived_metrics = {}
        
        try:
            # Ефективність турбіни
            if 'output_power' in telemetry_data and 'nominal_power_kw' in turbine_metadata:
                output_power = float(telemetry_data['output_power'])
                nominal_power = float(turbine_metadata['nominal_power_kw'])
                derived_metrics['efficiency_percent'] = round((output_power / nominal_power) * 100, 2) if nominal_power > 0 else 0
            
            # Статус роботи
            if 'output_power' in telemetry_data:
                output_power = float(telemetry_data['output_power'])
                if output_power > 100:
                    derived_metrics['operational_status'] = 'generating'
                elif output_power > 0:
                    derived_metrics['operational_status'] = 'low_generation'
                else:
                    derived_metrics['operational_status'] = 'stopped'
            
            # Електрична потужність (P = V * I * cos(φ))
            if all(field in telemetry_data for field in ['voltage', 'current', 'power_factor']):
                voltage = float(telemetry_data['voltage'])
                current = float(telemetry_data['current'])
                power_factor = float(telemetry_data['power_factor'])
                calculated_power = (voltage * current * power_factor) / 1000  # кВт
                derived_metrics['calculated_power_kw'] = round(calculated_power, 2)
        
        except (ValueError, TypeError) as e:
            print(f" Помилка розрахунку метрик: {e}")
        
        return derived_metrics
    
    def _calculate_data_quality(self, telemetry_data: Dict[str, Any]) -> float:
        """Розрахунок якості даних (0.0 - 1.0)"""
        quality_score = 1.0
        
        # Перевірка наявності обов'язкових полів
        required_fields = ['turbine_id', 'timestamp', 'output_power']
        for field in required_fields:
            if field not in telemetry_data or telemetry_data[field] is None:
                quality_score -= 0.2
        
        return max(0.0, min(1.0, quality_score))
    
    def save_to_delta_lake(self, enriched_data: Dict[str, Any]):
        """Імітація збереження у Delta Lake"""
        
        # Створення структури Delta Lake
        current_time = datetime.utcnow()
        turbine_id = enriched_data.get('turbine_id', 'unknown')
        
        # Delta Lake партиціонування
        partition_path = f"year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/turbine_id={turbine_id}"
        
        # Створення директорії (імітація Azure Storage)
        delta_dir = f"delta_lake_output/{partition_path}"
        os.makedirs(delta_dir, exist_ok=True)
        
        # Збереження JSON файлу (імітація Delta Lake)
        filename = f"enriched_data_{current_time.strftime('%H%M%S_%f')}.json"
        filepath = os.path.join(delta_dir, filename)
        
        # Додаємо Delta Lake метадані
        enriched_data['_delta_metadata'] = {
            'partition_year': current_time.year,
            'partition_month': current_time.month,
            'partition_day': current_time.day,
            'partition_turbine': turbine_id,
            'file_format': 'json',
            'schema_version': '1.0'
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(enriched_data, f, ensure_ascii=False, indent=2)
        
        print(f" Збережено у Delta Lake: {filepath}")

def simulate_telemetry_processing():
    """Симуляція обробки телеметрії з РЕАЛЬНИМИ даними з Azure SQL DB"""
    
    print(" Демонстрація збагачення телеметричних даних")
    print(" Використовуємо РЕАЛЬНІ метадані з Azure SQL Database")
    print("=" * 70)
    
    # Ініціалізація збагачувача
    enricher = TelemetryEnricher()
    
    # Перевірка чи завантажились реальні дані
    if not enricher.turbines_metadata:
        print(" ПОМИЛКА: Реальні метадані не завантажились!")
        print(" Запустіть спочатку:")
        print("   python load_metadata_sql_to_cache.py")
        return
    
    # Показуємо реальні турбіни з SQL бази
    print(f"\n Доступні турбіни з Azure SQL DB:")
    for turbine_id, turbine_data in list(enricher.turbines_metadata.items())[:3]:
        print(f"   {turbine_id}: {turbine_data.get('turbine_name', 'N/A')} ({turbine_data.get('manufacturer', 'N/A')})")
    
    # Використовуємо РЕАЛЬНІ ID турбін з SQL бази
    real_turbine_ids = list(enricher.turbines_metadata.keys())
    
    # Тестові телеметричні дані для РЕАЛЬНИХ турбін
    test_telemetry = []
    for i, turbine_id in enumerate(real_turbine_ids[:2]):  # Беремо перші 2 реальні турбіни
        telemetry = {
            "turbine_id": turbine_id,  # РЕАЛЬНИЙ ID з SQL бази
            "timestamp": f"2025-05-31T15:30:{i*5:02d}Z",
            "output_power": 1500.0 + i * 600,
            "rotor_rpm": 15.2 - i * 0.4,
            "max_power_limit": 9500.0 + i * 4500,
            "voltage": 690.0 + i * 30,
            "current": 2500.0 + i * 700,
            "power_factor": 0.92 - i * 0.03
        }
        test_telemetry.append(telemetry)
    
    # Обробка кожного повідомлення телеметрії
    for i, telemetry in enumerate(test_telemetry, 1):
        print(f"\n Тестове повідомлення {i} (РЕАЛЬНА турбіна з SQL):")
        
        # Обробка телеметрії (імітує Azure Function)
        enriched_result = enricher.process_telemetry_message(telemetry)
        
        # Показуємо результат збагачення даними
        turbine_meta = enriched_result['turbine_metadata']
        calc_metrics = enriched_result['calculated_metrics']
        
        print("\n Результат збагачення РЕАЛЬНИМИ метаданими з Azure SQL DB:")
        print(f"   Турбіна: {turbine_meta.get('turbine_name', 'N/A')}")
        print(f"   Виробник: {turbine_meta.get('manufacturer', 'N/A')}")
        print(f"   Модель: {turbine_meta.get('model', 'N/A')}")
        print(f"   Координати: {turbine_meta.get('location_lat', 'N/A')}, {turbine_meta.get('location_lng', 'N/A')}")
        print(f"   Номінальна потужність: {turbine_meta.get('nominal_power_kw', 'N/A')} кВт")
        print(f"   Ефективність: {calc_metrics.get('efficiency_percent', 0)}%")
        print(f"   Статус: {calc_metrics.get('operational_status', 'unknown')}")
        print(f"   Якість даних: {enriched_result['enrichment_info']['data_quality_score']}")
        
        # Показуємо метадані сенсорів з SQL
        sensor_meta = enriched_result['sensor_metadata']
        if sensor_meta:
            print(f"   Сенсори з SQL DB: {len(sensor_meta)} шт.")
            for param, sensor_info in list(sensor_meta.items())[:2]:
                print(f"     - {param}: {sensor_info.get('sensor_name', 'N/A')} ({sensor_info.get('unit_of_measurement', 'N/A')})")
    
    print(f"\n Демонстрація завершена!")
    print(" Перевірте папку 'delta_lake_output' для збережених файлів")
    print(" Використані РЕАЛЬНІ метадані з Azure SQL Database WindFarmDB")
    
    # Показуємо статистику використаних даних
    print(f"\n Статистика використаних РЕАЛЬНИХ даних:")
    print(f"   Турбін з SQL: {len(enricher.turbines_metadata)}")
    print(f"   Сенсорів з SQL: {len(enricher.sensors_metadata)}")
    print(f"   Зв'язків турбін-сенсорів: {len(enricher.turbine_sensors)}")


if __name__ == "__main__":
    simulate_telemetry_processing()