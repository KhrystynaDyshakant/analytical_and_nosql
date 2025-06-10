import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List
import pyodbc
import daft
from daft.io import IOConfig, AzureConfig
from azure.storage.blob import BlobServiceClient

STORAGE_CONNECTION = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=windfarm6storage;AccountKey=X7YVa9h/iiKRdw0sYlgaUkb8RFi3U+FRnKR/86DkYxiT8WB4KVPOeBxvGdp0yHDYRMAVVa1FpyIF+AStPVbVPQ==;BlobEndpoint=https://windfarm6storage.blob.core.windows.net/;FileEndpoint=https://windfarm6storage.file.core.windows.net/;QueueEndpoint=https://windfarm6storage.queue.core.windows.net/;TableEndpoint=https://windfarm6storage.table.core.windows.net/"

SQL_CONNECTION = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=windfarm6-sql-northeu.database.windows.net;DATABASE=WindFarmDB;UID=sqladmin;PWD=WindFarm123!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

STORAGE_ACCOUNT_NAME = "windfarm6storage"
STORAGE_ACCOUNT_KEY = "X7YVa9h/iiKRdw0sYlgaUkb8RFi3U+FRnKR/86DkYxiT8WB4KVPOeBxvGdp0yHDYRMAVVa1FpyIF+AStPVbVPQ=="
CONTAINER_NAME = "telemetry-data"
DELTA_TABLE_PATH = "delta-lake-turbine-telemetry"

class FixedTelemetryDeltaProcessor:
    """Процесор з правильним Daft API"""
    
    def __init__(self):
        
        # Azure Storage
        self.blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION)
        self.ensure_container_exists()
        
        # Daft IO Configuration
        self.io_config = IOConfig(
            azure=AzureConfig(
                storage_account=STORAGE_ACCOUNT_NAME,
                access_key=STORAGE_ACCOUNT_KEY
            )
        )
        
        print(" Processor ініціалізовано")
    
    def ensure_container_exists(self):
        """Створення контейнера"""
        try:
            self.blob_service.create_container(CONTAINER_NAME)
            print(f" Контейнер {CONTAINER_NAME} створено")
        except Exception:
            print(f" Контейнер {CONTAINER_NAME} вже існує")
    
    def get_sql_connection(self):
        """SQL підключення"""
        try:
            return pyodbc.connect(SQL_CONNECTION)
        except Exception as e:
            print(f" SQL помилка: {e}")
            return None
    
    def load_turbine_metadata(self, turbine_id: str) -> Dict[str, Any]:
        """Завантаження метаданих турбіни"""
        conn = self.get_sql_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            query = """
            SELECT turbine_id, location_name, latitude, longitude,
                   installation_date, manufacturer, model, nominal_power_kw, status
            FROM Turbines WHERE turbine_id = ?
            """
            
            cursor.execute(query, turbine_id)
            row = cursor.fetchone()
            
            if row:
                metadata = {
                    'turbine_id': row[0],
                    'turbine_name': row[1],
                    'location_lat': float(row[2]) if row[2] else None,
                    'location_lng': float(row[3]) if row[3] else None,
                    'installation_date': row[4].isoformat() if row[4] else None,
                    'manufacturer': row[5],
                    'model': row[6],
                    'nominal_power_kw': int(row[7]) if row[7] else None,
                    'status': row[8]
                }
                print(f" Метадані завантажено для {turbine_id}: {metadata['turbine_name']}")
                return metadata
            
            return {}
            
        except Exception as e:
            print(f" Помилка метаданих {turbine_id}: {e}")
            return {}
        finally:
            conn.close()
    
    def enrich_telemetry_data(self, telemetry_data: Dict[str, Any]) -> Dict[str, Any]:
        """Збагачення телеметрії"""
        turbine_id = telemetry_data.get('turbine_id')
        print(f" Збагачення для {turbine_id}")
        
        # Метадані з SQL
        turbine_metadata = self.load_turbine_metadata(turbine_id)
        
        # Розрахункові метрики
        calculated_metrics = self._calculate_metrics(telemetry_data, turbine_metadata)
        
        enriched_data = {
            **telemetry_data,
            'turbine_metadata': turbine_metadata,
            'calculated_metrics': calculated_metrics,
            'enrichment_info': {
                'processing_timestamp': datetime.now(timezone.utc).isoformat(),
                'enrichment_version': '2.1_Fixed_Daft',
                'source': 'Azure SQL Database WindFarmDB',
                'processed_by': 'Fixed Daft Delta Processor',
                'sql_metadata_loaded': len(turbine_metadata) > 0
            }
        }
        
        print(f" Збагачення завершено для {turbine_id}")
        return enriched_data
    
    def _calculate_metrics(self, telemetry_data: Dict[str, Any], turbine_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Розрахунок метрик"""
        metrics = {}
        
        try:
            if 'output_power' in telemetry_data and 'nominal_power_kw' in turbine_metadata:
                output_power = float(telemetry_data['output_power'])
                nominal_power = float(turbine_metadata['nominal_power_kw'])
                metrics['efficiency_percent'] = round((output_power / nominal_power) * 100, 2) if nominal_power > 0 else 0

            if 'output_power' in telemetry_data:
                power = float(telemetry_data['output_power'])
                if power > 100:
                    metrics['operational_status'] = 'generating'
                elif power > 0:
                    metrics['operational_status'] = 'low_generation'
                else:
                    metrics['operational_status'] = 'stopped'
            
            if all(field in telemetry_data for field in ['voltage', 'current', 'power_factor']):
                voltage = float(telemetry_data['voltage'])
                current = float(telemetry_data['current'])
                power_factor = float(telemetry_data['power_factor'])
                calculated_power = (voltage * current * power_factor * 1.732) / 1000
                metrics['calculated_power_kw'] = round(calculated_power, 2)
        
        except (ValueError, TypeError, ZeroDivisionError) as e:
            print(f" Помилка розрахунку: {e}")
        
        return metrics
    
    def save_to_delta_lake_fixed(self, enriched_data: Dict[str, Any]):
        """Збереження в Delta Lake через Daft"""
        try:
            print(" Збереження в Delta Lake ")
            
            # Підготовка даних
            flat_data = self._prepare_flat_data_fixed(enriched_data)
            
            # Створення DataFrame
            df = daft.from_pydict({
                key: [value] for key, value in flat_data.items()
            })
            
            # Шлях для Delta Lake
            current_time = datetime.now(timezone.utc)
            delta_path = f"az://{CONTAINER_NAME}/{DELTA_TABLE_PATH}"
            
            print(f" Збереження в: {delta_path}")
            
            # Збереження через Daft (правильний API)
            df.write_deltalake(
                delta_path,
                io_config=self.io_config,
                mode="append"
            )
            
            print(f" УСПІШНО збережено в Delta Lake!")
            
        except Exception as e:
            print(f" Помилка Delta Lake: {e}")
            self._save_json_backup(enriched_data)
    
    def _prepare_flat_data_fixed(self, enriched_data: Dict[str, Any]) -> Dict[str, Any]:
        """ВИПРАВЛЕНА підготовка даних"""
        current_time = datetime.now(timezone.utc)
        turbine_id = enriched_data.get('turbine_id', 'unknown')
        
        flat_data = {
            'record_id': f"{turbine_id}_{current_time.strftime('%Y%m%d_%H%M%S')}",
            'turbine_id': turbine_id,
            'timestamp': enriched_data.get('timestamp', ''),
            'processing_timestamp': current_time.isoformat(),
            
            'output_power': float(enriched_data.get('output_power', 0)),
            'rotor_rpm': float(enriched_data.get('rotor_rpm', 0)),
            'max_power_limit': float(enriched_data.get('max_power_limit', 0)),
            'voltage': float(enriched_data.get('voltage', 0)),
            'current': float(enriched_data.get('current', 0)),
            'power_factor': float(enriched_data.get('power_factor', 0)),

            'partition_year': current_time.year,
            'partition_month': current_time.month,
            'partition_day': current_time.day,
        }
        
        turbine_meta = enriched_data.get('turbine_metadata', {})
        flat_data.update({
            'turbine_name': str(turbine_meta.get('turbine_name', '')),
            'location_lat': float(turbine_meta.get('location_lat', 0)) if turbine_meta.get('location_lat') else 0.0,
            'location_lng': float(turbine_meta.get('location_lng', 0)) if turbine_meta.get('location_lng') else 0.0,
            'manufacturer': str(turbine_meta.get('manufacturer', '')),
            'model': str(turbine_meta.get('model', '')),
            'nominal_power_kw': int(turbine_meta.get('nominal_power_kw', 0)) if turbine_meta.get('nominal_power_kw') else 0,
            'installation_date': str(turbine_meta.get('installation_date', '')),
            'turbine_status': str(turbine_meta.get('status', '')),
        })
        
        calc_metrics = enriched_data.get('calculated_metrics', {})
        flat_data.update({
            'efficiency_percent': float(calc_metrics.get('efficiency_percent', 0)),
            'operational_status': str(calc_metrics.get('operational_status', '')),
            'calculated_power_kw': float(calc_metrics.get('calculated_power_kw', 0)),
        })
    
        enrichment_info = enriched_data.get('enrichment_info', {})
        flat_data.update({
            'data_quality_score': 1.0, 
            'enrichment_version': str(enrichment_info.get('enrichment_version', '')),
            'sql_metadata_loaded': bool(enrichment_info.get('sql_metadata_loaded', False)),
        })
        
        return flat_data
    
    def _save_json_backup(self, enriched_data: Dict[str, Any]):
        """JSON backup"""
        try:
            current_time = datetime.now(timezone.utc)
            turbine_id = enriched_data.get('turbine_id', 'unknown')
            
            blob_name = f"enriched-telemetry-backup/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/turbine={turbine_id}/enriched_{current_time.strftime('%H%M%S')}.json"
            
            blob_client = self.blob_service.get_blob_client(
                container=CONTAINER_NAME,
                blob=blob_name
            )
            
            json_data = json.dumps(enriched_data, ensure_ascii=False, indent=2)
            blob_client.upload_blob(json_data, overwrite=True)
            
            print(f" JSON backup: {blob_name}")
            
        except Exception as e:
            print(f" JSON backup помилка: {e}")
    
    def process_telemetry_batch(self, telemetry_list: List[Dict[str, Any]]):
        """Обробка пакету"""
        print(f"\n Обробка пакету з {len(telemetry_list)} записів")
        
        for i, telemetry in enumerate(telemetry_list, 1):
            print(f"\n--- Запис {i}/{len(telemetry_list)} ---")
            
            # Збагачення
            enriched_data = self.enrich_telemetry_data(telemetry)
            
            # Збереження в Delta Lake (виправлена версія)
            self.save_to_delta_lake_fixed(enriched_data)
            
            time.sleep(1)
        
        print(f"\n Пакет оброблено!")

def main():
    
    processor = FixedTelemetryDeltaProcessor()
    
    # Тестові дані
    test_telemetry = [
        {
            "turbine_id": "TURBINE_001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "output_power": 1500.0,
            "rotor_rpm": 15.2,
            "max_power_limit": 9500.0,
            "voltage": 690.0,
            "current": 2500.0,
            "power_factor": 0.92
        },
        {
            "turbine_id": "TURBINE_002",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "output_power": 2100.0,
            "rotor_rpm": 18.7,
            "max_power_limit": 14000.0,
            "voltage": 695.0,
            "current": 3200.0,
            "power_factor": 0.89
        }
    ]
    
    processor.process_telemetry_batch(test_telemetry)
    
    print(" ДЕМОНСТРАЦІЯ ЗАВЕРШЕНА!")

if __name__ == "__main__":
    main()