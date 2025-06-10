import json
import time
from datetime import datetime, timezone, timedelta
import pyodbc
import pandas as pd
import daft
from daft.io import IOConfig, AzureConfig
from azure.storage.blob import BlobServiceClient
import random
import warnings

warnings.filterwarnings('ignore')

STORAGE_CONNECTION = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=windfarm6storage;AccountKey=X7YVa9h/iiKRdw0sYlgaUkb8RFi3U+FRnKR/86DkYxiT8WB4KVPOeBxvGdp0yHDYRMAVVa1FpyIF+AStPVbVPQ==;BlobEndpoint=https://windfarm6storage.blob.core.windows.net/"

SQL_CONNECTION = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=windfarm6-sql-northeu.database.windows.net;DATABASE=WindFarmDB;UID=sqladmin;PWD=WindFarm123!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

STORAGE_ACCOUNT_NAME = "windfarm6storage"
STORAGE_ACCOUNT_KEY = "X7YVa9h/iiKRdw0sYlgaUkb8RFi3U+FRnKR/86DkYxiT8WB4KVPOeBxvGdp0yHDYRMAVVa1FpyIF+AStPVbVPQ=="
CONTAINER_NAME = "telemetry-data"
NEW_DELTA_TABLE_PATH = "delta-lake-turbine-telemetry-varied"

class TelemetryGenerator:
    
    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION)
        self.ensure_container_exists()
        
        self.io_config = IOConfig(
            azure=AzureConfig(
                storage_account=STORAGE_ACCOUNT_NAME,
                access_key=STORAGE_ACCOUNT_KEY
            )
        )
    
    def ensure_container_exists(self):
        try:
            self.blob_service.create_container(CONTAINER_NAME)
        except:
            pass
    
    def get_sql_connection(self):
        try:
            return pyodbc.connect(SQL_CONNECTION)
        except Exception:
            return None
    
    def get_all_turbine_ids(self):
        conn = self.get_sql_connection()
        if not conn:
            return []
        
        try:
            query = "SELECT DISTINCT turbine_id FROM dbo.Turbines ORDER BY turbine_id"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_sql(query, conn)
            return df['turbine_id'].tolist()
        except:
            return []
        finally:
            conn.close()
    
    def generate_realistic_timestamps(self, base_time, num_records, turbine_index):
        timestamps = []
        
        turbine_offset_hours = turbine_index * 0.5
        adjusted_base = base_time + timedelta(hours=turbine_offset_hours)
        
        for i in range(num_records):
            minutes_offset = i * 10 + random.randint(-2, 2)
            seconds_offset = random.randint(0, 59)
            
            timestamp = adjusted_base + timedelta(minutes=minutes_offset, seconds=seconds_offset)
            timestamps.append(timestamp)
        
        timestamps.sort()
        return timestamps
    
    def add_realistic_variation(self, base_value, variation_percent=10):
        if base_value == 0:
            return 0
        
        variation = base_value * (variation_percent / 100) * (random.random() - 0.5) * 2
        return round(base_value + variation, 2)
    
    def get_enriched_data_for_turbine(self, turbine_id, limit, turbine_index):
        conn = self.get_sql_connection()
        if not conn:
            return []
        
        try:
            query = f"""
            SELECT TOP ({limit})
                turbine_id, timestamp, output_power, rotor_rpm, max_power_limit,
                voltage, current_amperage, power_factor, data_quality_score, sensor_status,
                location_name, latitude, longitude, manufacturer, model, nominal_power_kw, 
                turbine_status, installation_date,
                wind_speed_ms, wind_direction_degrees, temperature_celsius,
                humidity_percent, air_pressure_hpa, visibility_km, precipitation_mm,
                maintenance_status, last_maintenance_date, next_maintenance_date,
                efficiency_rating, operating_hours, maintenance_notes, technician_name,
                efficiency_percent, operational_status, calculated_power_kw, load_factor,
                partition_year, partition_month, partition_day,
                enrichment_version, enrichment_source, processed_by
            FROM dbo.EnrichedTelemetryView 
            WHERE turbine_id = ?
            ORDER BY timestamp DESC
            """
            
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_sql(query, conn, params=[turbine_id])
            
            if len(df) == 0:
                return []
            
            base_time = datetime.now(timezone.utc) - timedelta(hours=3)
            realistic_timestamps = self.generate_realistic_timestamps(base_time, len(df), turbine_index)
            
            enriched_list = []
            for idx, (_, row) in enumerate(df.iterrows()):
                realistic_timestamp = realistic_timestamps[idx]
                
                output_power = self.add_realistic_variation(float(row['output_power']) if pd.notna(row['output_power']) else 0.0)
                rotor_rpm = self.add_realistic_variation(float(row['rotor_rpm']) if pd.notna(row['rotor_rpm']) else 0.0)
                voltage = self.add_realistic_variation(float(row['voltage']) if pd.notna(row['voltage']) else 0.0)
                current = self.add_realistic_variation(float(row['current_amperage']) if pd.notna(row['current_amperage']) else 0.0)
                wind_speed = self.add_realistic_variation(float(row['wind_speed_ms']) if pd.notna(row['wind_speed_ms']) else 0.0)
                temperature = self.add_realistic_variation(float(row['temperature_celsius']) if pd.notna(row['temperature_celsius']) else 0.0, 5)
                
                record = {
                    "turbine_id": turbine_id,
                    "timestamp": realistic_timestamp.isoformat(),
                    "output_power": output_power,
                    "rotor_rpm": rotor_rpm,
                    "max_power_limit": float(row['max_power_limit']) if pd.notna(row['max_power_limit']) else 0.0,
                    "voltage": voltage,
                    "current": current,
                    "power_factor": self.add_realistic_variation(float(row['power_factor']) if pd.notna(row['power_factor']) else 0.0, 3),
                    
                    "turbine_metadata": {
                        "turbine_name": str(row['location_name']) if pd.notna(row['location_name']) else '',
                        "location_lat": float(row['latitude']) if pd.notna(row['latitude']) else 0.0,
                        "location_lng": float(row['longitude']) if pd.notna(row['longitude']) else 0.0,
                        "manufacturer": str(row['manufacturer']) if pd.notna(row['manufacturer']) else '',
                        "model": str(row['model']) if pd.notna(row['model']) else '',
                        "nominal_power_kw": int(row['nominal_power_kw']) if pd.notna(row['nominal_power_kw']) else 0,
                        "installation_date": row['installation_date'].isoformat() if pd.notna(row['installation_date']) else '',
                        "status": str(row['turbine_status']) if pd.notna(row['turbine_status']) else 'unknown'
                    },
                    
                    "environmental_data": {
                        "wind_speed_ms": wind_speed,
                        "wind_direction_degrees": int(row['wind_direction_degrees']) if pd.notna(row['wind_direction_degrees']) else 0,
                        "temperature_celsius": temperature,
                        "humidity_percent": self.add_realistic_variation(float(row['humidity_percent']) if pd.notna(row['humidity_percent']) else 0.0),
                        "air_pressure_hpa": self.add_realistic_variation(float(row['air_pressure_hpa']) if pd.notna(row['air_pressure_hpa']) else 0.0, 2),
                        "visibility_km": float(row['visibility_km']) if pd.notna(row['visibility_km']) else 0.0,
                        "precipitation_mm": float(row['precipitation_mm']) if pd.notna(row['precipitation_mm']) else 0.0
                    },
                    
                    "maintenance_data": {
                        "maintenance_status": str(row['maintenance_status']) if pd.notna(row['maintenance_status']) else '',
                        "last_maintenance_date": row['last_maintenance_date'].isoformat() if pd.notna(row['last_maintenance_date']) else '',
                        "next_maintenance_date": row['next_maintenance_date'].isoformat() if pd.notna(row['next_maintenance_date']) else '',
                        "efficiency_rating": float(row['efficiency_rating']) if pd.notna(row['efficiency_rating']) else 0.0,
                        "operating_hours": float(row['operating_hours']) if pd.notna(row['operating_hours']) else 0.0,
                        "maintenance_notes": str(row['maintenance_notes']) if pd.notna(row['maintenance_notes']) else "Regular maintenance completed",
                        "technician_name": str(row['technician_name']) if pd.notna(row['technician_name']) else f"Tech_{turbine_id[-1]}"
                    },
                    
                    "calculated_metrics": {
                        "efficiency_percent": self.add_realistic_variation(float(row['efficiency_percent']) if pd.notna(row['efficiency_percent']) else 0.0, 5),
                        "operational_status": str(row['operational_status']) if pd.notna(row['operational_status']) else '',
                        "calculated_power_kw": self.add_realistic_variation(float(row['calculated_power_kw']) if pd.notna(row['calculated_power_kw']) else 0.0)
                    },
                    
                    "enrichment_info": {
                        "processing_timestamp": realistic_timestamp.isoformat(),
                        "data_quality_score": float(row['data_quality_score']) if pd.notna(row['data_quality_score']) else 1.0,
                        "enrichment_version": "varied_timestamps_v1",
                        "source": "Azure SQL Database + Time Variation",
                        "processed_by": "Telemetry Generator"
                    }
                }
                
                enriched_list.append(record)
            
            return enriched_list
            
        except Exception:
            return []
        finally:
            conn.close()
    
    def save_to_delta_table(self, enriched_data):
        try:
            flat_data = self.prepare_flat_data(enriched_data)
            
            df = daft.from_pydict({
                key: [value] for key, value in flat_data.items()
            })
            
            delta_path = f"az://{CONTAINER_NAME}/{NEW_DELTA_TABLE_PATH}"
            
            df.write_deltalake(
                delta_path,
                io_config=self.io_config,
                mode="append"
            )
            
            return True
            
        except Exception:
            return False
    
    def save_json_backup(self, enriched_data):
        try:
            current_time = datetime.now(timezone.utc)
            turbine_id = enriched_data.get('turbine_id', 'unknown')
            
            timestamp_str = current_time.strftime('%H%M%S_%f')[:-3]
            blob_name = f"varied-telemetry-backup/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/turbine={turbine_id}/telemetry_{timestamp_str}.json"
            
            blob_client = self.blob_service.get_blob_client(
                container=CONTAINER_NAME,
                blob=blob_name
            )
            
            json_data = json.dumps(enriched_data, ensure_ascii=False, indent=2)
            blob_client.upload_blob(json_data, overwrite=True)
            
            return True
            
        except Exception:
            return False
    
    def prepare_flat_data(self, enriched_data):
        current_time = datetime.now(timezone.utc)
        turbine_id = enriched_data.get('turbine_id', 'unknown')
        
        flat_data = {
            'record_id': f"{turbine_id}_{current_time.strftime('%Y%m%d_%H%M%S_%f')[:-3]}",
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
            'partition_turbine': turbine_id,
        }
        
        turbine_meta = enriched_data.get('turbine_metadata', {})
        flat_data.update({
            'turbine_name': str(turbine_meta.get('turbine_name', '')),
            'location_lat': float(turbine_meta.get('location_lat', 0)),
            'location_lng': float(turbine_meta.get('location_lng', 0)),
            'manufacturer': str(turbine_meta.get('manufacturer', '')),
            'model': str(turbine_meta.get('model', '')),
            'nominal_power_kw': int(turbine_meta.get('nominal_power_kw', 0)),
            'installation_date': str(turbine_meta.get('installation_date', '')),
            'turbine_status': str(turbine_meta.get('status', '')),
        })
        
        env_data = enriched_data.get('environmental_data', {})
        flat_data.update({
            'wind_speed_ms': float(env_data.get('wind_speed_ms', 0)),
            'wind_direction_degrees': int(env_data.get('wind_direction_degrees', 0)),
            'temperature_celsius': float(env_data.get('temperature_celsius', 0)),
            'humidity_percent': float(env_data.get('humidity_percent', 0)),
            'air_pressure_hpa': float(env_data.get('air_pressure_hpa', 0)),
            'visibility_km': float(env_data.get('visibility_km', 0)),
            'precipitation_mm': float(env_data.get('precipitation_mm', 0)),
        })
        
        maintenance_data = enriched_data.get('maintenance_data', {})
        flat_data.update({
            'maintenance_status': str(maintenance_data.get('maintenance_status', '')),
            'last_maintenance_date': str(maintenance_data.get('last_maintenance_date', '')),
            'next_maintenance_date': str(maintenance_data.get('next_maintenance_date', '')),
            'efficiency_rating': float(maintenance_data.get('efficiency_rating', 0)),
            'operating_hours': float(maintenance_data.get('operating_hours', 0)),
            'maintenance_notes': str(maintenance_data.get('maintenance_notes', '')),
            'technician_name': str(maintenance_data.get('technician_name', '')),
        })
        
        calc_metrics = enriched_data.get('calculated_metrics', {})
        flat_data.update({
            'efficiency_percent': float(calc_metrics.get('efficiency_percent', 0)),
            'operational_status': str(calc_metrics.get('operational_status', '')),
            'calculated_power_kw': float(calc_metrics.get('calculated_power_kw', 0)),
        })
        
        enrichment_info = enriched_data.get('enrichment_info', {})
        flat_data.update({
            'data_quality_score': float(enrichment_info.get('data_quality_score', 1.0)),
            'enrichment_version': str(enrichment_info.get('enrichment_version', '')),
            'source': str(enrichment_info.get('source', '')),
            'processed_by': str(enrichment_info.get('processed_by', '')),
        })
        
        flat_data.update({
            'record_version': 'varied_v1',
            'has_time_variation': True,
        })
        
        return flat_data
    
    def process_all_turbines(self, records_per_turbine=12):
        print("Starting telemetry generation...")
        
        turbine_ids = self.get_all_turbine_ids()
        
        if not turbine_ids:
            return
        
        total_processed = 0
        delta_successful = 0
        json_successful = 0
        
        for turbine_index, turbine_id in enumerate(turbine_ids):
            print(f"Processing {turbine_id} ({turbine_index + 1}/{len(turbine_ids)})")
            
            enriched_data_list = self.get_enriched_data_for_turbine(turbine_id, records_per_turbine, turbine_index)
            
            if not enriched_data_list:
                continue
            
            for enriched_data in enriched_data_list:
                delta_success = self.save_to_delta_table(enriched_data)
                json_success = self.save_json_backup(enriched_data)
                
                if delta_success:
                    delta_successful += 1
                if json_success:
                    json_successful += 1
                
                total_processed += 1
                time.sleep(0.05)
        
        print(f"Complete: {total_processed} records, {delta_successful} delta, {json_successful} json")

def main():
    generator = TelemetryGenerator()
    generator.process_all_turbines(records_per_turbine=12)

if __name__ == "__main__":
    main()