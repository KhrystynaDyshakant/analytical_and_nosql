import pyodbc
import json
import os
from datetime import datetime
from typing import Dict, List

class SQLToFileCacheLoader:
    """Завантажувач метаданих з Azure SQL DB до файлового кешу"""
    
    def __init__(self):
        # SQL Database connection
        self.sql_server = "windfarm6-sql-northeu.database.windows.net"
        self.sql_database = "WindFarmDB"
        self.sql_username = "sqladmin"
        self.sql_password = os.getenv("AZURE_SQL_PASSWORD", "WindFarm123!")
        
        # Директорія для кешу
        self.cache_dir = "metadata_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.driver = self._get_best_driver()
        
        if self.sql_password == "your_password_here":
            raise ValueError("Встановіть AZURE_SQL_PASSWORD: set AZURE_SQL_PASSWORD=ваш_пароль")
    
    def _get_best_driver(self):
        drivers = pyodbc.drivers()
        
        # Пріоритетний список драйверів
        preferred_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server"
        ]
        
        for preferred in preferred_drivers:
            if preferred in drivers:
                print(f" Використовуємо драйвер: {preferred}")
                return preferred
        
        raise Exception("Не знайдено підходящого ODBC драйвера для SQL Server")
    
    def connect_to_sql(self):
        """Підключення до Azure SQL Database"""
        try:
            conn_string = f"DRIVER={{{self.driver}}};SERVER={self.sql_server};DATABASE={self.sql_database};UID={self.sql_username};PWD={self.sql_password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            
            conn = pyodbc.connect(conn_string)
            print(" Успішно підключено до Azure SQL Database")
            return conn
            
        except Exception as e:
            print(f" Помилка підключення до SQL: {e}")
            return None
    
    def load_turbines_metadata(self, sql_conn) -> List[Dict]:
        """Завантаження метаданих турбін з SQL"""
        query = """
        SELECT 
            turbine_id,
            location_name,
            latitude,
            longitude,
            installation_date,
            manufacturer,
            model,
            nominal_power_kw,
            status
        FROM Turbines
        ORDER BY turbine_id
        """
        
        cursor = sql_conn.cursor()
        cursor.execute(query)
        
        turbines = []
        for row in cursor.fetchall():
            turbine = {
                'turbine_id': row[0],
                'turbine_name': row[1],
                'location_lat': float(row[2]) if row[2] else None,
                'location_lng': float(row[3]) if row[3] else None,
                'installation_date': row[4].isoformat() if row[4] else None,
                'manufacturer': row[5],
                'model': row[6],
                'nominal_power_kw': int(row[7]) if row[7] else None,
                'hub_height_m': None,
                'rotor_diameter_m': None,
                'status': row[8]
            }
            turbines.append(turbine)
        
        print(f" Завантажено {len(turbines)} турбін з SQL Database")
        return turbines
    
    def load_sensor_types_metadata(self, sql_conn) -> List[Dict]:
        """Завантаження метаданих типів сенсорів з SQL"""
        query = """
        SELECT 
            sensor_type_id,
            sensor_name,
            parameter_name,
            description,
            unit_of_measurement,
            min_value,
            max_value,
            manufacturer,
            sensor_model
        FROM SensorTypes
        ORDER BY sensor_type_id
        """
        
        cursor = sql_conn.cursor()
        cursor.execute(query)
        
        sensors = []
        for row in cursor.fetchall():
            sensor = {
                'sensor_type_id': row[0],
                'sensor_name': row[1],
                'parameter_name': row[2],
                'description': row[3],
                'unit_of_measurement': row[4],
                'min_value': float(row[5]) if row[5] else None,
                'max_value': float(row[6]) if row[6] else None,
                'manufacturer': row[7],
                'sensor_model': row[8]
            }
            sensors.append(sensor)
        
        print(f" Завантажено {len(sensors)} типів сенсорів з SQL Database")
        return sensors
    
    def load_turbine_sensors_metadata(self, sql_conn) -> List[Dict]:
        """Завантаження метаданих сенсорів на турбінах з SQL"""
        query = """
        SELECT 
            ts.turbine_sensor_id,
            ts.turbine_id,
            ts.sensor_type_id,
            t.location_name as turbine_name,
            st.sensor_name,
            st.parameter_name,
            st.unit_of_measurement,
            st.description
        FROM TurbineSensors ts
        INNER JOIN Turbines t ON ts.turbine_id = t.turbine_id
        INNER JOIN SensorTypes st ON ts.sensor_type_id = st.sensor_type_id
        ORDER BY ts.turbine_id, ts.sensor_type_id
        """
        
        cursor = sql_conn.cursor()
        cursor.execute(query)
        
        turbine_sensors = []
        for row in cursor.fetchall():
            turbine_sensor = {
                'turbine_sensor_id': row[0],
                'turbine_id': row[1],
                'sensor_type_id': row[2],
                'turbine_name': row[3],
                'sensor_name': row[4],
                'parameter_name': row[5],
                'unit_of_measurement': row[6],
                'description': row[7]
            }
            turbine_sensors.append(turbine_sensor)
        
        print(f" Завантажено {len(turbine_sensors)} зв'язків турбін-сенсорів з SQL Database")
        return turbine_sensors
    
    def save_to_cache_file(self, filename: str, data, description: str = ""):
        """Збереження даних у файловий кеш (замість Redis)"""
        try:
            filepath = os.path.join(self.cache_dir, f"{filename}.json")
            
            # Структура, що імітує Redis
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'description': description,
                'source': 'Azure SQL Database',
                'count': len(data) if isinstance(data, (list, dict)) else 1,
                'data': data,
                'cache_type': 'file_based_redis_replacement'
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            print(f" Збережено у файловий кеш: {filepath} ({cache_data['count']} записів)")
        except Exception as e:
            print(f" Помилка збереження у кеш {filename}: {e}")
    
    def create_lookup_indices(self, turbines: List[Dict], sensors: List[Dict], turbine_sensors: List[Dict]):
        """Створення індексів для швидкого пошуку (як Redis Hash Maps)"""
        
        # Індекс турбін по ID (імітує Redis HGETALL turbines:lookup)
        turbine_lookup = {str(t['turbine_id']): t for t in turbines}
        self.save_to_cache_file("turbines_lookup", turbine_lookup, "Lookup турбін по ID")
        
        # Індекс сенсорів по ID (імітує Redis HGETALL sensors:lookup)
        sensor_lookup = {str(s['sensor_type_id']): s for s in sensors}
        self.save_to_cache_file("sensors_lookup", sensor_lookup, "Lookup сенсорів по ID")
        
        # Індекс сенсорів по турбіні (імітує Redis HGETALL turbine_sensors:lookup)
        sensors_by_turbine = {}
        for ts in turbine_sensors:
            turbine_id = str(ts['turbine_id'])
            if turbine_id not in sensors_by_turbine:
                sensors_by_turbine[turbine_id] = []
            sensors_by_turbine[turbine_id].append(ts)
        
        self.save_to_cache_file("turbine_sensors_lookup", sensors_by_turbine, "Lookup сенсорів по турбінах")
        
        print(" Створено всі індекси для швидкого пошуку")
    
    def validate_database_structure(self, sql_conn) -> bool:
        """Перевірка структури бази даних"""
        try:
            cursor = sql_conn.cursor()
            
            # Перевірка наявності таблиць
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]
            
            required_tables = ['Turbines', 'SensorTypes', 'TurbineSensors']
            missing_tables = [table for table in required_tables if table not in tables]
            
            if missing_tables:
                print(f" Відсутні таблиці: {missing_tables}")
                return False
            
            print(f" Всі необхідні таблиці присутні: {required_tables}")
            
            # Перевірка кількості записів
            for table in required_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   {table}: {count} записів")
            
            return True
            
        except Exception as e:
            print(f" Помилка перевірки структури БД: {e}")
            return False
    
    def run(self):
        """Основний метод: SQL → File Cache (замість Redis)"""
        print("Завантаження метаданих з Azure SQL Database до файлового кешу...")
        
        # Підключення до SQL
        sql_conn = self.connect_to_sql()
        if not sql_conn:
            return False
        
        try:
            # Перевірка структури БД
            if not self.validate_database_structure(sql_conn):
                return False
            
            # Завантаження даних з SQL
            print("Завантаження даних з Azure SQL Database...")
            turbines = self.load_turbines_metadata(sql_conn)
            sensors = self.load_sensor_types_metadata(sql_conn)
            turbine_sensors = self.load_turbine_sensors_metadata(sql_conn)
            
            # Збереження в файловий кеш (замість Redis)
            print("Збереження у файловий кеш...")
            self.save_to_cache_file("turbines_all", turbines, "Всі турбіни")
            self.save_to_cache_file("sensors_all", sensors, "Всі типи сенсорів")
            self.save_to_cache_file("turbine_sensors_all", turbine_sensors, "Всі зв'язки турбін-сенсорів")
            
            # Створення індексів
            print("Створення індексів для швидкого пошуку...")
            self.create_lookup_indices(turbines, sensors, turbine_sensors)
            
            # Підсумок
            print(f"Успішно завантажено метадані з Azure SQL Database!")
            print(f"Статистика:")
            print(f"Турбін: {len(turbines)}")
            print(f"Типів сенсорів: {len(sensors)}")
            print(f"Зв'язків турбін-сенсорів: {len(turbine_sensors)}")
            print(f"Файлів кешу створено: 6")
            
            # Показуємо приклад даних
            if turbines:
                print(f"Приклад турбіни: {turbines[0]['turbine_id']} - {turbines[0].get('turbine_name', 'N/A')}")
            if sensors:
                print(f"Приклад сенсора: {sensors[0].get('sensor_name', 'N/A')} ({sensors[0].get('parameter_name', 'N/A')})")
            
            return True
            
        except Exception as e:
            print(f"Помилка під час виконання: {e}")
            return False
        finally:
            sql_conn.close()
            print("Підключення до SQL закрито")

# Функція для тестування використання кешу
def test_cache_usage():
    """Тестування використання створеного файлового кешу"""
    print("Тестування використання файлового кешу...")
    
    cache_dir = "metadata_cache"
    
    try:
        # Тест завантаження турбіни
        with open(f"{cache_dir}/turbines_lookup.json", 'r', encoding='utf-8') as f:
            turbines_cache = json.load(f)
            turbines_data = turbines_cache['data']
        
        if 'TURBINE_001' in turbines_data:
            turbine = turbines_data['TURBINE_001']
            print(f" Турбіна TURBINE_001: {turbine.get('turbine_name', 'N/A')} ({turbine.get('manufacturer', 'N/A')})")
        
        # Тест завантаження сенсорів турбіни
        with open(f"{cache_dir}/turbine_sensors_lookup.json", 'r', encoding='utf-8') as f:
            sensors_cache = json.load(f)
            sensors_data = sensors_cache['data']
        
        if 'TURBINE_001' in sensors_data:
            sensors = sensors_data['TURBINE_001']
            print(f" Сенсори TURBINE_001: {len(sensors)} шт.")
            
            for sensor in sensors[:3]:  # Показуємо перші 3
                print(f"   - {sensor.get('sensor_name', 'N/A')}: {sensor.get('parameter_name', 'N/A')} ({sensor.get('unit_of_measurement', 'N/A')})")
        
        print(" Файловий кеш працює правильно!")
        return True
        
    except Exception as e:
        print(f" Помилка тестування кешу: {e}")
        return False

if __name__ == "__main__":
    print("=== Завантажувач метаданих: Azure SQL DB → File Cache ===")
    
    try:
        loader = SQLToFileCacheLoader()
        
        # Завантаження метаданих
        if loader.run():
            # Тестування використання
            test_cache_usage()

            print("Azure Functions читатимуть метадані з файлового кешу")
        else:
            print(" Завантаження не вдалось")
            
    except Exception as e:
        print(f" Помилка: {e}")