import random
from datetime import datetime

STUDENT_NUMBER = 6
LATE_SUBMISSION = True
DEVICE_COUNT = 7
DATA_VARIANT = 3
TELEMETRY_MIN_INTERVAL = 6
TELEMETRY_MAX_INTERVAL = 56

# Унікальний префікс для ресурсів
UNIQUE_PREFIX = f"windfarm{STUDENT_NUMBER}"

# Azure конфігурація
AZURE_CONFIG = {
    'resource_group': 'WindFarmLabRG',
    'location': 'West Europe',
    'iot_hub_name': f'{UNIQUE_PREFIX}-iothub',
    'event_hub_namespace': f'{UNIQUE_PREFIX}-eventhub',
    'event_hub_name': 'turbine-telemetry',
    'sql_server_name': f'{UNIQUE_PREFIX}-sqlserver',
    'sql_database_name': 'WindFarmDB',
    'redis_name': f'{UNIQUE_PREFIX}-redis',
    'storage_account_name': f'{UNIQUE_PREFIX}storage',
    'function_app_name': f'{UNIQUE_PREFIX}-functions',
    'container_name': 'telemetry-data'
}

TELEMETRY_SCHEMA = {
    'turbine_id': 'string',
    'timestamp': 'datetime',
    'output_power': 'float',
    'rotor_rpm': 'float',
    'max_power_limit': 'float',
    'voltage': 'float',
    'current': 'float',
    'power_factor': 'float'
}

# Реалістичні діапазони для електричних параметрів
SENSOR_RANGES = {
    'output_power': {'min': 0, 'max': 2000, 'unit': 'kW'},
    'rotor_rpm': {'min': 0, 'max': 30, 'unit': 'RPM'},
    'max_power_limit': {'min': 1000, 'max': 1800, 'unit': 'kW'},
    'voltage': {'min': 380, 'max': 690, 'unit': 'V'},
    'current': {'min': 0, 'max': 3000, 'unit': 'A'},
    'power_factor': {'min': 0.8, 'max': 0.95, 'unit': ''}
}

# Назви турбін
TURBINE_IDS = [f"TURBINE_{str(i+1).zfill(3)}" for i in range(DEVICE_COUNT)]

def generate_realistic_telemetry(previous_values=None):
    """Генерація телеметричних даних з 10% варіацією"""
    
    if previous_values is None:
        # Початкові значення
        data = {
            'output_power': random.uniform(800, 1600),
            'rotor_rpm': random.uniform(8, 25),
            'max_power_limit': random.uniform(1400, 1700),
            'voltage': random.uniform(400, 650),
            'current': random.uniform(1000, 2500),
            'power_factor': random.uniform(0.85, 0.92)
        }
    else:
        # Варіація не більше 10% від попередніх значень
        data = {}
        for key, prev_value in previous_values.items():
            if key in SENSOR_RANGES:
                variation = prev_value * 0.1
                min_val = max(SENSOR_RANGES[key]['min'], prev_value - variation)
                max_val = min(SENSOR_RANGES[key]['max'], prev_value + variation)
                data[key] = random.uniform(min_val, max_val)
    
    # Розрахунок струму на основі потужності та напруги
    if data['output_power'] > 0 and data['voltage'] > 0:
        data['current'] = (data['output_power'] * 1000) / (data['voltage'] * data['power_factor'] * 1.732)
    
    return data

def get_telemetry_interval():
    """Повертає випадковий інтервал між передачами"""
    return random.randint(TELEMETRY_MIN_INTERVAL, TELEMETRY_MAX_INTERVAL)

CONNECTION_STRINGS = {
    'iot_hub': '',
    'event_hub': '',
    'sql_database': '',
    'redis': '',
    'storage_account': ''
}

print(f"Кількість пристроїв: {DEVICE_COUNT}")
print(f"Варіант даних: {DATA_VARIANT} (Електричні параметри)")
print(f"Інтервал телеметрії: {TELEMETRY_MIN_INTERVAL}-{TELEMETRY_MAX_INTERVAL} секунд")
print(f"Турбіни: {', '.join(TURBINE_IDS)}")