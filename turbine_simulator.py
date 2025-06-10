import asyncio
import json
import random
import time
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message

TELEMETRY_MIN_INTERVAL = 6
TELEMETRY_MAX_INTERVAL = 56
MAX_VARIATION = 0.1  # 10% максимальна варіація між відліками

# Реалістичні діапазони для електричних параметрів
SENSOR_RANGES = {
    'output_power': {'min': 0, 'max': 15000, 'unit': 'kW', 'typical': 8000},
    'rotor_rpm': {'min': 0, 'max': 30, 'unit': 'RPM', 'typical': 15},
    'max_power_limit': {'min': 7000, 'max': 14000, 'unit': 'kW', 'typical': 12000},
    'voltage': {'min': 400, 'max': 690, 'unit': 'V', 'typical': 630},
    'current': {'min': 0, 'max': 5000, 'unit': 'A', 'typical': 2000},
    'power_factor': {'min': 0.80, 'max': 0.95, 'unit': '', 'typical': 0.90}
}

class TurbineSimulator:
    def __init__(self, turbine_id, connection_string):
        self.turbine_id = turbine_id
        self.connection_string = connection_string
        self.client = None
        self.previous_values = {}
        self.is_running = False
        
        # Ініціалізація початкових значень
        self._initialize_values()
    
    def _initialize_values(self):
        """Ініціалізація початкових реалістичних значень"""
        self.previous_values = {}
        for param, config in SENSOR_RANGES.items():
            # Початкові значення близько до типових
            typical = config['typical']
            variation = typical * 0.2  # 20% варіація від типового
            initial_value = random.uniform(typical - variation, typical + variation)
            
            # Обмеження в межах допустимого діапазону
            initial_value = max(config['min'], min(config['max'], initial_value))
            self.previous_values[param] = initial_value
    
    def generate_telemetry_data(self):
        """Генерація реалістичних телеметричних даних з 10% варіацією"""
        data = {
            'turbine_id': self.turbine_id,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        # Генерація кожного параметра з урахуванням попередніх значень
        for param, config in SENSOR_RANGES.items():
            if param in self.previous_values:
                prev_value = self.previous_values[param]
                
                # 10% варіація від попереднього значення
                variation = prev_value * MAX_VARIATION
                min_val = max(config['min'], prev_value - variation)
                max_val = min(config['max'], prev_value + variation)
                
                new_value = random.uniform(min_val, max_val)
            else:
                # Якщо немає попереднього значення, генеруємо нове
                new_value = random.uniform(config['min'], config['max'])
            
            # Округлення для кращого вигляду
            if param == 'power_factor':
                new_value = round(new_value, 3)
            elif param in ['output_power', 'max_power_limit', 'current']:
                new_value = round(new_value, 1)
            elif param in ['voltage']:
                new_value = round(new_value, 0)
            elif param == 'rotor_rpm':
                new_value = round(new_value, 2)
            
            data[param] = new_value
            self.previous_values[param] = new_value
        
        # Розрахунок додаткових параметрів для реалістичності
        self._calculate_derived_parameters(data)
        
        return data
    
    def _calculate_derived_parameters(self, data):
        """Розрахунок похідних параметрів для реалістичності"""
        # Струм залежить від потужності та напруги
        if data['output_power'] > 0 and data['voltage'] > 0:
            # P = U * I * cos(φ) * √3 (для 3-фазної системи)
            calculated_current = (data['output_power'] * 1000) / (data['voltage'] * data['power_factor'] * 1.732)
            
            # Змішуємо розрахований струм з генерованим для реалізму
            data['current'] = round((data['current'] + calculated_current) / 2, 1)
            self.previous_values['current'] = data['current']
        
        # max_power_limit має бути більшим за поточну потужність
        if data['max_power_limit'] < data['output_power']:
            data['max_power_limit'] = data['output_power'] * random.uniform(1.1, 1.3)
            data['max_power_limit'] = round(data['max_power_limit'], 1)
            self.previous_values['max_power_limit'] = data['max_power_limit']
    
    async def connect(self):
        """Підключення до IoT Hub"""
        try:
            self.client = IoTHubDeviceClient.create_from_connection_string(self.connection_string)
            await self.client.connect()
            print(f" {self.turbine_id}: Підключено до IoT Hub")
            return True
        except Exception as e:
            print(f" {self.turbine_id}: Помилка підключення: {e}")
            return False
    
    async def disconnect(self):
        """Відключення від IoT Hub"""
        if self.client:
            await self.client.disconnect()
            print(f"🔌 {self.turbine_id}: Відключено від IoT Hub")
    
    async def send_telemetry(self, data):
        """Відправка телеметрії в IoT Hub"""
        try:
            message = Message(json.dumps(data))
            message.content_encoding = "utf-8"
            message.content_type = "application/json" # Використовує MQTT
            
            await self.client.send_message(message)
            print(f" {self.turbine_id}: Відправлено телеметрію - Power: {data['output_power']}kW, RPM: {data['rotor_rpm']}")
            return True
        except Exception as e:
            print(f" {self.turbine_id}: Помилка відправки: {e}")
            return False
    
    def get_next_interval(self):
        """Генерація випадкового інтервалу передачі"""
        return random.randint(TELEMETRY_MIN_INTERVAL, TELEMETRY_MAX_INTERVAL)
    
    async def run_simulation(self):
        """Головний цикл симуляції"""
        if not await self.connect():
            return
        
        self.is_running = True
        print(f" {self.turbine_id}: Розпочато симуляцію телеметрії")
        
        try:
            while self.is_running:
                # Генерація та відправка телеметрії
                telemetry_data = self.generate_telemetry_data()
                await self.send_telemetry(telemetry_data)
                
                # Чекання до наступної передачі
                next_interval = self.get_next_interval()
                print(f" {self.turbine_id}: Наступна передача через {next_interval} секунд")
                await asyncio.sleep(next_interval)
                
        except KeyboardInterrupt:
            print(f" {self.turbine_id}: Зупинка симуляції...")
        except Exception as e:
            print(f" {self.turbine_id}: Помилка симуляції: {e}")
        finally:
            self.is_running = False
            await self.disconnect()

async def run_multiple_turbines():
    """Запуск симуляції для всіх турбін"""
    # Завантаження connection strings
    try:
        with open('all_connections.json', 'r', encoding='utf-8') as f:
            connections = json.load(f)
    except FileNotFoundError:
        print(" Файл all_connections.json не знайдено!")
        return
    
    # Створення симуляторів для всіх турбін
    simulators = []
    for i in range(1, 8):  # TURBINE_001 до TURBINE_007
        turbine_id = f"TURBINE_{str(i).zfill(3)}"
        if turbine_id in connections:
            simulator = TurbineSimulator(turbine_id, connections[turbine_id])
            simulators.append(simulator)
        else:
            print(f" Connection string для {turbine_id} не знайдено")
    
    if not simulators:
        print(" Жодного симулятора не створено!")
        return
    
    print(f" Запуск {len(simulators)} симуляторів турбін...")
    print(" Електричні параметри: output_power, rotor_rpm, max_power_limit, voltage, current, power_factor")
    print(" Інтервал передачі: 6-56 секунд")
    print(" Варіація між відліками: ±10%")
    print("Ctrl+C для зупинки\n")
    
    # Запуск всіх симуляторів паралельно
    tasks = [simulator.run_simulation() for simulator in simulators]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n Зупинка всіх симуляторів...")
        for simulator in simulators:
            simulator.is_running = False

if __name__ == "__main__":
    print(" Симулятор вітрових турбін - Варіант 3: Електричні параметри")
    print("=" * 70)
    asyncio.run(run_multiple_turbines())