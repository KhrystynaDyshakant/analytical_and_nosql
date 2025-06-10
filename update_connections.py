import json
import subprocess

def get_iot_hub_connection():
    """Отримання IoT Hub connection string"""
    command = "az iot hub connection-string show --hub-name windlabiot6 --output tsv --query connectionString"
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None
    except:
        return None

def get_event_hub_connection():
    """Отримання Event Hub connection string"""
    command = """az eventhubs namespace authorization-rule keys list \
        --resource-group WindFarmLabRG \
        --namespace-name windfarm6-eventhub \
        --name RootManageSharedAccessKey \
        --query primaryConnectionString \
        --output tsv"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None
    except:
        return None

def get_storage_connection():
    """Отримання Storage Account connection string"""
    command = """az storage account show-connection-string \
        --resource-group WindFarmLabRG \
        --name windfarm6storage \
        --query connectionString \
        --output tsv"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None
    except:
        return None

def main():
    print(" Оновлення всіх connection strings")
    print("=" * 50)
    
    # Завантаження існуючих device connections
    try:
        with open('device_connections.json', 'r', encoding='utf-8') as f:
            connections = json.load(f)
    except FileNotFoundError:
        connections = {}
    
    # SQL Connection String (вручну, оскільки CLI не працює)
    sql_connection = """Server=tcp:windfarm6-sql-northeu.database.windows.net,1433;Initial Catalog=WindFarmDB;Persist Security Info=False;User ID=sqladmin;Password=WindFarm123!;MultipleActiveResultSets=False;Encrypt=true;TrustServerCertificate=False;Connection Timeout=30;"""
    
    # Отримання інших connection strings
    print("Отримання IoT Hub connection string...")
    iot_hub_conn = get_iot_hub_connection()
    
    print("Отримання Event Hub connection string...")
    event_hub_conn = get_event_hub_connection()
    
    print("Отримання Storage Account connection string...")
    storage_conn = get_storage_connection()
    
    # Оновлення connections
    connections.update({
        'sql_database': sql_connection,
        'iot_hub_service': iot_hub_conn,
        'event_hub': event_hub_conn,
        'storage_account': storage_conn,
        'redis': 'windfarm6-redis.redis.cache.windows.net:6380,password=<key>,ssl=True,abortConnect=False',  # Буде оновлено пізніше
        'azure_config': {
            'resource_group': 'WindFarmLabRG',
            'location': 'North Europe',
            'iot_hub_name': 'windlabiot6',
            'event_hub_namespace': 'windfarm6-eventhub',
            'event_hub_name': 'turbine-telemetry',
            'sql_server_name': 'windfarm6-sql-northeu',
            'sql_database_name': 'WindFarmDB',
            'redis_name': 'windfarm6-redis',
            'storage_account_name': 'windfarm6storage',
            'container_name': 'telemetry-data'
        }
    })
    
    # Збереження
    with open('all_connections.json', 'w', encoding='utf-8') as f:
        json.dump(connections, f, indent=2, ensure_ascii=False)
    
    print("\n Connection strings оновлено!")
    print(" Збережено в 'all_connections.json'")
    
    # Показати статус
    print(f"\n Статус підключень:")
    print(f" SQL Database: OK")
    print(f"{'✅' if iot_hub_conn else '❌'} IoT Hub: {'OK' if iot_hub_conn else 'Помилка'}")
    print(f"{'✅' if event_hub_conn else '❌'} Event Hub: {'OK' if event_hub_conn else 'Помилка'}")
    print(f"{'✅' if storage_conn else '❌'} Storage Account: {'OK' if storage_conn else 'Помилка'}")
    print(f" Redis Cache: В процесі створення...")
    
    print(f"\n Кількість IoT пристроїв: {len([k for k in connections.keys() if k.startswith('TURBINE_')])}")

if __name__ == "__main__":
    main()