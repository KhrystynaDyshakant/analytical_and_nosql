import pandas as pd
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from io import BytesIO

STORAGE_CONNECTION = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=windfarm6storage;AccountKey=X7YVa9h/iiKRdw0sYlgaUkb8RFi3U+FRnKR/86DkYxiT8WB4KVPOeBxvGdp0yHDYRMAVVa1FpyIF+AStPVbVPQ==;BlobEndpoint=https://windfarm6storage.blob.core.windows.net/"

CONTAINER_NAME = "telemetry-data"
DELTA_TABLE_PATH = "delta-lake-turbine-telemetry-full"

class DeltaLakeReader:
    """Читач Delta Lake файлів"""
    
    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION)
    
    def list_delta_files(self):
        """Показати всі файли в Delta Lake"""
        print(" Файли в Delta Lake:")
        print("=" * 50)
        
        container_client = self.blob_service.get_container_client(CONTAINER_NAME)
        
        blobs = container_client.list_blobs(name_starts_with=DELTA_TABLE_PATH)
        
        parquet_files = []
        json_files = []
        
        for blob in blobs:
            file_size = f"{blob.size / 1024:.1f} KB" if blob.size else "0 KB"
            print(f" {blob.name} ({file_size})")
            
            if blob.name.endswith('.parquet'):
                parquet_files.append(blob.name)
            elif blob.name.endswith('.json'):
                json_files.append(blob.name)
        
        print(f"\n Знайдено:")
        print(f"   • Parquet файлів: {len(parquet_files)}")
        print(f"   • JSON файлів: {len(json_files)}")
        
        return parquet_files, json_files
    
    def read_parquet_file(self, blob_name: str):
        """Прочитати конкретний Parquet файл"""
        try:
            print(f"\n Читання файлу: {blob_name}")
            
            # Завантажити файл з Azure
            blob_client = self.blob_service.get_blob_client(
                container=CONTAINER_NAME,
                blob=blob_name
            )
            
            blob_data = blob_client.download_blob().readall()
            
            # Прочитати як Parquet
            buffer = BytesIO(blob_data)
            df = pd.read_parquet(buffer)
            
            print(f" Файл прочитано успішно!")
            print(f" Розмір: {len(df)} рядків, {len(df.columns)} колонок")
            print(f" Колонки: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            print(f" Помилка читання {blob_name}: {e}")
            return None
    
    def read_all_delta_data(self):
        """Прочитати всі дані з Delta Lake"""
        print("\n Читання всіх даних з Delta Lake...")
        
        parquet_files, _ = self.list_delta_files()
        
        all_data = []
        
        for file_name in parquet_files:
            df = self.read_parquet_file(file_name)
            if df is not None:
                all_data.append(df)
        
        if all_data:
            # Об'єднати всі DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)
            
            print(f"\n РЕЗУЛЬТАТ:")
            print(f" Всього записів: {len(combined_df)}")
            print(f" Колонки: {len(combined_df.columns)}")
            
            # Показати перші записи
            print(f"\n Перші 5 записів:")
            print(combined_df.head())
            
            # Показати статистику по турбінам
            if 'turbine_id' in combined_df.columns:
                print(f"\n Статистика по турбінам:")
                turbine_stats = combined_df['turbine_id'].value_counts()
                for turbine, count in turbine_stats.items():
                    print(f"   • {turbine}: {count} записів")
            
            # Показати колонки з типами
            print(f"\n Структура даних:")
            for col in combined_df.columns:
                dtype = combined_df[col].dtype
                print(f"   • {col}: {dtype}")
            
            return combined_df
        
        else:
            print(" Не вдалося прочитати жодного файлу")
            return None
    
    def read_delta_log(self):
        """Прочитати метадані Delta Lake"""
        print("\n Читання Delta Log метаданих...")
        
        try:
            container_client = self.blob_service.get_container_client(CONTAINER_NAME)
            
            log_blobs = container_client.list_blobs(name_starts_with=f"{DELTA_TABLE_PATH}/_delta_log")
            
            for blob in log_blobs:
                if blob.name.endswith('.json'):
                    print(f"\n Лог файл: {blob.name}")
                    
                    blob_client = self.blob_service.get_blob_client(
                        container=CONTAINER_NAME,
                        blob=blob.name
                    )
                    
                    content = blob_client.download_blob().readall().decode('utf-8')
                    
                    print(f" Вміст (перші 1000 символів):")
                    print(content[:1000])
                    
                    if len(content) > 1000:
                        print("...")
        
        except Exception as e:
            print(f" Помилка читання логів: {e}")

def main():
    
    reader = DeltaLakeReader()
    
    # 1. Показати список файлів
    reader.list_delta_files()
    
    # 2. Прочитати всі дані
    df = reader.read_all_delta_data()
    
    # 3. Показати метадані
    reader.read_delta_log()
    
    print("\n" + "=" * 80)
    print(" ЧИТАННЯ ЗАВЕРШЕНО!")
    if df is not None:
        print(f"Успішно прочитано {len(df)} записів з Delta Lake")

if __name__ == "__main__":
    main()