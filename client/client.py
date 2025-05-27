# client/client.py
import requests
import socket
import os
import sys
from typing import List

# Добавляем корневую директорию в путь
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.protocols import Protocol, MessageType, ServerInfo, FileInfo

class FileManagerClient:
    def __init__(self):
        self.discovery_server_url = "http://localhost:8000"  # Прямое подключение
        self.available_servers = []
        
    def discover_discovery_server(self):
        """Находит Discovery Server (упрощенная версия)"""
        try:
            response = requests.get(f"{self.discovery_server_url}/servers", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            print("Discovery server not found at localhost:8000")
            return False
    
    def get_available_servers(self) -> List[dict]:
        """Получает список доступных серверов"""
        if not self.discover_discovery_server():
            return []
        
        try:
            response = requests.get(f"{self.discovery_server_url}/servers", timeout=5)
            if response.status_code == 200:
                servers_data = response.json()
                # Преобразуем данные из базы в нужный формат
                self.available_servers = []
                for server in servers_data:
                    # Обрабатываем данные из базы (могут быть разные ключи)
                    server_info = {
                        'server_id': server.get('server_id', ''),
                        'ip_address': str(server.get('ip_address', 'localhost')),
                        'port': server.get('port', 0),
                        'service_type': server.get('service_type', ''),
                        'status': server.get('status', 'unknown')
                    }
                    self.available_servers.append(server_info)
                return self.available_servers
        except requests.RequestException as e:
            print(f"Error getting servers: {e}")
        
        return []
    
    def get_files_from_server(self, server: dict) -> List[dict]:
        """Получает список файлов с конкретного сервера"""
        try:
            url = f"http://{server['ip_address']}:{server['port']}/files"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return response.json()
        except requests.RequestException as e:
            print(f"Error getting files from {server['server_id']}: {e}")
        
        return []
    
    def get_all_files(self) -> List[dict]:
        """Получает список всех доступных файлов"""
        all_files = []
        servers = self.get_available_servers()
        
        print(f"Found {len(servers)} servers")
        
        # Используем set для отслеживания уже обработанных серверов
        processed_servers = set()
        
        for server in servers:
            server_key = f"{server['server_id']}:{server['port']}"
            
            if server['service_type'] == 'file_server' and server_key not in processed_servers:
                print(f"Getting files from server {server['server_id']} on port {server['port']}")
                files = self.get_files_from_server(server)
                
                # Добавляем информацию о сервере к каждому файлу
                for file in files:
                    file['server_info'] = server  # Сохраняем полную информацию о сервере
                
                all_files.extend(files)
                processed_servers.add(server_key)
        
        return all_files

    
    def download_file(self, filename: str, server_info: dict, save_path: str = "./downloads"):
        """Скачивает файл с указанного сервера"""
        try:
            # Проверяем корректность server_info
            if not server_info or 'ip_address' not in server_info or 'port' not in server_info:
                print(f"Invalid server info: {server_info}")
                return False
                
            url = f"http://{server_info['ip_address']}:{server_info['port']}/download/{filename}"
            print(f"Downloading from: {url}")
            
            response = requests.get(url, stream=True, timeout=30)
            
            if response.status_code == 200:
                os.makedirs(save_path, exist_ok=True)
                file_path = os.path.join(save_path, filename)
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print(f"File {filename} downloaded successfully to {file_path}")
                return True
            else:
                print(f"Failed to download {filename}: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except requests.RequestException as e:
            print(f"Error downloading file: {e}")
            return False

    
    def get_server_by_id(self, server_id: str) -> dict:
        """Находит сервер по ID"""
        for server in self.available_servers:
            if server['server_id'] == server_id:
                return server
        return None
    
    def show_server_status(self):
        """Показывает статус всех серверов с проверкой доступности"""
        servers = self.get_available_servers()
        
        if not servers:
            print("No servers found")
            return
        
        print("\n=== Server Status ===")
        for server in servers:
            print(f"Server ID: {server['server_id']}")
            print(f"Type: {server['service_type']}")
            print(f"Address: {server['ip_address']}:{server['port']}")
            print(f"DB Status: {server['status']}")
            
            # Проверяем heartbeat
            if 'seconds_since_heartbeat' in server:
                heartbeat_age = server['seconds_since_heartbeat']
                if heartbeat_age > 30:
                    print(f"Heartbeat: OLD ({heartbeat_age:.1f}s ago) ⚠️")
                else:
                    print(f"Heartbeat: OK ({heartbeat_age:.1f}s ago) ✅")
            
            # Проверяем реальную доступность
            if server['service_type'] == 'file_server':
                try:
                    status_url = f"http://{server['ip_address']}:{server['port']}/status"
                    response = requests.get(status_url, timeout=3)
                    if response.status_code == 200:
                        status_data = response.json()
                        print(f"Real Status: ONLINE ✅")
                        print(f"Files: {status_data.get('files_count', 0)}")
                        print(f"Total Size: {status_data.get('total_size', 0)} bytes")
                    else:
                        print(f"Real Status: ERROR (HTTP {response.status_code}) ❌")
                except Exception as e:
                    print(f"Real Status: UNREACHABLE ❌")
                    print(f"Error: {str(e)}")
            print("-" * 50)
        
        # Предлагаем очистку неактивных серверов
        print("\nOptions:")
        print("c - Cleanup inactive servers")
        choice = input("Enter option (or press Enter to continue): ").strip().lower()
        
        if choice == 'c':
            self.cleanup_inactive_servers()

    def cleanup_inactive_servers(self):
        """Запрашивает очистку неактивных серверов"""
        try:
            response = requests.post(f"{self.discovery_server_url}/cleanup", timeout=5)
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Cleanup completed. Updated {result['updated_servers']} servers.")
            else:
                print(f"❌ Cleanup failed: {response.status_code}")
        except requests.RequestException as e:
            print(f"❌ Cleanup error: {e}")

    
    def run_interactive(self):
        """Интерактивный режим работы клиента"""
        print("=== Distributed File Manager Client ===")
        
        while True:
            print("\n1. List all files")
            print("2. Download file")
            print("3. Show server status")
            print("4. Refresh servers")
            print("5. Exit")
            
            choice = input("Choose option: ").strip()
            
            if choice == "1":
                print("\nGetting files from all servers...")
                files = self.get_all_files()
                if files:
                    print(f"\nFound {len(files)} files:")
                    for i, file in enumerate(files, 1):
                        print(f"{i}. {file['filename']} ({file['file_size']} bytes) - Server: {file['server_id']}")
                else:
                    print("No files available")
            
            elif choice == "2":
                files = self.get_all_files()
                if not files:
                    print("No files available")
                    continue
                
                print("\nAvailable files:")
                for i, file in enumerate(files, 1):
                    print(f"{i}. {file['filename']} - Server: {file['server_id']}")
                
                try:
                    file_index = int(input("Enter file number: ")) - 1
                    if 0 <= file_index < len(files):
                        selected_file = files[file_index]
                        server = self.get_server_by_id(selected_file['server_id'])
                        if server:
                            print(f"Downloading {selected_file['filename']}...")
                            self.download_file(selected_file['filename'], server)
                        else:
                            print("Server not found")
                    else:
                        print("Invalid file number")
                except ValueError:
                    print("Invalid input")
            
            elif choice == "3":
                self.show_server_status()
            
            elif choice == "4":
                servers = self.get_available_servers()
                print(f"Found {len(servers)} servers")
            
            elif choice == "5":
                break
            
            else:
                print("Invalid choice")

if __name__ == "__main__":
    client = FileManagerClient()
    client.run_interactive()
