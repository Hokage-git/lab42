# file_server/file_server.py
import os
import hashlib
import requests
import threading
import time
import socket
from flask import Flask, request, jsonify, send_file
from typing import List
import uuid
import sys
from datetime import datetime

# Добавляем корневую директорию в путь
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.protocols import Protocol, MessageType, ServerInfo, FileInfo
from shared.database import DatabaseManager

class FileServer:
    def __init__(self, server_id=None, port=9000, files_directory=None):
        self.server_id = server_id or str(uuid.uuid4())
        self.port = port
        # Создаем уникальную директорию для каждого сервера
        self.files_directory = files_directory or f"./server_{port}_files"
        self.discovery_server_url = "http://localhost:8000"
        self.db = DatabaseManager()  # Подключение к базе данных
        self.app = Flask(__name__)
        
        # Включаем отладку Flask
        self.app.config['DEBUG'] = False
        
        self.setup_routes()
        
        # Создаем директорию для файлов
        os.makedirs(self.files_directory, exist_ok=True)
        print(f"Files directory: {os.path.abspath(self.files_directory)}")
        
        self.create_sample_files()
        
    def create_sample_files(self):
        """Создает тестовые файлы и регистрирует их в БД"""
        print(f"Creating sample files in: {self.files_directory}")
        
        sample_files = {
            f"document_{self.port}.txt": f"Тестовый документ с сервера {self.port}\nВремя создания: {time.ctime()}\nСервер ID: {self.server_id}\n\nЭто содержимое файла для демонстрации работы распределенной файловой системы.",
            f"data_{self.port}.csv": f"id,name,server,timestamp,description\n1,Test Data,{self.port},{time.time()},Sample data entry\n2,Example,{self.port},{time.time()},Another sample entry\n3,Demo,{self.port},{time.time()},Demo data for testing",
            f"config_{self.port}.json": f'{{"server_id": "{self.server_id}", "port": {self.port}, "created": "{time.ctime()}", "type": "file_server", "version": "1.0"}}',
            f"readme_{self.port}.md": f"""# Файловый сервер {self.port}

Это тестовый файл для лабораторной работы по распределенным системам.

## Информация о сервере
- Сервер ID: {self.server_id}
- Порт: {self.port}
- Время создания: {time.ctime()}

## Возможности
- Загрузка файлов
- Скачивание файлов
- Автоматическое обнаружение
- Отказоустойчивость

## Использование
Этот сервер является частью распределенной файловой системы.
"""
        }
        
        created_count = 0
        registered_count = 0
        
        for filename, content in sample_files.items():
            file_path = os.path.join(self.files_directory, filename)
            try:
                # Создаем файл если его нет
                if not os.path.exists(file_path):
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"✓ Создан тестовый файл: {filename}")
                    created_count += 1
                else:
                    print(f"- Файл уже существует: {filename}")
                
                # Регистрируем файл в базе данных
                file_size = os.path.getsize(file_path)
                file_hash = self.calculate_file_hash(file_path)
                
                file_info = FileInfo(
                    filename=filename,
                    file_size=file_size,
                    file_hash=file_hash,
                    server_id=self.server_id
                )
                # Добавляем путь к файлу для базы данных
                file_info.file_path = file_path
                
                self.db.register_file(file_info)
                print(f"✓ Файл {filename} зарегистрирован в БД")
                registered_count += 1
                
            except Exception as e:
                print(f"✗ Ошибка создания/регистрации файла {filename}: {e}")
        
        print(f"Создано {created_count} новых файлов, зарегистрировано {registered_count} файлов в БД")
        
        # Показываем итоговую статистику
        if os.path.exists(self.files_directory):
            actual_files = os.listdir(self.files_directory)
            print(f"Всего файлов в директории: {len(actual_files)}")
            total_size = 0
            for f in actual_files:
                file_path = os.path.join(self.files_directory, f)
                if os.path.isfile(file_path):
                    size = os.path.getsize(file_path)
                    total_size += size
                    print(f"  - {f} ({size} bytes)")
            print(f"Общий размер файлов: {total_size} bytes ({total_size/1024:.2f} KB)")
        
    def setup_routes(self):
        @self.app.route('/files', methods=['GET'])
        def get_files():
            try:
                # Сначала пытаемся получить файлы из базы данных
                files_from_db = self.db.get_files_by_server(self.server_id)
                
                if files_from_db:
                    print(f"Retrieved {len(files_from_db)} files from database")
                    return jsonify(files_from_db)
                else:
                    # Если в БД нет файлов, возвращаем локальные файлы
                    print("No files in database, scanning local files")
                    files = self.scan_files()
                    
                    # Попытаемся зарегистрировать файлы в БД
                    try:
                        for file in files:
                            file_info = FileInfo(
                                filename=file.filename,
                                file_size=file.file_size,
                                file_hash=file.file_hash,
                                server_id=self.server_id
                            )
                            file_info.file_path = os.path.join(self.files_directory, file.filename)
                            self.db.register_file(file_info)
                        print(f"Registered {len(files)} files in database")
                    except Exception as e:
                        print(f"Failed to register files in DB: {e}")
                    
                    return jsonify([{
                        'filename': f.filename,
                        'file_size': f.file_size,
                        'file_hash': f.file_hash,
                        'server_id': f.server_id
                    } for f in files])
                    
            except Exception as e:
                print(f"Error in get_files: {e}")
                # В случае ошибки БД, возвращаем локальные файлы
                files = self.scan_files()
                return jsonify([{
                    'filename': f.filename,
                    'file_size': f.file_size,
                    'file_hash': f.file_hash,
                    'server_id': f.server_id
                } for f in files])

        
        @self.app.route('/download/<filename>', methods=['GET'])
        def download_file(filename):
            transfer_id = None
            try:
                # Логируем начало передачи
                client_id = request.remote_addr or 'unknown'
                transfer_id = self.db.log_file_transfer(client_id, filename, self.server_id, 'in_progress')
                
                # Проверяем безопасность имени файла
                if '..' in filename or '/' in filename or '\\' in filename:
                    self.db.update_transfer_status(transfer_id, 'failed')
                    return jsonify({"error": "Invalid filename"}), 400
                
                file_path = os.path.join(self.files_directory, filename)
                print(f"Attempting to download: {file_path}")
                print(f"Files directory: {self.files_directory}")
                print(f"File exists: {os.path.exists(file_path)}")
                
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    print(f"Sending file: {filename}")
                    abs_path = os.path.abspath(file_path)
                    
                    # Обновляем статус передачи как завершенную
                    self.db.update_transfer_status(transfer_id, 'completed', datetime.now())
                    
                    return send_file(
                        abs_path, 
                        as_attachment=True, 
                        download_name=filename,
                        mimetype='application/octet-stream'
                    )
                else:
                    print(f"File not found: {file_path}")
                    self.db.update_transfer_status(transfer_id, 'failed')
                    
                    # Показываем доступные файлы для отладки
                    available_files = os.listdir(self.files_directory) if os.path.exists(self.files_directory) else []
                    print(f"Available files: {available_files}")
                    return jsonify({
                        "error": "File not found",
                        "requested": filename,
                        "available_files": available_files
                    }), 404
            except Exception as e:
                print(f"Download error: {e}")
                if transfer_id:
                    self.db.update_transfer_status(transfer_id, 'failed')
                import traceback
                traceback.print_exc()
                return jsonify({"error": f"Internal server error: {str(e)}"}), 500
        
        @self.app.route('/upload', methods=['POST'])
        def upload_file():
            try:
                if 'file' not in request.files:
                    return jsonify({"error": "No file provided"}), 400
                
                file = request.files['file']
                if file.filename == '':
                    return jsonify({"error": "No file selected"}), 400
                
                # Проверяем безопасность имени файла
                filename = file.filename
                if '..' in filename or '/' in filename or '\\' in filename:
                    return jsonify({"error": "Invalid filename"}), 400
                
                file_path = os.path.join(self.files_directory, filename)
                file.save(file_path)
                
                # Регистрируем новый файл в БД
                file_size = os.path.getsize(file_path)
                file_hash = self.calculate_file_hash(file_path)
                
                file_info = FileInfo(
                    filename=filename,
                    file_size=file_size,
                    file_hash=file_hash,
                    server_id=self.server_id
                )
                file_info.file_path = file_path
                
                self.db.register_file(file_info)
                
                print(f"File uploaded and registered: {filename}")
                return jsonify({
                    "status": "uploaded", 
                    "filename": filename,
                    "size": file_size,
                    "hash": file_hash
                })
            except Exception as e:
                print(f"Upload error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/status', methods=['GET'])
        def get_status():
            try:
                files = self.scan_files()
                files_in_db = self.db.get_files_by_server(self.server_id)
                
                return jsonify({
                    "server_id": self.server_id,
                    "port": self.port,
                    "files_count": len(files),
                    "files_in_db": len(files_in_db),
                    "total_size": sum(f.file_size for f in files),
                    "status": "active",
                    "files_directory": self.files_directory,
                    "uptime": "running"
                })
            except Exception as e:
                print(f"Status error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/list-files', methods=['GET'])
        def list_files_debug():
            """Отладочный endpoint для просмотра файлов"""
            try:
                if not os.path.exists(self.files_directory):
                    return jsonify({
                        "error": "Files directory does not exist", 
                        "directory": self.files_directory
                    })
                
                files_info = []
                for filename in os.listdir(self.files_directory):
                    file_path = os.path.join(self.files_directory, filename)
                    if os.path.isfile(file_path):
                        files_info.append({
                            "filename": filename,
                            "size": os.path.getsize(file_path),
                            "path": file_path,
                            "exists": os.path.exists(file_path),
                            "hash": self.calculate_file_hash(file_path)
                        })
                
                # Также получаем файлы из БД
                files_in_db = self.db.get_files_by_server(self.server_id)
                
                return jsonify({
                    "directory": self.files_directory,
                    "local_files": files_info,
                    "database_files": files_in_db,
                    "total_local_files": len(files_info),
                    "total_db_files": len(files_in_db)
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/transfers', methods=['GET'])
        def get_transfers():
            """Получает статистику передач файлов для этого сервера"""
            try:
                with self.db.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT filename, client_id, status, started_at, completed_at
                        FROM file_transfers 
                        WHERE server_id = %s
                        ORDER BY started_at DESC
                        LIMIT 50
                    """, (self.server_id,))
                    
                    transfers = []
                    for row in cursor.fetchall():
                        transfers.append({
                            "filename": row[0],
                            "client_id": row[1],
                            "status": row[2],
                            "started_at": row[3].isoformat() if row[3] else None,
                            "completed_at": row[4].isoformat() if row[4] else None
                        })
                    
                    return jsonify({
                        "server_id": self.server_id,
                        "transfers": transfers,
                        "total_transfers": len(transfers)
                    })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
            
        @self.app.route('/sync-db', methods=['POST'])
        def sync_database():
            """Принудительная синхронизация локальных файлов с БД"""
            try:
                files = self.scan_files()
                registered_count = 0
                
                for file in files:
                    try:
                        file_info = FileInfo(
                            filename=file.filename,
                            file_size=file.file_size,
                            file_hash=file.file_hash,
                            server_id=self.server_id
                        )
                        file_info.file_path = os.path.join(self.files_directory, file.filename)
                        self.db.register_file(file_info)
                        registered_count += 1
                    except Exception as e:
                        print(f"Failed to register {file.filename}: {e}")
                
                return jsonify({
                    "status": "success",
                    "local_files": len(files),
                    "registered_files": registered_count
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    
    def scan_files(self) -> List[FileInfo]:
        """Сканирует директорию и возвращает список файлов"""
        files = []
        if not os.path.exists(self.files_directory):
            return files
            
        for filename in os.listdir(self.files_directory):
            file_path = os.path.join(self.files_directory, filename)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                file_hash = self.calculate_file_hash(file_path)
                files.append(FileInfo(
                    filename=filename,
                    file_size=file_size,
                    file_hash=file_hash,
                    server_id=self.server_id
                ))
        return files
    
    def calculate_file_hash(self, file_path: str) -> str:
        """Вычисляет SHA-256 хеш файла"""
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            print(f"Error calculating hash for {file_path}: {e}")
            return "error"
    
    def register_with_discovery_server(self):
        """Регистрируется в Discovery Server через HTTP"""
        # Получаем локальный IP без маски подсети
        try:
            # Подключаемся к внешнему адресу для определения локального IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
        except:
            local_ip = "127.0.0.1"
        
        server_info = {
            "server_id": self.server_id,
            "ip_address": local_ip,
            "port": self.port,
            "service_type": "file_server"
        }
        
        try:
            response = requests.post(
                f"{self.discovery_server_url}/register",
                json=server_info,
                timeout=5
            )
            if response.status_code == 200:
                print(f"Registered with Discovery Server at {local_ip}:{self.port}")
                return True
            else:
                print(f"Registration failed: HTTP {response.status_code}")
                return False
        except requests.RequestException as e:
            print(f"Registration failed: {e}")
            return False
    
    def send_heartbeat(self):
        """Отправляет heartbeat через HTTP"""
        while True:
            try:
                response = requests.post(
                    f"{self.discovery_server_url}/heartbeat",
                    json={"server_id": self.server_id},
                    timeout=5
                )
                if response.status_code == 200:
                    print(f"Heartbeat sent successfully")
                else:
                    print(f"Heartbeat failed: HTTP {response.status_code}")
            except requests.RequestException as e:
                print(f"Heartbeat error: {e}")
            
            time.sleep(15)  # Heartbeat каждые 15 секунд
    
    def start_heartbeat(self):
        """Запускает heartbeat в отдельном потоке"""
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        print("Heartbeat thread started")
    
    def run(self):
        print("=" * 50)
        print(f"File Server Starting")
        print(f"Server ID: {self.server_id}")
        print(f"Port: {self.port}")
        print(f"Files Directory: {os.path.abspath(self.files_directory)}")
        print("=" * 50)
        
        # Регистрируемся в Discovery Server
        if self.register_with_discovery_server():
            print(f"✓ File server registered with Discovery Server")
            # Запускаем heartbeat
            self.start_heartbeat()
        else:
            print("✗ Failed to register with Discovery Server, but continuing...")
        
        # Показываем информацию о файлах
        files = self.scan_files()
        files_in_db = self.db.get_files_by_server(self.server_id)
        
        print(f"\nFile Statistics:")
        print(f"- Local files: {len(files)}")
        print(f"- Files in database: {len(files_in_db)}")
        print(f"- Total size: {sum(f.file_size for f in files)} bytes")
        
        print(f"\nFiles being served:")
        for file in files:
            print(f"  - {file.filename} ({file.file_size} bytes)")
        
        print(f"\nServer endpoints:")
        print(f"  - Files list: http://localhost:{self.port}/files")
        print(f"  - Server status: http://localhost:{self.port}/status")
        print(f"  - Debug files: http://localhost:{self.port}/list-files")
        print(f"  - Transfers log: http://localhost:{self.port}/transfers")
        print(f"  - Download: http://localhost:{self.port}/download/<filename>")
        
        print("=" * 50)
        print(f"File server running on port {self.port}")
        print("Press Ctrl+C to stop")
        print("=" * 50)
        
        # Запускаем Flask сервер
        self.app.run(host='0.0.0.0', port=self.port, threaded=True)

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    server = FileServer(port=port)
    try:
        server.run()
    except KeyboardInterrupt:
        print(f"\nFile server on port {port} stopped")
    except Exception as e:
        print(f"File server error: {e}")
