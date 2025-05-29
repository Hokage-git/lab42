import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime

class DatabaseManager:
    def __init__(self):
        self.connection = psycopg2.connect(
            host='localhost',
            database='distributed_files',
            user='postgres',
            password='123',
            port=5432
        )
        self.connection.autocommit = True
        self._create_tables()
    
    def _create_tables(self):
        """Создает необходимые таблицы"""
        with self.connection.cursor() as cursor:
            # Создание таблицы серверов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    id SERIAL PRIMARY KEY,
                    server_id VARCHAR(50) UNIQUE NOT NULL,
                    ip_address INET NOT NULL,
                    port INTEGER NOT NULL,
                    service_type VARCHAR(20) NOT NULL,
                    status VARCHAR(10) DEFAULT 'active',
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Создание таблицы файлов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id SERIAL PRIMARY KEY,
                    filename VARCHAR(255) NOT NULL,
                    file_path TEXT NOT NULL,
                    file_size BIGINT NOT NULL,
                    server_id VARCHAR(50),
                    file_hash VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Создание таблицы передач файлов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_transfers (
                    id SERIAL PRIMARY KEY,
                    client_id VARCHAR(50) NOT NULL,
                    filename VARCHAR(255) NOT NULL,
                    server_id VARCHAR(50) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """)
            
            # Создание индексов
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_servers_status ON servers(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_server_id ON files(server_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transfers_status ON file_transfers(status)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_files_unique ON files(filename, server_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transfers_client_file ON file_transfers(client_id, filename)")
    
    def register_server(self, server_info):
        """Регистрирует сервер в базе данных"""
        print(f"Registering server in DB: {server_info.server_id}")
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO servers (server_id, ip_address, port, service_type, status, last_heartbeat)
                VALUES (%s, %s, %s, %s, 'active', CURRENT_TIMESTAMP)
                ON CONFLICT (server_id) 
                DO UPDATE SET 
                    ip_address = EXCLUDED.ip_address,
                    port = EXCLUDED.port,
                    service_type = EXCLUDED.service_type,
                    last_heartbeat = CURRENT_TIMESTAMP,
                    status = 'active'
            """, (server_info.server_id, server_info.ip_address, 
                  server_info.port, server_info.service_type))
        print(f"Server {server_info.server_id} registered in database")
    
    def get_active_servers(self):
        """Получает только активные серверы с проверкой heartbeat"""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    server_id, 
                    host(ip_address) as ip_address, 
                    port, 
                    service_type, 
                    status, 
                    last_heartbeat,
                    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_heartbeat)) as seconds_since_heartbeat
                FROM servers 
                WHERE status = 'active'
                AND last_heartbeat > CURRENT_TIMESTAMP - INTERVAL '60 seconds'
                ORDER BY service_type, port
            """)
            servers = [dict(row) for row in cursor.fetchall()]
            print(f"Retrieved {len(servers)} active servers (heartbeat < 60s)")
            for server in servers:
                print(f"Server: {server['server_id']} at {server['ip_address']}:{server['port']}")
            return servers
    
    def get_all_servers(self):
        """Получает все серверы (включая неактивные)"""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    server_id, 
                    host(ip_address) as ip_address, 
                    port, 
                    service_type, 
                    status, 
                    last_heartbeat
                FROM servers 
                ORDER BY service_type, port
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def mark_server_inactive(self, server_id):
        """Помечает сервер как неактивный"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                UPDATE servers 
                SET status = 'inactive' 
                WHERE server_id = %s
            """, (server_id,))
            print(f"Server {server_id} marked as inactive in database")
    
    def update_heartbeat(self, server_id):
        """Обновляет время последнего heartbeat сервера"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                UPDATE servers 
                SET last_heartbeat = CURRENT_TIMESTAMP 
                WHERE server_id = %s
            """, (server_id,))
    
    def register_file(self, file_info):
        """Регистрирует файл в базе данных"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO files (filename, file_path, file_size, server_id, file_hash)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (filename, server_id) 
                DO UPDATE SET 
                    file_path = EXCLUDED.file_path,
                    file_size = EXCLUDED.file_size,
                    file_hash = EXCLUDED.file_hash,
                    updated_at = CURRENT_TIMESTAMP
            """, (file_info.filename, file_info.file_path, 
                  file_info.file_size, file_info.server_id, file_info.file_hash))
            print(f"File {file_info.filename} registered in database")

    def remove_file(self, filename, server_id):
        """Удаляет файл из базы данных"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM files 
                WHERE filename = %s AND server_id = %s
            """, (filename, server_id))

    def get_files_by_server(self, server_id):
        """Получает файлы конкретного сервера"""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT filename, file_path, file_size, file_hash, server_id
                FROM files 
                WHERE server_id = %s
                ORDER BY filename
            """, (server_id,))
            return [dict(row) for row in cursor.fetchall()]

    def log_file_transfer(self, client_id, filename, server_id, status='pending'):
        """Логирует передачу файла"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO file_transfers (client_id, filename, server_id, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (client_id, filename, server_id, status))
            transfer_id = cursor.fetchone()[0]
            print(f"Logged file transfer: {filename} to {client_id} (ID: {transfer_id})")
            return transfer_id

    def update_transfer_status(self, transfer_id, status, completed_at=None):
        """Обновляет статус передачи файла"""
        with self.connection.cursor() as cursor:
            if completed_at:
                cursor.execute("""
                    UPDATE file_transfers 
                    SET status = %s, completed_at = %s
                    WHERE id = %s
                """, (status, completed_at, transfer_id))
            else:
                cursor.execute("""
                    UPDATE file_transfers 
                    SET status = %s
                    WHERE id = %s
                """, (status, transfer_id))
            print(f"Updated transfer {transfer_id} status to {status}")

    def get_transfer_statistics(self):
        """Получает статистику передач файлов"""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration
                FROM file_transfers 
                WHERE completed_at IS NOT NULL
                GROUP BY status
                ORDER BY status
            """)
            return [dict(row) for row in cursor.fetchall()]

    def cleanup_old_transfers(self, days=7):
        """Очищает старые записи передач файлов"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM file_transfers 
                WHERE started_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
                AND status IN ('completed', 'failed')
            """, (days,))
            deleted_count = cursor.rowcount
            print(f"Cleaned up {deleted_count} old transfer records")
            return deleted_count
