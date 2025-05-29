import asyncio
import websockets
import socket
import threading
import time
import sys
import os
import requests
import json
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, request, jsonify
from shared.database import DatabaseManager
from shared.protocols import Protocol, MessageType, ServerInfo

class DiscoveryServer:
    def __init__(self, port=8000, ws_port=8001, multicast_port=8002):
        self.port = port
        self.ws_port = ws_port
        self.multicast_port = multicast_port
        self.db = DatabaseManager()
        self.app = Flask(__name__)
        self.connected_clients = {}
        self.setup_routes()
        
        self.start_server_health_checker()
        self.start_notification_monitor()
        
    def setup_routes(self):
        @self.app.route('/register', methods=['POST'])
        def register_server():
            try:
                data = request.json
                print(f"Registering server: {data}")
                
                server_info = ServerInfo(
                    server_id=data['server_id'],
                    ip_address=data['ip_address'],
                    port=data['port'],
                    service_type=data['service_type']
                )
                
                self.db.register_server(server_info)
                print(f"Server {server_info.server_id} registered successfully")
                
                if server_info.service_type == 'file_server':
                    self.notify_clients_about_server_changes()
                
                return jsonify({"status": "registered", "server_id": server_info.server_id})
            except Exception as e:
                print(f"Registration error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/register_client', methods=['POST'])
        def register_client():
            try:
                data = request.json
                print(f"Registering client: {data}")
                
                client_info = {
                    "client_id": data['client_id'],
                    "ip_address": data['ip_address'],
                    "port": data['port'],
                    "service_type": "client",
                    "status": "active"
                }
                
                with self.db.connection.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO servers (server_id, ip_address, port, service_type, status, last_heartbeat)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (server_id) 
                        DO UPDATE SET 
                            ip_address = EXCLUDED.ip_address,
                            port = EXCLUDED.port,
                            last_heartbeat = CURRENT_TIMESTAMP,
                            status = 'active'
                    """, (client_info['client_id'], client_info['ip_address'], 
                          client_info['port'], client_info['service_type'], client_info['status']))
                
                self.notify_clients_about_new_peer(client_info)
                
                return jsonify({"status": "registered", "client_id": client_info['client_id']})
            except Exception as e:
                print(f"Client registration error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/servers', methods=['GET'])
        def get_servers():
            try:
                servers = self.db.get_active_servers()
                print(f"Returning {len(servers)} servers")
                return jsonify(servers)
            except Exception as e:
                print(f"Error getting servers: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/clients', methods=['GET'])
        def get_clients():
            try:
                clients = self.get_active_clients()
                return jsonify(clients)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/heartbeat', methods=['POST'])
        def heartbeat():
            try:
                server_id = request.json.get('server_id')
                self.db.update_heartbeat(server_id)
                print(f"Heartbeat received from {server_id}")
                return jsonify({"status": "ok"})
            except Exception as e:
                print(f"Heartbeat error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/file_uploaded', methods=['POST'])
        def file_uploaded():
            try:
                data = request.json
                client_id = data.get('client_id')
                filename = data.get('filename')
                file_size = data.get('file_size')
                
                print(f"File upload notification: {filename} from {client_id}")
                
                self.notify_clients_about_file_upload(client_id, filename, file_size)
                
                return jsonify({"status": "notified"})
            except Exception as e:
                print(f"File upload notification error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/file_received', methods=['POST'])
        def file_received():
            try:
                data = request.json
                client_id = data.get('client_id')
                filename = data.get('filename')
                
                print(f"File received notification: {filename} by {client_id}")
                
                return jsonify({"status": "acknowledged"})
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/debug', methods=['GET'])
        def debug():
            try:
                with self.db.connection.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM servers")
                    server_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM servers WHERE status = 'active'")
                    active_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM servers WHERE service_type = 'client'")
                    client_count = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT server_id, host(ip_address) as ip_address, port, service_type, status, 
                               EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_heartbeat)) as seconds_since_heartbeat
                        FROM servers 
                        ORDER BY created_at DESC 
                        LIMIT 10
                    """)
                    recent_servers = [dict(zip([desc[0] for desc in cursor.description], row)) 
                                    for row in cursor.fetchall()]
                
                return jsonify({
                    "total_servers": server_count,
                    "active_servers": active_count,
                    "active_clients": client_count,
                    "recent_servers": recent_servers,
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/cleanup', methods=['POST'])
        def cleanup_inactive_servers():
            try:
                updated_count = self.cleanup_dead_servers()
                return jsonify({
                    "status": "success", 
                    "updated_servers": updated_count,
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                print(f"Cleanup error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/status', methods=['GET'])
        def get_discovery_status():
            try:
                servers = self.db.get_all_servers()
                active_servers = [s for s in servers if s['status'] == 'active']
                inactive_servers = [s for s in servers if s['status'] == 'inactive']
                
                file_servers = [s for s in active_servers if s['service_type'] == 'file_server']
                clients = [s for s in active_servers if s['service_type'] == 'client']
                
                return jsonify({
                    "discovery_server": {
                        "status": "running",
                        "port": self.port,
                        "websocket_port": self.ws_port,
                        "multicast_port": self.multicast_port,
                        "uptime": "running"
                    },
                    "servers": {
                        "total": len(servers),
                        "active": len(active_servers),
                        "inactive": len(inactive_servers)
                    },
                    "services": {
                        "file_servers": len(file_servers),
                        "clients": len(clients),
                        "discovery_servers": 1
                    }
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    
    def get_active_clients(self):
        try:
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT server_id as client_id, host(ip_address) as ip_address, port
                    FROM servers 
                    WHERE service_type = 'client' 
                    AND status = 'active'
                    AND last_heartbeat > CURRENT_TIMESTAMP - INTERVAL '60 seconds'
                """)
                
                clients = []
                for row in cursor.fetchall():
                    clients.append({
                        'client_id': row[0],
                        'ip_address': row[1],
                        'port': row[2]
                    })
                return clients
        except Exception as e:
            print(f"Error getting active clients: {e}")
            return []
    
    def notify_clients_about_new_peer(self, new_client_info):
        active_clients = self.get_active_clients()
        
        notification = {
            "type": "new_peer",
            "peer_info": new_client_info,
            "timestamp": time.time()
        }
        
        for client in active_clients:
            if client['client_id'] != new_client_info['client_id']:
                self.send_notification_to_client(client, notification)
    
    def notify_clients_about_file_upload(self, uploader_id, filename, file_size):
        active_clients = self.get_active_clients()
        
        notification = {
            "type": "file_uploaded",
            "uploader_id": uploader_id,
            "filename": filename,
            "file_size": file_size,
            "timestamp": time.time()
        }
        
        for client in active_clients:
            if client['client_id'] != uploader_id:
                self.send_notification_to_client(client, notification)
    
    def notify_clients_about_server_changes(self):
        active_clients = self.get_active_clients()
        active_servers = self.db.get_active_servers()
        
        file_servers = [s for s in active_servers if s['service_type'] == 'file_server']
        
        notification = {
            "type": "servers_updated",
            "servers": file_servers,
            "count": len(file_servers),
            "timestamp": time.time()
        }
        
        for client in active_clients:
            self.send_notification_to_client(client, notification)
    
    def send_notification_to_client(self, client, notification):
        try:
            url = f"http://{client['ip_address']}:{client['port']}/notification"
            response = requests.post(url, json=notification, timeout=3)
            
            if response.status_code == 200:
                print(f"Notification sent to client {client['client_id'][:8]}...")
            else:
                print(f"Failed to notify client {client['client_id'][:8]}...: {response.status_code}")
        except Exception as e:
            print(f"Error notifying client {client['client_id'][:8]}...: {e}")
    
    def check_server_health(self, server):
        try:
            if server['service_type'] == 'file_server':
                url = f"http://{server['ip_address']}:{server['port']}/status"
                response = requests.get(url, timeout=3)
                return response.status_code == 200
            elif server['service_type'] == 'client':
                url = f"http://{server['ip_address']}:{server['port']}/status"
                response = requests.get(url, timeout=3)
                return response.status_code == 200
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                result = sock.connect_ex((server['ip_address'], server['port']))
                sock.close()
                return result == 0
        except Exception as e:
            print(f"Health check failed for {server['server_id']}: {e}")
            return False
    
    def cleanup_dead_servers(self):
        servers = self.db.get_all_servers()
        updated_count = 0
        
        print("Starting server health check...")
        
        for server in servers:
            if server['status'] == 'active':
                last_heartbeat = server.get('last_heartbeat')
                should_check_health = False
                
                if last_heartbeat:
                    if isinstance(last_heartbeat, str):
                        try:
                            last_heartbeat = datetime.fromisoformat(last_heartbeat.replace('Z', '+00:00'))
                        except:
                            should_check_health = True
                    
                    if isinstance(last_heartbeat, datetime):
                        heartbeat_age = datetime.now() - last_heartbeat.replace(tzinfo=None)
                        if heartbeat_age > timedelta(seconds=60):
                            should_check_health = True
                            print(f"Server {server['server_id']} has old heartbeat ({heartbeat_age})")
                else:
                    should_check_health = True
                    print(f"Server {server['server_id']} has no heartbeat")
                
                if should_check_health:
                    print(f"Checking health of {server['server_id']} ({server['ip_address']}:{server['port']})")
                    if not self.check_server_health(server):
                        self.db.mark_server_inactive(server['server_id'])
                        updated_count += 1
                        print(f"Marked server {server['server_id']} as inactive")
                        
                        if server['service_type'] == 'file_server':
                            self.notify_clients_about_server_changes()
                    else:
                        print(f"Server {server['server_id']} is healthy")
        
        print(f"Health check completed. Updated {updated_count} servers.")
        return updated_count
    
    def start_server_health_checker(self):
        def health_check_worker():
            print("Server health checker started")
            while True:
                try:
                    time.sleep(30)
                    updated = self.cleanup_dead_servers()
                    if updated > 0:
                        print(f"Health checker: deactivated {updated} servers")
                except Exception as e:
                    print(f"Health check error: {e}")
                    time.sleep(30)
        
        health_thread = threading.Thread(target=health_check_worker)
        health_thread.daemon = True
        health_thread.start()
    
    def start_notification_monitor(self):
        def monitor_changes():
            last_server_count = 0
            last_client_count = 0
            
            while True:
                try:
                    current_servers = self.db.get_active_servers()
                    file_servers = [s for s in current_servers if s['service_type'] == 'file_server']
                    clients = [s for s in current_servers if s['service_type'] == 'client']
                    
                    current_server_count = len(file_servers)
                    current_client_count = len(clients)
                    
                    if current_server_count != last_server_count:
                        print(f"File server count changed: {last_server_count} -> {current_server_count}")
                        self.notify_clients_about_server_changes()
                        last_server_count = current_server_count
                    
                    if current_client_count != last_client_count:
                        print(f"Client count changed: {last_client_count} -> {current_client_count}")
                        last_client_count = current_client_count
                    
                    time.sleep(10)
                except Exception as e:
                    print(f"Notification monitor error: {e}")
                    time.sleep(10)
        
        monitor_thread = threading.Thread(target=monitor_changes)
        monitor_thread.daemon = True
        monitor_thread.start()
        print("Notification monitor started")
    
    async def websocket_handler(self, websocket, path):
        client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        print(f"WebSocket connection from {client_id}")
        
        try:
            welcome_msg = {
                "type": "welcome",
                "message": "Connected to Discovery Server",
                "timestamp": time.time()
            }
            await websocket.send(json.dumps(welcome_msg))
            
            servers = self.db.get_active_servers()
            servers_msg = {
                "type": "servers_list",
                "servers": servers,
                "count": len(servers),
                "timestamp": time.time()
            }
            await websocket.send(json.dumps(servers_msg))
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    msg_type = data.get('type')
                    print(f"WebSocket message from {client_id}: {msg_type}")
                    
                    if msg_type == 'heartbeat':
                        server_id = data.get('server_id')
                        if server_id:
                            self.db.update_heartbeat(server_id)
                            response = {
                                "type": "heartbeat_ack",
                                "server_id": server_id,
                                "status": "ok",
                                "timestamp": time.time()
                            }
                            await websocket.send(json.dumps(response))
                    
                    elif msg_type == 'get_servers':
                        servers = self.db.get_active_servers()
                        response = {
                            "type": "servers_list",
                            "servers": servers,
                            "count": len(servers),
                            "timestamp": time.time()
                        }
                        await websocket.send(json.dumps(response))
                    
                    elif msg_type == 'subscribe_updates':
                        response = {
                            "type": "subscription_confirmed",
                            "message": "Subscribed to server updates",
                            "timestamp": time.time()
                        }
                        await websocket.send(json.dumps(response))
                        
                except json.JSONDecodeError:
                    error_msg = {
                        "type": "error",
                        "message": "Invalid JSON format",
                        "timestamp": time.time()
                    }
                    await websocket.send(json.dumps(error_msg))
                    
        except websockets.exceptions.ConnectionClosed:
            print(f"WebSocket connection closed: {client_id}")
        except Exception as e:
            print(f"WebSocket error for {client_id}: {e}")
    
    def start_multicast_listener(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('', self.multicast_port))
            
            mreq = socket.inet_aton('224.1.1.1') + socket.inet_aton('0.0.0.0')
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            print(f"UDP Multicast listener started on 224.1.1.1:{self.multicast_port}")
            
            while True:
                try:
                    data, addr = sock.recvfrom(1024)
                    message = json.loads(data.decode())
                    
                    if message.get('type') == 'discovery_request':
                        print(f"Discovery request from {addr}")
                        response = {
                            "type": "discovery_response",
                            "discovery_server": f"http://{socket.gethostbyname(socket.gethostname())}:{self.port}",
                            "websocket": f"ws://{socket.gethostbyname(socket.gethostname())}:{self.ws_port}",
                            "timestamp": time.time()
                        }
                        sock.sendto(json.dumps(response).encode(), addr)
                        print(f"Sent discovery response to {addr}")
                except Exception as e:
                    print(f"Multicast error: {e}")
        except Exception as e:
            print(f"Failed to start multicast listener: {e}")
    
    def run(self):
        print("=" * 60)
        print("DISCOVERY SERVER STARTING")
        print("=" * 60)
        print(f"HTTP API: http://localhost:{self.port}")
        print(f"WebSocket: ws://localhost:{self.ws_port}")
        print(f"UDP Multicast: 224.1.1.1:{self.multicast_port}")
        print("=" * 60)
        
        multicast_thread = threading.Thread(target=self.start_multicast_listener)
        multicast_thread.daemon = True
        multicast_thread.start()
        
        def start_websocket():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                ws_server = websockets.serve(self.websocket_handler, "0.0.0.0", self.ws_port)
                loop.run_until_complete(ws_server)
                loop.run_forever()
            except Exception as e:
                print(f"WebSocket server error: {e}")
        
        ws_thread = threading.Thread(target=start_websocket)
        ws_thread.daemon = True
        ws_thread.start()
        print(f"WebSocket server started on port {self.ws_port}")
        
        try:
            servers = self.db.get_all_servers()
            print(f"Database contains {len(servers)} servers")
            active_servers = [s for s in servers if s['status'] == 'active']
            print(f"Active servers: {len(active_servers)}")
        except Exception as e:
            print(f"Database check error: {e}")
        
        print(f"Starting HTTP server on port {self.port}")
        print("Available endpoints:")
        print("  GET  /servers - List active servers")
        print("  GET  /clients - List active clients")
        print("  POST /register - Register new server")
        print("  POST /register_client - Register new client")
        print("  POST /heartbeat - Send heartbeat")
        print("  POST /file_uploaded - Notify file upload")
        print("  GET  /debug - Debug information")
        print("  POST /cleanup - Cleanup inactive servers")
        print("  GET  /status - Discovery server status")
        print("=" * 60)
        
        self.app.run(host='0.0.0.0', port=self.port, debug=False, threaded=True)

if __name__ == "__main__":
    server = DiscoveryServer()
    try:
        server.run()
    except KeyboardInterrupt:
        print("\nDiscovery Server stopped")
    except Exception as e:
        print(f"Discovery Server error: {e}")
        import traceback
        traceback.print_exc()
