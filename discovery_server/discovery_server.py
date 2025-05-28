import asyncio
import websockets
import socket
import threading
import time
import sys
import os
import requests
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
        self.setup_routes()
        
        self.start_server_health_checker()
        
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
                return jsonify({"status": "registered", "server_id": server_info.server_id})
            except Exception as e:
                print(f"Registration error: {e}")
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
        
        @self.app.route('/debug', methods=['GET'])
        def debug():
            try:
                with self.db.connection.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM servers")
                    server_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM servers WHERE status = 'active'")
                    active_count = cursor.fetchone()[0]
                    
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
                
                return jsonify({
                    "discovery_server": {
                        "status": "running",
                        "port": self.port,
                        "uptime": "running"
                    },
                    "servers": {
                        "total": len(servers),
                        "active": len(active_servers),
                        "inactive": len(inactive_servers)
                    },
                    "services": {
                        "file_servers": len([s for s in active_servers if s['service_type'] == 'file_server']),
                        "discovery_servers": len([s for s in active_servers if s['service_type'] == 'discovery']),
                        "other_servers": len([s for s in active_servers if s['service_type'] not in ['file_server', 'discovery']])
                    }
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    
    def check_server_health(self, server):
        try:
            if server['service_type'] == 'file_server':
                url = f"http://{server['ip_address']}:{server['port']}/status"
                response = requests.get(url, timeout=3)
                return response.status_code == 200
            elif server['service_type'] == 'discovery':
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
                        print(f"✗ Marked server {server['server_id']} as inactive")
                    else:
                        print(f"✓ Server {server['server_id']} is healthy")
        
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
    
    async def websocket_handler(self, websocket, path):
        print(f"WebSocket connection from {websocket.remote_address}")
        try:
            async for message in websocket:
                try:
                    data = Protocol.parse_message(message)
                    print(f"WebSocket message: {data}")
                    
                    if data['type'] == MessageType.HEARTBEAT.value:
                        server_id = data['data']['server_id']
                        self.db.update_heartbeat(server_id)
                        await websocket.send(Protocol.create_message(
                            MessageType.HEARTBEAT, {"status": "ok"}
                        ))
                    elif data['type'] == MessageType.GET_SERVERS.value:
                        servers = self.db.get_active_servers()
                        await websocket.send(Protocol.create_message(
                            MessageType.GET_SERVERS, {"servers": servers}
                        ))
                except Exception as e:
                    print(f"WebSocket message error: {e}")
                    await websocket.send(Protocol.create_message(
                        MessageType.HEARTBEAT, {"status": "error", "message": str(e)}
                    ))
        except websockets.exceptions.ConnectionClosed:
            print(f"WebSocket connection closed: {websocket.remote_address}")
        except Exception as e:
            print(f"WebSocket error: {e}")
    
    def start_multicast_listener(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('', self.multicast_port))
            
            mreq = socket.inet_aton('224.1.1.1') + socket.inet_aton('0.0.0.0')
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            print(f"Multicast listener started on port {self.multicast_port}")
            
            while True:
                try:
                    data, addr = sock.recvfrom(1024)
                    message = Protocol.parse_message(data.decode())
                    
                    if message['type'] == MessageType.DISCOVERY_REQUEST.value:
                        print(f"Discovery request from {addr}")
                        response = Protocol.create_message(
                            MessageType.DISCOVERY_RESPONSE,
                            {
                                "discovery_server": f"http://{socket.gethostbyname(socket.gethostname())}:{self.port}",
                                "websocket": f"ws://{socket.gethostbyname(socket.gethostname())}:{self.ws_port}"
                            }
                        )
                        sock.sendto(response.encode(), addr)
                        print(f"Sent discovery response to {addr}")
                except Exception as e:
                    print(f"Multicast error: {e}")
        except Exception as e:
            print(f"Failed to start multicast listener: {e}")
    
    def run(self):
        print("=== Discovery Server Starting ===")
        print(f"HTTP API: http://localhost:{self.port}")
        print(f"WebSocket: ws://localhost:{self.ws_port}")
        print(f"Multicast: 224.1.1.1:{self.multicast_port}")
        print("=" * 40)
        
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
        print("  POST /register - Register new server")
        print("  POST /heartbeat - Send heartbeat")
        print("  GET  /debug - Debug information")
        print("  POST /cleanup - Cleanup inactive servers")
        print("  GET  /status - Discovery server status")
        print("=" * 40)
        
        self.app.run(host='0.0.0.0', port=self.port, debug=False, threaded=True)

if __name__ == "__main__":
    server = DiscoveryServer()
    try:
        server.run()
    except KeyboardInterrupt:
        print("\nDiscovery Server stopped")
    except Exception as e:
        print(f"Discovery Server error: {e}")
