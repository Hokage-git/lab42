# client/client.py
import requests
import socket
import os
import sys
import uuid
import json
import time
import threading
from flask import Flask, request, jsonify
from typing import List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.protocols import Protocol, MessageType, ServerInfo, FileInfo

class FileManagerClient:
    def __init__(self):
        print("=== Distributed File Manager Client ===")
        print("Network Configuration:")
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            self.local_ip = s.getsockname()[0]
            s.close()
            print(f"Your IP address: {self.local_ip}")
        except:
            self.local_ip = "127.0.0.1"
            print("Could not determine local IP")
        
        discovery_ip = input("Enter Discovery Server IP (or press Enter for localhost): ").strip()
        if discovery_ip:
            self.discovery_server_url = f"http://{discovery_ip}:8000"
        else:
            self.discovery_server_url = "http://localhost:8000"
        
        self.available_servers = []
        self.client_id = str(uuid.uuid4())
        self.client_port = self.find_free_port()
        self.pending_notifications = []
        self.auto_request_files = True
        
        print(f"Client ID: {self.client_id}")
        print(f"Client Port: {self.client_port}")
        print(f"Discovery Server: {self.discovery_server_url}")
        
        self.app = Flask(__name__)
        self.setup_client_routes()
        self.start_client_server()
        
        self.test_discovery_connection()
        self.register_with_discovery_server()
        
    def find_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port
    
    def setup_client_routes(self):
        @self.app.route('/notification', methods=['POST'])
        def handle_notification():
            try:
                notification = request.json
                notification_type = notification.get('type')
                
                print(f"\nReceived notification: {notification_type}")
                
                if notification_type == 'new_peer':
                    peer_info = notification.get('peer_info')
                    print(f"New client joined: {peer_info['client_id'][:8]}... at {peer_info['ip_address']}:{peer_info['port']}")
                    
                elif notification_type == 'file_uploaded':
                    uploader_id = notification.get('uploader_id')
                    filename = notification.get('filename')
                    file_size = notification.get('file_size')
                    
                    print(f"New file available: {filename} ({file_size} bytes) from client {uploader_id[:8]}...")
                    
                    if self.auto_request_files:
                        self.auto_request_new_file(uploader_id, filename)
                    else:
                        self.pending_notifications.append(notification)
                
                elif notification_type == 'servers_updated':
                    servers = notification.get('servers', [])
                    print(f"Server list updated: {len(servers)} file servers available")
                
                return jsonify({"status": "received"})
            except Exception as e:
                print(f"Error handling notification: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/receive_file', methods=['POST'])
        def receive_file():
            try:
                data = request.json
                sender_id = data.get('sender_id')
                filename = data.get('filename')
                file_content = data.get('file_content')
                
                print(f"Receiving file {filename} from client {sender_id}")
                
                os.makedirs("./received_files", exist_ok=True)
                file_path = os.path.join("./received_files", filename)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(file_content)
                
                print(f"File {filename} saved to {file_path}")
                
                return jsonify({
                    "status": "received",
                    "filename": filename,
                    "saved_to": file_path
                })
            except Exception as e:
                print(f"Error receiving file: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/file_request', methods=['POST'])
        def handle_file_request():
            try:
                data = request.json
                requester_id = data.get('requester_id')
                filename = data.get('filename')
                
                print(f"File request for {filename} from client {requester_id}")
                
                file_path = os.path.join("./downloads", filename)
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        file_content = f.read()
                    
                    return jsonify({
                        "status": "found",
                        "filename": filename,
                        "file_content": file_content
                    })
                else:
                    return jsonify({"status": "not_found"}), 404
                    
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/status', methods=['GET'])
        def client_status():
            return jsonify({
                "client_id": self.client_id,
                "status": "active",
                "ip_address": self.local_ip,
                "port": self.client_port
            })
    
    def start_client_server(self):
        def run_server():
            self.app.run(host='0.0.0.0', port=self.client_port, threaded=True, debug=False)
        
        server_thread = threading.Thread(target=run_server)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(1)
        print(f"Client server started on port {self.client_port}")
    
    def register_with_discovery_server(self):
        try:
            client_info = {
                "client_id": self.client_id,
                "ip_address": self.local_ip,
                "port": self.client_port
            }
            
            response = requests.post(
                f"{self.discovery_server_url}/register_client",
                json=client_info,
                timeout=5
            )
            
            if response.status_code == 200:
                print("Client registered with Discovery Server")
                self.start_heartbeat()
            else:
                print(f"Client registration failed: {response.status_code}")
        except Exception as e:
            print(f"Client registration error: {e}")
    
    def start_heartbeat(self):
        def send_heartbeat():
            while True:
                try:
                    requests.post(
                        f"{self.discovery_server_url}/heartbeat",
                        json={"server_id": self.client_id},
                        timeout=5
                    )
                except:
                    pass
                time.sleep(15)
        
        heartbeat_thread = threading.Thread(target=send_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
    
    def get_available_clients(self):
        try:
            response = requests.get(f"{self.discovery_server_url}/clients", timeout=5)
            if response.status_code == 200:
                clients = response.json()
                return [c for c in clients if c['client_id'] != self.client_id]
            return []
        except Exception as e:
            print(f"Error getting clients: {e}")
            return []
    
    def auto_request_new_file(self, uploader_id, filename):
        try:
            clients = self.get_available_clients()
            uploader_client = next((c for c in clients if c['client_id'] == uploader_id), None)
            
            if uploader_client:
                print(f"Auto-requesting file {filename} from {uploader_id[:8]}...")
                success = self.request_file_from_client(uploader_client, filename)
                
                if success:
                    print(f"Successfully received {filename}")
                else:
                    print(f"Failed to receive {filename}")
        except Exception as e:
            print(f"Auto-request error: {e}")
    
    def send_file_to_client(self, target_client, filename, file_content):
        try:
            url = f"http://{target_client['ip_address']}:{target_client['port']}/receive_file"
            
            data = {
                "sender_id": self.client_id,
                "filename": filename,
                "file_content": file_content
            }
            
            response = requests.post(url, json=data, timeout=10)
            
            if response.status_code == 200:
                print(f"File {filename} sent successfully to client {target_client['client_id']}")
                return True
            else:
                print(f"Failed to send file: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Error sending file to client: {e}")
            return False
    
    def request_file_from_client(self, target_client, filename):
        try:
            url = f"http://{target_client['ip_address']}:{target_client['port']}/file_request"
            
            data = {
                "requester_id": self.client_id,
                "filename": filename
            }
            
            response = requests.post(url, json=data, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result['status'] == 'found':
                    os.makedirs("./received_files", exist_ok=True)
                    file_path = os.path.join("./received_files", filename)
                    
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(result['file_content'])
                    
                    print(f"File {filename} received from client {target_client['client_id']}")
                    return True
                else:
                    print(f"File {filename} not found on client {target_client['client_id']}")
                    return False
            else:
                print(f"Request failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Error requesting file from client: {e}")
            return False
    
    def test_discovery_connection(self):
        try:
            response = requests.get(f"{self.discovery_server_url}/status", timeout=5)
            if response.status_code == 200:
                print("Discovery Server connection successful")
                return True
            else:
                print(f"Discovery Server returned HTTP {response.status_code}")
        except requests.RequestException as e:
            print(f"Cannot connect to Discovery Server: {e}")
            print("Trying UDP Multicast discovery...")
            return self.discover_via_udp_multicast()
        return False
        
    def discover_via_udp_multicast(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            
            discovery_request = {
                "type": "discovery_request",
                "service": "client",
                "client_id": self.client_id,
                "timestamp": time.time()
            }
            
            multicast_addr = ('224.1.1.1', 8002)
            sock.sendto(json.dumps(discovery_request).encode(), multicast_addr)
            print(f"UDP Multicast discovery sent to {multicast_addr}")
            
            try:
                data, addr = sock.recvfrom(1024)
                response = json.loads(data.decode())
                
                if response.get('type') == 'discovery_response':
                    self.discovery_server_url = response.get('discovery_server')
                    print(f"Discovery Server found via UDP: {self.discovery_server_url}")
                    return True
            except socket.timeout:
                print("UDP Multicast discovery timeout")
                
            sock.close()
            return False
            
        except Exception as e:
            print(f"UDP Multicast discovery error: {e}")
            return False
    
    def get_available_servers(self) -> List[dict]:
        try:
            response = requests.get(f"{self.discovery_server_url}/servers", timeout=10)
            if response.status_code == 200:
                servers_data = response.json()
                
                self.available_servers = []
                for server in servers_data:
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
            if self.discover_via_udp_multicast():
                return self.get_available_servers()
        
        return []
    
    def get_files_from_server(self, server: dict) -> List[dict]:
        try:
            url = f"http://{server['ip_address']}:{server['port']}/files"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                files_data = response.json()
                for file in files_data:
                    file['server_info'] = server
                return files_data
            return []
        except requests.RequestException as e:
            print(f"Error getting files from server: {e}")
            return []
    
    def get_all_files(self) -> List[dict]:
        all_files = []
        servers = self.get_available_servers()
        
        print(f"Found {len(servers)} servers")
        
        if not servers:
            print("No servers available. Check network connection and Discovery Server.")
            return []
        
        processed_servers = set()
        
        for server in servers:
            server_key = f"{server['server_id']}:{server['port']}"
            
            if server['service_type'] == 'file_server' and server_key not in processed_servers:
                print(f"Getting files from server {server['server_id']} at {server['ip_address']}:{server['port']}")
                files = self.get_files_from_server(server)
                
                if files:
                    print(f"Got {len(files)} files from server {server['ip_address']}:{server['port']}")
                    all_files.extend(files)
                else:
                    print(f"No files received from server {server['ip_address']}:{server['port']}")
                
                processed_servers.add(server_key)
        
        print(f"Total files collected: {len(all_files)}")
        return all_files
    
    def download_file(self, filename: str, server_info: dict, save_path: str = "./downloads"):
        try:
            if not server_info or 'ip_address' not in server_info or 'port' not in server_info:
                print(f"Invalid server info: {server_info}")
                return False
                
            url = f"http://{server_info['ip_address']}:{server_info['port']}/download/{filename}"
            print(f"Downloading from: {url}")
            
            headers = {'X-Client-ID': self.client_id}
            response = requests.get(url, stream=True, timeout=30, headers=headers)
            
            if response.status_code == 200:
                os.makedirs(save_path, exist_ok=True)
                file_path = os.path.join(save_path, filename)
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                progress = (downloaded / total_size) * 100
                                print(f"\rDownloading: {progress:.1f}%", end='', flush=True)
                
                print(f"\nFile {filename} downloaded successfully to {file_path}")
                print(f"File size: {downloaded} bytes")
                
                # Уведомляем Discovery Server о загрузке файла
                self.notify_discovery_about_file_upload(filename, downloaded)
                
                return True
            else:
                print(f"Failed to download {filename}: HTTP {response.status_code}")
                return False
                
        except requests.RequestException as e:
            print(f"Error downloading file: {e}")
            return False
    
    def notify_discovery_about_file_upload(self, filename, file_size):
        try:
            data = {
                "client_id": self.client_id,
                "filename": filename,
                "file_size": file_size
            }
            
            response = requests.post(
                f"{self.discovery_server_url}/file_uploaded",
                json=data,
                timeout=5
            )
            
            if response.status_code == 200:
                print("Discovery Server notified about file upload")
        except Exception as e:
            print(f"Error notifying Discovery Server: {e}")
    
    def run_interactive(self):
        print("\n" + "="*50)
        print("DISTRIBUTED FILE MANAGER CLIENT WITH P2P")
        print("="*50)
        
        while True:
            print("\nOptions:")
            print("1. List all files from servers")
            print("2. Download file from server")
            print("3. Show available clients")
            print("4. Request file from another client")
            print("5. Show my received files")
            print("6. Show pending notifications")
            print("7. Toggle auto-request files")
            print("8. Exit")
            
            choice = input("\nChoose option: ").strip()
            
            if choice == "1":
                files = self.get_all_files()
                if files:
                    print(f"\nFound {len(files)} files:")
                    for i, file in enumerate(files, 1):
                        server_info = file.get('server_info', {})
                        server_addr = f"{server_info.get('ip_address')}:{server_info.get('port')}"
                        print(f"{i}. {file.get('filename')} ({file.get('file_size')} bytes) - {server_addr}")
                else:
                    print("No files available")
            
            elif choice == "2":
                files = self.get_all_files()
                if not files:
                    print("No files available")
                    continue
                
                print("\nAvailable files:")
                for i, file in enumerate(files, 1):
                    print(f"{i}. {file.get('filename')}")
                
                try:
                    file_index = int(input("Enter file number: ")) - 1
                    if 0 <= file_index < len(files):
                        selected_file = files[file_index]
                        server_info = selected_file.get('server_info')
                        
                        if server_info:
                            filename = selected_file.get('filename')
                            self.download_file(filename, server_info)
                except ValueError:
                    print("Invalid input")
            
            elif choice == "3":
                clients = self.get_available_clients()
                if clients:
                    print(f"\nAvailable clients ({len(clients)}):")
                    for i, client in enumerate(clients, 1):
                        print(f"{i}. {client['client_id'][:8]}... at {client['ip_address']}:{client['port']}")
                else:
                    print("No other clients available")
            
            elif choice == "4":
                clients = self.get_available_clients()
                if not clients:
                    print("No other clients available")
                    continue
                
                print("\nAvailable clients:")
                for i, client in enumerate(clients, 1):
                    print(f"{i}. {client['client_id'][:8]}... at {client['ip_address']}:{client['port']}")
                
                try:
                    client_index = int(input("Enter client number: ")) - 1
                    if 0 <= client_index < len(clients):
                        target_client = clients[client_index]
                        filename = input("Enter filename to request: ").strip()
                        
                        if filename:
                            self.request_file_from_client(target_client, filename)
                except ValueError:
                    print("Invalid input")
            
            elif choice == "5":
                received_dir = "./received_files"
                if os.path.exists(received_dir):
                    files = os.listdir(received_dir)
                    if files:
                        print(f"\nReceived files ({len(files)}):")
                        for file in files:
                            file_path = os.path.join(received_dir, file)
                            size = os.path.getsize(file_path)
                            print(f"- {file} ({size} bytes)")
                    else:
                        print("No received files")
                else:
                    print("No received files directory")
            
            elif choice == "6":
                if self.pending_notifications:
                    print(f"\nPending notifications ({len(self.pending_notifications)}):")
                    for i, notif in enumerate(self.pending_notifications, 1):
                        if notif['type'] == 'file_uploaded':
                            print(f"{i}. New file: {notif['filename']} from {notif['uploader_id'][:8]}...")
                else:
                    print("No pending notifications")
            
            elif choice == "7":
                self.auto_request_files = not self.auto_request_files
                status = "enabled" if self.auto_request_files else "disabled"
                print(f"Auto-request files: {status}")
            
            elif choice == "8":
                print("Goodbye!")
                break
            
            else:
                print("Invalid choice")

if __name__ == "__main__":
    try:
        client = FileManagerClient()
        client.run_interactive()
    except KeyboardInterrupt:
        print("\nClient stopped by user")
    except Exception as e:
        print(f"Client error: {e}")
        import traceback
        traceback.print_exc()
