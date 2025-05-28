import requests
import socket
import os
import sys
import uuid
import json
import time
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
            local_ip = s.getsockname()[0]
            s.close()
            print(f"Your IP address: {local_ip}")
        except:
            print("Could not determine local IP")
        
        discovery_ip = input("Enter Discovery Server IP (or press Enter for localhost): ").strip()
        if discovery_ip:
            self.discovery_server_url = f"http://{discovery_ip}:8000"
        else:
            self.discovery_server_url = "http://localhost:8000"
        
        self.available_servers = []
        self.client_id = str(uuid.uuid4())
        print(f"Client ID: {self.client_id}")
        print(f"Discovery Server: {self.discovery_server_url}")
        
        self.test_discovery_connection()
        
    def test_discovery_connection(self):
        try:
            response = requests.get(f"{self.discovery_server_url}/status", timeout=5)
            if response.status_code == 200:
                print(" Discovery Server connection successful")
                return True
            else:
                print(f"✗ Discovery Server returned HTTP {response.status_code}")
        except requests.RequestException as e:
            print(f"✗ Cannot connect to Discovery Server: {e}")
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
            print(f" UDP Multicast discovery sent to {multicast_addr}")
            
            try:
                data, addr = sock.recvfrom(1024)
                response = json.loads(data.decode())
                
                if response.get('type') == 'discovery_response':
                    self.discovery_server_url = response.get('discovery_server')
                    print(f" Discovery Server found via UDP: {self.discovery_server_url}")
                    return True
            except socket.timeout:
                print("✗ UDP Multicast discovery timeout")
                
            sock.close()
            return False
            
        except Exception as e:
            print(f"✗ UDP Multicast discovery error: {e}")
            return False
    
    def get_available_servers(self) -> List[dict]:
        try:
            response = requests.get(f"{self.discovery_server_url}/servers", timeout=10)
            if response.status_code == 200:
                servers_data = response.json()
                print(f"Raw server data: {servers_data}")
                
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
                    print(f"Added server: {server_info['ip_address']}:{server_info['port']}")
                
                return self.available_servers
        except requests.RequestException as e:
            print(f"Error getting servers: {e}")
            if self.discover_via_udp_multicast():
                return self.get_available_servers()
        
        return []
    
    def get_files_from_server(self, server: dict) -> List[dict]:
        try:
            url = f"http://{server['ip_address']}:{server['port']}/files"
            print(f"Requesting files from: {url}")
            
            response = requests.get(url, timeout=10)
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                files_data = response.json()
                print(f"Received {len(files_data)} files from {server['ip_address']}:{server['port']}")
                
                for file in files_data:
                    file['server_info'] = server
                
                return files_data
            else:
                print(f"Error response from {server['ip_address']}:{server['port']}: {response.text}")
                return []
                
        except requests.RequestException as e:
            print(f"Error getting files from {server['server_id']} ({server['ip_address']}:{server['port']}): {e}")
            
            try:
                debug_url = f"http://{server['ip_address']}:{server['port']}/status"
                debug_response = requests.get(debug_url, timeout=5)
                if debug_response.status_code == 200:
                    debug_data = debug_response.json()
                    print(f"Server status: {debug_data}")
                else:
                    print(f"Server {server['ip_address']}:{server['port']} is unreachable")
            except:
                print(f"Server {server['ip_address']}:{server['port']} is completely unreachable")
        
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
                
                print(f"\n File {filename} downloaded successfully to {file_path}")
                print(f"File size: {downloaded} bytes")
                return True
            else:
                print(f"✗ Failed to download {filename}: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except requests.RequestException as e:
            print(f"✗ Error downloading file: {e}")
            return False
    
    def show_server_status(self):
        servers = self.get_available_servers()
        
        if not servers:
            print("No servers found")
            return
        
        print("\n" + "="*60)
        print("SERVER STATUS")
        print("="*60)
        
        for i, server in enumerate(servers, 1):
            print(f"\n[{i}] Server: {server['server_id'][:8]}...")
            print(f"    Type: {server['service_type']}")
            print(f"    Address: {server['ip_address']}:{server['port']}")
            print(f"    DB Status: {server['status']}")
            
            if server['service_type'] == 'file_server':
                try:
                    status_url = f"http://{server['ip_address']}:{server['port']}/status"
                    response = requests.get(status_url, timeout=5)
                    if response.status_code == 200:
                        status_data = response.json()
                        print(f"    Real Status: ONLINE ✅")
                        print(f"    Files: {status_data.get('files_count', 0)}")
                        print(f"    Files in DB: {status_data.get('files_in_db', 0)}")
                        print(f"    Total Size: {status_data.get('total_size', 0)} bytes")
                        
                        files_url = f"http://{server['ip_address']}:{server['port']}/files"
                        files_response = requests.get(files_url, timeout=5)
                        if files_response.status_code == 200:
                            files_data = files_response.json()
                            print(f"    Available files: {len(files_data)}")
                            for file in files_data[:3]: 
                                filename = file.get('filename', 'unknown')
                                filesize = file.get('file_size', 0)
                                print(f"      - {filename} ({filesize} bytes)")
                            if len(files_data) > 3:
                                print(f"      ... and {len(files_data) - 3} more files")
                        else:
                            print(f"    Files endpoint error: HTTP {files_response.status_code}")
                    else:
                        print(f"    Real Status: ERROR (HTTP {response.status_code}) ")
                except Exception as e:
                    print(f"    Real Status: UNREACHABLE ")
                    print(f"    Error: {str(e)}")
            
            print("-" * 60)
    
    def test_server_connection(self):
        servers = self.get_available_servers()
        if not servers:
            print("No servers to test")
            return
            
        print("\n" + "="*50)
        print("CONNECTION TEST")
        print("="*50)
        
        for server in servers:
            if server['service_type'] == 'file_server':
                print(f"\nTesting {server['ip_address']}:{server['port']}")
                
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(3)
                    result = sock.connect_ex((server['ip_address'], server['port']))
                    sock.close()
                    
                    if result == 0:
                        print("   TCP connection successful")
                    else:
                        print("  ✗ TCP connection failed")
                        continue
                    
                    test_url = f"http://{server['ip_address']}:{server['port']}/status"
                    response = requests.get(test_url, timeout=5)
                    print(f"   HTTP status: {response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        print(f"   Server response: {data.get('status', 'unknown')}")
                        
                        files_url = f"http://{server['ip_address']}:{server['port']}/files"
                        files_response = requests.get(files_url, timeout=5)
                        print(f"   Files endpoint: {files_response.status_code}")
                        
                        if files_response.status_code == 200:
                            files = files_response.json()
                            print(f"   Available files: {len(files)}")
                    
                except Exception as e:
                    print(f"  ✗ Connection test failed: {e}")
    
    def run_interactive(self):
        print("\n" + "="*50)
        print("DISTRIBUTED FILE MANAGER CLIENT")
        print("="*50)
        
        while True:
            print("\nOptions:")
            print("1. List all files")
            print("2. Download file")
            print("3. Show server status")
            print("4. Refresh servers")
            print("5. Test server connections")
            print("6. Change Discovery Server")
            print("7. Exit")
            
            choice = input("\nChoose option: ").strip()
            
            if choice == "1":
                print("\n" + "-"*50)
                print("GETTING FILES FROM ALL SERVERS")
                print("-"*50)
                files = self.get_all_files()
                if files:
                    print(f"\nFound {len(files)} files:")
                    print("-"*50)
                    for i, file in enumerate(files, 1):
                        server_info = file.get('server_info', {})
                        server_addr = f"{server_info.get('ip_address', 'unknown')}:{server_info.get('port', 'unknown')}"
                        file_size = file.get('file_size', 0)
                        filename = file.get('filename', 'unknown')
                        print(f"{i:2d}. {filename:<30} ({file_size:>8} bytes) - {server_addr}")
                else:
                    print("No files available")
                    print("Try option 3 to check server status or option 5 to test connections")
            
            elif choice == "2":
                files = self.get_all_files()
                if not files:
                    print("No files available for download")
                    continue
                
                print(f"\nAvailable files:")
                print("-"*70)
                for i, file in enumerate(files, 1):
                    server_info = file.get('server_info', {})
                    server_addr = f"{server_info.get('ip_address', 'unknown')}:{server_info.get('port', 'unknown')}"
                    filename = file.get('filename', 'unknown')
                    print(f"{i:2d}. {filename:<30} - {server_addr}")
                
                try:
                    file_index = int(input(f"\nEnter file number (1-{len(files)}): ")) - 1
                    if 0 <= file_index < len(files):
                        selected_file = files[file_index]
                        server_info = selected_file.get('server_info')
                        
                        if server_info:
                            filename = selected_file.get('filename', 'unknown')
                            server_addr = f"{server_info['ip_address']}:{server_info['port']}"
                            print(f"\nDownloading {filename} from {server_addr}...")
                            success = self.download_file(filename, server_info)
                            if success:
                                print(" Download completed successfully!")
                            else:
                                print("✗ Download failed!")
                        else:
                            print("✗ Server information not available")
                    else:
                        print("✗ Invalid file number")
                except ValueError:
                    print("✗ Invalid input - please enter a number")
            
            elif choice == "3":
                self.show_server_status()
            
            elif choice == "4":
                print("\nRefreshing server list...")
                servers = self.get_available_servers()
                print(f"Found {len(servers)} servers")
                for server in servers:
                    if server['service_type'] == 'file_server':
                        print(f"  - {server['ip_address']}:{server['port']} ({server['status']})")
            
            elif choice == "5":
                self.test_server_connection()
            
            elif choice == "6":
                discovery_ip = input("Enter new Discovery Server IP: ").strip()
                if discovery_ip:
                    self.discovery_server_url = f"http://{discovery_ip}:8000"
                    print(f"Discovery Server changed to: {self.discovery_server_url}")
                    self.test_discovery_connection()
            
            elif choice == "7":
                print("Goodbye!")
                break
            
            else:
                print("Invalid choice. Please select 1-7.")

if __name__ == "__main__":
    try:
        client = FileManagerClient()
        client.run_interactive()
    except KeyboardInterrupt:
        print("\n\nClient stopped by user")
    except Exception as e:
        print(f"\nClient error: {e}")
        import traceback
        traceback.print_exc()
