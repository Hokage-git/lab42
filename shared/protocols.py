import json
import time
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional

class MessageType(Enum):
    REGISTER_SERVER = "register_server"
    HEARTBEAT = "heartbeat"
    GET_SERVERS = "get_servers"
    GET_FILES = "get_files"
    DOWNLOAD_FILE = "download_file"
    FILE_CHUNK = "file_chunk"
    DISCOVERY_REQUEST = "discovery_request"
    DISCOVERY_RESPONSE = "discovery_response"

@dataclass
class ServerInfo:
    server_id: str
    ip_address: str
    port: int
    service_type: str
    status: str = "active"

@dataclass
class FileInfo:
    filename: str
    file_size: int
    file_hash: str
    server_id: str

class Protocol:
    @staticmethod
    def create_message(msg_type: MessageType, data: dict) -> str:
        return json.dumps({
            "type": msg_type.value,
            "data": data,
            "timestamp": time.time()
        })
    
    @staticmethod
    def parse_message(message: str) -> dict:
        return json.loads(message)
