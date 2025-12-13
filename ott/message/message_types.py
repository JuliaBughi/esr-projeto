import json
import time
from typing import List, Dict, Optional


# ----------------------------
# Classe base
# ----------------------------
class Message:
    TYPE = "BASE"

    def to_json(self) -> str:
        """
        Serializa a mensagem para JSON
        """
        data = {"type": self.TYPE}
        for k, v in self.__dict__.items():
            data[k] = v
        return json.dumps(data)

# ----------------------------
# Mensagem JOIN (enviada pelo nó)
# ----------------------------
class JoinMessage(Message):
    TYPE = "JOIN"

    def __init__(self, node_id: str):
        self.node_id = node_id

# ----------------------------
# Mensagem WELCOME (resposta do bootstrapper)
# ----------------------------
class WelcomeMessage(Message):
    TYPE = "WELCOME"

    def __init__(self, node_id: str, neighbors: List[Dict[str, str]]):
        self.node_id = node_id        
        self.neighbors = neighbors      

# ----------------------------
# Mensagem HELLOWORLD (tentativa de conexão com outros nós)
# ----------------------------
class HelloMessage(Message):
    TYPE = "HELLO"

    def __init__(self, node_id, ip, port):
        self.type = "HELLO"
        self.node_id = node_id
        self.ip = ip
        self.port = port
        
# ----------------------------
# Mensagem HELLOBACK (aceitação de conexão com outro nó)
# ----------------------------
class HelloBackMessage(Message):
    TYPE = "HELLOBACK"

    def __init__(self, node_id, ip, port):
        self.type = "HELLOBACK"
        self.node_id = node_id
        self.ip = ip
        self.port = port

# ----------------------------
# Mensagem FLOOD (inundação do overlay)
# ----------------------------
class FloodMessage(Message):
    TYPE = "FLOOD"

    def __init__(self, origin: str, sender: str, grandparent: Optional[Dict], jumps: int, loss: float = 0.0,
                 bandwidth: float = 0.0, latency: float = 0.0):
        self.origin = origin
        self.sender = sender
        self.grandparent = grandparent
        self.jumps = jumps
        self.loss = loss
        self.bandwidth = bandwidth
        self.latency = latency

# ----------------------------
# Mensagem ACTIVATE (ativação da rota)
# ----------------------------
class ActivateMessage(Message):
    TYPE = "ACTIVATE"

    def __init__(self, origin: str, client_id: str):
        self.origin = origin
        self.client_id = client_id

# ----------------------------
# Mensagem DANGER (proteção de nodos folha)
# ----------------------------
class DangerMessage(Message):
    TYPE = "DANGER"
    
    def __init__(self, node_id: str):
        self.node_id = node_id

# ----------------------------
# Mensagem SAFEOPTION (proteção de nodos folha)
# ----------------------------
class SafeOptionMessage(Message):
    TYPE = "SAFEOPTION"
    
    def __init__(self, node_id: str, safe: str):
        self.node_id = node_id
        self.safe = safe
  
# ----------------------------
# Mensagem HEARTBEAT (monitorar)
# ----------------------------
class HeartBeatActiveMessage(Message):
    TYPE = "HB_ACTIVE"

    def __init__(self, node_id: str, timestamp: float):
        self.node_id = node_id
        self.timestamp = timestamp

class ClientStreamRequest(Message):
    TYPE = "CLIENT_REQ"
    
    def __init__(self, client_id: str):
        self.client_id = client_id
