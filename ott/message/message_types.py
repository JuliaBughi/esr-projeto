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

    def __init__(self, node_id: str):
        self.node_id = node_id
        
# ----------------------------
# Mensagem HELLOBACK (aceitação de conexão com outro nó)
# ----------------------------
class HelloBackMessage(Message):
    TYPE = "HELLOBACK"

    def __init__(self, node_id: str):
        self.node_id = node_id

# JOIN, HELLO E HELLOBACK são iguais, vale a pena diferenciar? ver mais para frente se é preciso mais alguma coisa, se não dá para usar só hello


# ----------------------------
# Mensagem LEAVE (saída do overlay)
# ----------------------------
class LeaveMessage(Message):
    TYPE = "LEAVE"

    def __init__(self, node_id: str, recommended: Optional[Dict]):
        self.node_id = node_id
        self.recommended = recommended


# ----------------------------
# Mensagem FLOOD (inundação do overlay)
# ----------------------------
class FloodMessage(Message):
    TYPE = "FLOOD"

    def __init__(self, origin: str, sender: str, jumps: int, loss: float = 0.0,
                 bandwidth: float = 0.0, latency: float = 0.0):
        self.origin = origin
        self.sender = sender
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
    
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
