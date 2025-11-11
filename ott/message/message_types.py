import json
import time
from typing import List, Dict


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

    def __init__(self, neighbors: List[Dict[str, str]]):
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

class LeaveMessage(Message):
    TYPE = "LEAVE"

    def __init__(self, node_id: str, neighbors:List[Dict[str, str]]):
        self.node_id = node_id
        self.neighbors = neighbors

class PingMessage(Message):
    TYPE="PING"

    def __init__(self, node_id: str, seq: int, timestamp: float = None):
        self.node_id = node_id  
        self.seq = seq       # identificador de mensangens correspondentes, mas como estamos a usar tcp este seq nao é importante mas em udp pode dar jeito
    
class PongMessage(Message):
    TYPE = "PONG"

    def __init__(self, node_id: str, seq: int):
        self.node_id = node_id
        self.seq = seq

class FloodMessage(Message):
    TYPE = "FLOOD"

    def __init__(self, node_id : str, jump: int, flood_id : int, neighbors:List[Dict[str, str]], server_ip: str, route_status: bool):
        self.node_id = node_id
        self.jump = jump
        self.flood_id = flood_id
        self.neighbors = neighbors
        self.server_ip = server_ip
        self.route_status = route_status
    