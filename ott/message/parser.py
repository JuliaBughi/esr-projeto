import json
from .message_types import Message, JoinMessage, WelcomeMessage, HelloMessage, HelloBackMessage, LeaveMessage, PingMessage, PongMessage, FloodMessage

def parse_message(json_str: str) -> Message:
    """
    Desserializa JSON em uma instância de Message adequada
    """
    data = json.loads(json_str)
    msg_type = data.get("type")

    if msg_type == "JOIN":
        return JoinMessage(node_id=data["node_id"])
    elif msg_type == "WELCOME":
        return WelcomeMessage(neighbors=data["neighbors"])
    elif msg_type == "HELLO":
        return HelloMessage(node_id=data["node_id"])
    elif msg_type == "HELLOBACK":
        return HelloBackMessage(node_id=data["node_id"])
    elif msg_type == "LEAVE":
        return LeaveMessage(node_id=data["node_id"], neighbors=data["neighbors"])
    elif msg_type == "PING":
        return PingMessage(node_id=data["node_id"], seq=data["seq"])
    elif msg_type == "PONG":
        return PongMessage(node_id=data["node_id"], seq=data["seq"])
    elif msg_type == "FLOOD":
        return FloodMessage(node_id=data["node_id"], jump=data["jump"], flood_id=data["flood_id"], neighbors=data["neighbors"], server_ip=data["server_ip"])
    else:
        # Mensagem desconhecida
        return Message()