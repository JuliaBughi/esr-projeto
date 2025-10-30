import json
from .message_types import Message, JoinMessage, WelcomeMessage, HelloMessage, HelloBackMessage, LeaveMessage

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
    else:
        # Mensagem desconhecida
        return Message()