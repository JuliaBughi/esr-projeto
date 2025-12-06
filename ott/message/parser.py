import json
from .message_types import Message, JoinMessage, WelcomeMessage, HelloMessage, HelloBackMessage, LeaveMessage, FloodMessage, ActivateMessage, HeartBeatActiveMessage

def parse_message(json_str: str) -> Message:
    """
    Desserializa JSON em uma instância de Message adequada
    """
    data = json.loads(json_str)
    msg_type = data.get("type")

    if msg_type == "JOIN":
        return JoinMessage(node_id=data["node_id"])
    
    elif msg_type == "WELCOME":
        return WelcomeMessage(node_id=data["node_id"], neighbors=data["neighbors"])
    
    elif msg_type == "HELLO":
        return HelloMessage(node_id=data["node_id"])
    
    elif msg_type == "HELLOBACK":
        return HelloBackMessage(node_id=data["node_id"])
    
    elif msg_type == "LEAVE":
        return LeaveMessage(node_id=data["node_id"], recommended=data["recommended"])
    
    elif msg_type == "FLOOD":
        return FloodMessage(
            origin=data["origin"], sender=data["sender"],
            jumps=data.get("jumps", 0), loss=data.get("loss", 0),
            bandwidth=data.get("bandwidth", 0), latency=data.get("latency", 0))

    elif msg_type == "ACTIVATE":
        return ActivateMessage(origin=data["origin"], client_id=data["client_id"])

    elif msg_type == "DANGER":
        return DangerMessage(node_id=data["node_id"])

    elif msg_type == "SAFEOPTION":
        return SafeOptionMessage(
            node_id=data["node_id"],
            safe=data["safe"]
        )

    elif msg_type == "HB_ACTIVE":
        return HeartBeatActiveMessage(
            node_id=data["node_id"],
            timestamp=data["timestamp"]
        )

    else:
        # Mensagem desconhecida
        return Message()