# oot/message/parser.py
import json

def create_message(msg_type, payload={}):
    """Cria uma mensagem formatada em JSON e codificada em bytes."""
    message = {
        "type": msg_type,
        "payload": payload
    }
    return json.dumps(message).encode('utf-8')

def parse_message(data_bytes):
    """Recebe bytes, descodifica e faz parse do JSON para um dicionário Python."""
    try:
        return json.loads(data_bytes.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None # Retorna None se a mensagem for inválida