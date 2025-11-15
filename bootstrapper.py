import json
import socket
import threading
import argparse
import os
import struct
from ott.message.message_types import JoinMessage, WelcomeMessage
from ott.message.parser import parse_message

def load_topology(config_number):
    """Carrega o ficheiro JSON da topologia, ex: overlay2.json"""
    filename = f"configs/overlay{config_number}.json"
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Ficheiro {filename} não encontrado.")
    with open(filename, "r") as f:
        data = json.load(f)
    return data["nodes"]


def send_length_prefixed(conn, message_bytes):
    length_prefix = struct.pack('!I', len(message_bytes)) # converte os dados em bytes que podem ser mandados pela rede
    conn.sendall(length_prefix + message_bytes)

def recv_length_prefixed(conn):
    header = conn.recv(4)
    if not header or len(header) < 4: # se a mensagem tiver menos de 4 bytes ou a conexão fechou damos return
        return None
    msg_length = struct.unpack('!I', header)[0] # converte o prefixo de 4 bytes num numero que é o tamanho de bytes que tem de ler
    msg_bytes = b''
    while len(msg_bytes) < msg_length: # o tcp pode mandar a mensagem em chunks entao nao paramos de ler ate a mensagem acabar
        chunk = conn.recv(msg_length - len(msg_bytes))
        if not chunk:
            return None
        msg_bytes += chunk
    return msg_bytes

def handle_client(conn, nodes):
    """Atende pedidos JOIN de novos nós"""
    try:
        data = recv_length_prefixed(conn)
        if not data:
            return
        req_msg = parse_message(data.decode()) 
        if isinstance(req_msg, JoinMessage):
            node_id = req_msg.node_id
            node_ip, node_port = node_id.split(":") 
            node_port = int(node_port)
            node_key = next((n for n, info in nodes.items() if info["host"] == node_ip), None)  
            neighbors = [{"id": n, "host": nodes[n]["host"], "port": node_port}                
                            for n in nodes[node_key]["neighbors"] if n in nodes]
            print(neighbors)
            resp_msg = WelcomeMessage(neighbors)
            send_length_prefixed(conn, resp_msg.to_json().encode())
    except Exception as e:
        print(f"[ERRO] {e}")
    finally:
        conn.close()

def start_bootstrapper(config_number, host="0.0.0.0", port=5000):
    nodes = load_topology(config_number)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"[BOOTSTRAPPER] Online em {host}:{port} com {len(nodes)} nós carregados")

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn, nodes)).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bootstrapper externo OTT Overlay")
    parser.add_argument("--config", type=int, required=True, help="Número do overlay JSON")
    parser.add_argument("--host", default="0.0.0.0", help="IP do bootstrapper")
    parser.add_argument("--port", type=int, default=5000, help="Porta do bootstrapper")
    args = parser.parse_args()

    start_bootstrapper(args.config, host=args.host, port=args.port)
