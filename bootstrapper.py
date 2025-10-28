import json
import socket
import threading
import argparse
import os
from ott.message.message_types import JoinMessage, WelcomeMessage, Message

def load_topology(config_number):
    """Carrega o ficheiro JSON da topologia, ex: overlay2.json"""
    filename = f"configs/overlay{config_number}.json"
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Ficheiro {filename} não encontrado.")
    with open(filename, "r") as f:
        data = json.load(f)
    return data["nodes"]

def handle_client(conn, nodes):
    """Atende pedidos JOIN de novos nós"""
    try:
        data = conn.recv(1024).decode()
        req_msg = Message.from_json(data)  
        if isinstance(req_msg, JoinMessage):
            node_id = req_msg.node_id
            neighbors = [{"id": n, "host": nodes[n]["host"], "port": nodes[n]["port"]}
                            for n in nodes[node_id]["neighbors"] if n in nodes]
            resp_msg = WelcomeMessage(neighbors)
            conn.send(resp_msg.to_json().encode())
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
