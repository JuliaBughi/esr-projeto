# bootstraper.py
import socket
import threading
import json
import random

# Limite de vizinhos que o bootstrapper sugere a um novo nó.
MAX_NEIGHBORS = 3

class Bootstrapper:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.known_nodes = [] # Lista de tuplos (ip, port)
        self.lock = threading.Lock()
        print(f"Bootstrapper a escutar em {ip}:{port}")

    def start(self):
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(10)
        while True:
            client_socket, addr = self.server_socket.accept()
            thread = threading.Thread(target=self._handle_client, args=(client_socket, addr))
            thread.start()

    def _handle_client(self, client_socket, addr):
        print(f"Nova conexão de {addr}")
        try:
            data = client_socket.recv(1024)
            if not data:
                return

            message = json.loads(data.decode('utf-8'))
            
            if message.get("type") == "JOIN_REQUEST":
                node_ip = message["payload"]["ip"]
                node_port = message["payload"]["port"]
                new_node_addr = (node_ip, node_port)
                
                with self.lock:
                    # Seleciona vizinhos para enviar de volta, excluindo o próprio nó
                    other_nodes = [node for node in self.known_nodes if node != new_node_addr]
                    
                    # Usa min() para não falhar se houver menos nós que MAX_NEIGHBORS
                    num_to_send = min(len(other_nodes), MAX_NEIGHBORS)
                    neighbors_to_send = random.sample(other_nodes, num_to_send)

                    # Adiciona o novo nó à lista para futuros pedidos
                    if new_node_addr not in self.known_nodes:
                        self.known_nodes.append(new_node_addr)

                response = {
                    "type": "NEIGHBOR_LIST",
                    "payload": neighbors_to_send
                }
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                print(f"Enviada lista de {len(neighbors_to_send)} vizinhos para {addr}.")
                print(f"Nós conhecidos agora: {self.known_nodes}")

        except Exception as e:
            print(f"Erro ao lidar com o cliente {addr}: {e}")
        finally:
            client_socket.close()

if __name__ == "__main__":
    # O bootstrapper corre num IP e porta conhecidos
    bootstrapper = Bootstrapper('0.0.0.0', 8000)
    bootstrapper.start()