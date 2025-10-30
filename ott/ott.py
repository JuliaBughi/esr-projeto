import sys, os
import time
from transport.tcp_transport import TCPTransport
from message.message_types import HelloMessage, HelloBackMessage, JoinMessage, WelcomeMessage, LeaveMessage
from message.parser import parse_message

MY_PORT = 5000

class Node:
    def __init__(self, my_ip, bootstrapper_addr):
        self.my_ip = my_ip
        self.node_id = f"{my_ip}:{MY_PORT}"
        self.bootstrapper_addr = bootstrapper_addr
        self.transport = TCPTransport(my_ip, MY_PORT, self.handle_message)
        self.neighbors = {}  # node_id -> {"ip": ip, "port": port, "socket": sock}

    def start(self):
        self.transport.start_listening()
        time.sleep(0.5)
        self.bootstrap()
        print(f"[Node] {self.node_id} rodando. Ctrl+C para sair.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.graceful_leave()

    def bootstrap(self):
        """Conecta ao bootstrapper e recebe vizinhos"""
        sock = self.transport.connect(*self.bootstrapper_addr)
        if not sock:
            print("[Bootstrapper] Falha ao conectar")
            return
        # Envia JOIN
        TCPTransport.send(sock, JoinMessage(self.node_id).to_json().encode())
        # Aguarda WelcomeMessage
        time.sleep(0.5)  # espera simples
        # Se houver resposta, a callback handle_message tratará
        # (recebe WelcomeMessage e inicia handshake)

    def graceful_leave(self):
        num_neighbors = len(self.neighbors)

        if num_neighbors == 1:
        # Caso 1: apenas 1 vizinho, avisar e fechar
            only_neighbor_id = list(self.neighbors.keys())[0]
            neighbor = self.neighbors[only_neighbor_id]
            try:
                TCPTransport.send(neighbor["socket"], LeaveMessage(self.node_id, []).to_json().encode())
                print(f"[Node] Avisei {only_neighbor_id} e fechando conexão...")
                neighbor["socket"].close()
            except Exception as e:
                print(f"[Node] Erro ao avisar {only_neighbor_id}: {e}")
            self.neighbors.clear()
            self.transport.stop_listening()
            return

        else: #Futuramente explorar opções melhores
        # Caso 2: >1 vizinho, mandar tabela de vizinhos
            print(f"[Node] {self.node_id} enviando tabela de vizinhos antes de sair...")
            for v_id, v_info in list(self.neighbors.items()):
                # construir lista de vizinhos excluindo o próprio destinatário e o nó que sai
                neighbor_list = [ 
                    {"id": nid, "ip": info["ip"], "port": info["port"]} 
                    for nid, info in self.neighbors.items() 
                    if nid != v_id # exclui destinatário 
                    ]
                try:
                    TCPTransport.send(v_info["socket"], LeaveMessage(self.node_id, neighbor_list).to_json().encode())
                    print(f"[Node] Avisei {v_id} com a lista de vizinhos.")
                except Exception as e:
                    print(f"[Node] Erro ao avisar {v_id}: {e}")

            # esperar alguns segundos para que os vizinhos se reconectem
            time.sleep(2)
            # fechar todas as conexões e parar
            for v_id, v_info in self.neighbors.items():
                try:
                    v_info["socket"].close()
                except:
                    pass
            self.neighbors.clear()
            self.transport.stop_listening()
            print(f"[Node] {self.node_id} saiu da rede com sucesso.")


    def handle_message(self, sock, data):
        """Callback para mensagens recebidas"""
        if not data:
            return

        try:
            msg_obj = parse_message(data.decode())

            if isinstance(msg_obj, WelcomeMessage):
                print(f"[Node] Recebi vizinhos: {msg_obj.neighbors}")
                for n in msg_obj.neighbors:
                    peer_sock = self.transport.connect(n["host"], n["port"])
                    if peer_sock:
                        # Envia HELLO para iniciar handshake 1:1
                        TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                        # Armazena vizinho e socket
                        self.neighbors[n["id"]] = {"ip": n["host"], "port": n["port"], "socket": peer_sock}

            elif isinstance(msg_obj, HelloMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLO de {sender_id}")

                # Se ainda não temos a conexão com esse nó, registramos
                if sender_id not in self.neighbors:
                    peer_ip, peer_port = sock.getpeername()  # pega IP e porta do socket
                    self.neighbors[sender_id] = {"ip": peer_ip, "port": peer_port, "socket": sock}

                TCPTransport.send(sock, HelloBackMessage(self.node_id).to_json().encode())

            elif isinstance(msg_obj, HelloBackMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLOBACK de {sender_id}")
                # Marca conexão como estabelecida ou atualiza estado
                if sender_id in self.neighbors:
                    self.neighbors[sender_id]["established"] = True

            elif isinstance(msg_obj, LeaveMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi LEAVE de {sender_id}")

                # Remove o nó que saiu da tabela de vizinhos e fecha socket
                if sender_id in self.neighbors:
                    try:
                        self.neighbors[sender_id]["socket"].close()
                    except:
                        pass
                    del self.neighbors[sender_id]
                    print(f"[Node] Removi {sender_id} da tabela de vizinhos")

                # Se houver lista de vizinhos, reconectar
                for n in msg_obj.neighbors:
                    n_id = n["id"]
                    if n_id != self.node_id and n_id not in self.neighbors:
                        peer_sock = self.transport.connect(n["ip"], n["port"])
                        if peer_sock:
                            self.neighbors[n_id] = {"ip": n["id"], "port": n["port"], "socket": peer_sock}
                            TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                            print(f"[Node] Conectei-me a novo vizinho {n_id}")

            else:
                print(f"[Node] Recebi mensagem desconhecida: {msg_obj}")

        except Exception as e:
            print(f"[Node] Erro ao processar mensagem: {e}")

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python ott.py <IP do node> <IP do bootstrapper>")
        sys.exit(1)

    print(len(sys.argv))
    bootstrap_ip = sys.argv[2]
    node = Node(sys.argv[1], (bootstrap_ip, 5000)) # trocar para ip do node
    node.start()