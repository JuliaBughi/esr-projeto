import sys, os
import time
import argparse
import threading
from transport.tcp_transport import TCPTransport
from message.message_types import HelloMessage, HelloBackMessage, JoinMessage, WelcomeMessage, LeaveMessage, PingMessage, PongMessage, FloodMessage
from message.parser import parse_message

MY_PORT = 5000


class Node:
    def __init__(self, my_ip, bootstrapper_addr, my_role):
        self.my_ip = my_ip
        self.my_role = my_role
        self.node_id = f"{my_ip}:{MY_PORT}"
        self.bootstrapper_addr = bootstrapper_addr
        self.transport = TCPTransport(my_ip, MY_PORT, self.handle_message)
        self.neighbors = {} # node_id -> {"ip": ip, "port": port, "socket": sock}
        self.next_seq = 1 
        self.flood_id = 0
        self.seen_floods = set()

    def start(self):
        self.transport.start_listening()
        time.sleep(0.5)
        self.bootstrap()
        print(f"[Node] {self.node_id} rodando. Ctrl+C para sair.")
        time.sleep(1)
        self.initial_flood()

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

    def monitor_thread(self, interval):
        """Thread que envia PING a todos os vizinhos a cada 'interval' segundos."""
        def ping_loop():
            while True:
                time.sleep(interval)
                for neighbor_id, info in list(self.neighbors.items()):
                    try:
                        self.next_seq += 1
                        ping = PingMessage(self.node_id, self.next_seq)
                        TCPTransport.send(info["socket"], ping.to_json().encode())
                        print(f"[PING] Enviei PING para {neighbor_id}")
                    except Exception as e:
                        print(f"[PING] Falha ao enviar PING para {neighbor_id}: {e}")

        t = threading.Thread(target=ping_loop, daemon=True) # daemon = True -> fecha automaticamente a thread quando o processo terminar
        t.start()


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
            

    def initial_flood(self):
        if self.my_role == "server":
            self.flood_id += 1 # flood id pode ser usado como o id do fluxo e no futuro podemos guardar a rota 
            start_jump = 0
            route_status = False # inativa

            for neighbor_id, n_info in self.neighbors.items():
                node_ip = n_info["ip"]
                node_port = n_info["port"]
                print(f"[NODE] Vou começar o flood em {node_ip}:{node_port}")

                next_nodes = [{

                    "dest": list(self.neighbors.keys())
                
                }]

                try:                              # se os vizinhos não estiverem conectados o flood nao acontece
                    TCPTransport.send( n_info["socket"], FloodMessage(self.node_id, start_jump, self.flood_id, next_nodes, self.node_id, route_status).to_json().encode()) 
                    print(f"[NODE] Enviei FLOOD para {neighbor_id}")                                                     
                except Exception as e:
                    print(f"[NODE] Falha ao enviar FLOOD para {neighbor_id}: {e}")
        else: 
            return

    def handle_message(self, sock, data):
        """Callback para mensagens recebidas"""
        if not data:
            return

        try:
            msg_obj = parse_message(data.decode())

            if isinstance(msg_obj, WelcomeMessage):
                print(f"[Node] Recebi vizinhos: {msg_obj.neighbors}")
                for n in msg_obj.neighbors:
                    n_id = f"{n['host']}:{n['port']}"
                    peer_sock = self.transport.connect(n["host"], n["port"])
                    if peer_sock:
                        TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                        self.neighbors[n_id] = {"ip": n["host"], "port": n["port"], "socket": peer_sock}


            elif isinstance(msg_obj, HelloMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLO de {sender_id}")

                # Se ainda não temos a conexão com esse nó, registramos
                if sender_id not in self.neighbors:
                    ip_part, port_part = sender_id.split(":")
                    self.neighbors[sender_id] = {"ip": ip_part, "port": int(port_part), "socket": sock}

                TCPTransport.send(sock, HelloBackMessage(self.node_id).to_json().encode())

            elif isinstance(msg_obj, HelloBackMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLOBACK de {sender_id}")
                # Marca conexão como estabelecida ou atualiza estado
                if sender_id in self.neighbors:
                    self.neighbors[sender_id]["established"] = True

                if not hasattr(self, "monitor_thread_started"):
                    self.monitor_thread_started = True
                    self.monitor_thread(interval=120) # quanto tempo de interval?


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

            elif isinstance(msg_obj, FloodMessage):
                sender_id = msg_obj.node_id
                jump = msg_obj.jump
                flood_id = msg_obj.flood_id
                server_ip = msg_obj.server_ip
                route_status = msg_obj.route_status

                next_nodes = [{

                    "dest": list(self.neighbors.keys())
                
                }]


                if flood_id in self.seen_floods:
                    print(f"[Node] A ignorar FLOOD duplicado de {sender_id}")
                    return

                self.seen_floods.add(flood_id)
                print(f"[Node] Recebi FLOOD de {sender_id}")

                ip_part, port_part = sender_id.split(":")
                self.neighbors[sender_id] = {"ip": ip_part, "port": int(port_part), "socket": sock}

                for neighbor_id, n_info in self.neighbors.items():
                    if neighbor_id == sender_id:
                        continue  # skip the sender

                    try:
                        sock = n_info["socket"]
                        TCPTransport.send(
                            sock,
                            FloodMessage(self.node_id, jump + 1, flood_id, next_nodes, server_ip, route_status).to_json().encode()
                        )
                        print(f"[Node] Reenviei FLOOD para {neighbor_id}")
                    except Exception as e:
                        print(f"[Node] Falha ao reenviar FLOOD para {neighbor_id}: {e}")

            elif isinstance(msg_obj, PingMessage):
                sender_id = msg_obj.node_id
                seq = msg_obj.seq
                print(f"[Node] Recebi PING de {sender_id}")

                ip_part, port_part = sender_id.split(":")
                self.neighbors[sender_id] = {"ip": ip_part, "port": int(port_part), "socket": sock}

                TCPTransport.send(sock, PongMessage(self.node_id,seq).to_json().encode())
                print(f"[Node] Enviei PONG para {sender_id}")

            elif isinstance(msg_obj, PongMessage):
                sender_id = msg_obj.node_id
                seq = msg_obj.seq
                print(f"[Node] Recebi PONG de {sender_id}")
            

            else:
                print(f"[Node] Recebi mensagem desconhecida: {msg_obj}")

        except Exception as e:
            print(f"[Node] Erro ao processar mensagem: {e}")
    
# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Over the top application")
    parser.add_argument("--role", type=str, required=True, choices = ["server", "client"], help="Server ou Client")
    parser.add_argument("--host", default="0.0.0.0", help="IP do node Host")
    parser.add_argument("--bootstrap", default="0.0.0.0", help="IP do Bootstrapper")
    args = parser.parse_args()

    node = Node(args.host, (args.bootstrap, 5000), args.role) 
    node.start() 