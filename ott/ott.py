import sys
import os
import time
import argparse
import threading
from transport.tcp_transport import TCPTransport
from message.message_types import HelloMessage, HelloBackMessage, JoinMessage, WelcomeMessage, LeaveMessage, FloodMessage, ActivateMessage
from message.parser import parse_message

MY_PORT = 5000


class Node:
    def __init__(self, my_ip, bootstrapper_addr, my_role):
        self.my_ip = my_ip
        self.my_role = my_role
        self.node_id = f"{my_ip}:{MY_PORT}"
        self.bootstrapper_addr = bootstrapper_addr
        self.lock = threading.Lock()
        self.transport = TCPTransport(my_ip, MY_PORT, self.handle_message)

        self.overlay_mapping = {}  # id do overlay -> {"host": ip, "port": port}

        # node_id -> {"ip": ip, "port": port, "socket": sock, "established": bool}
        self.neighbors = {}

        # routing table entries: dicts with keys origin, sender, metric, state, timestamp
        self.routing_table = []

        # socket -> node_id mapping (para identificar o remetente dos pacotes)
        self.sock_to_id = {}

        # children table: origin -> set(node_id) : nodos que pediram ativação via este nó
        self.children = {}

        # Track best metric seen recently per origin (to help cortar repropagação/ciclos)
        # origin -> {"metric": best_metric, "timestamp": last_seen}
        self.recent_best_metric = {}

    def start(self):
        self.transport.start_listening()
        time.sleep(0.5)
        self.bootstrap()
        print(f"[Node] {self.node_id} rodando. Ctrl+C para sair.")
        time.sleep(1)
        # ligar server por último mas futuramente ver se dá para melhorar isso idk       
        if self.my_role == "server" and len(self.neighbors) > 0:
            t = threading.Thread(target=self.periodic_flood, daemon=True)
            t.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.graceful_leave()

    def bootstrap(self):
        sock = self.transport.connect(*self.bootstrapper_addr)
        if not sock:
            print("[Bootstrapper] Falha ao conectar")
            return
        # Quando conectamos ao bootstrapper, não conhecemos ainda o node_id dele — mas enviamos Join
        physical_id = f"{self.my_ip}:{MY_PORT}"
        TCPTransport.send(sock, JoinMessage(physical_id).to_json().encode())
        # registamos temporariamente o socket (sem node_id conhecido)
        self.sock_to_id[sock] = None
        time.sleep(0.5)

    def periodic_flood(self, interval=60):
        """
        Server periodically floods the network announcing flows.
        """
        while True:
            start_jump = 0
            with self.lock:
                neighbors_items = list(self.neighbors.items())
            for neighbor_id, n_info in neighbors_items:
                try:
                    TCPTransport.send(
                        n_info["socket"],
                        FloodMessage(origin=self.node_id, sender=self.node_id, jumps=start_jump, metric=start_jump).to_json().encode()
                    )
                    print(f"[NODE] Enviei FLOOD para {neighbor_id}")
                except Exception as e:
                    print(f"[NODE] Falha ao enviar FLOOD para {neighbor_id}: {e}")
            time.sleep(interval)

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

        else:
            # Caso 2: >1 vizinho, mandar tabela de vizinhos
            print(f"[Node] {self.node_id} enviando tabela de vizinhos antes de sair...")
            for v_id, v_info in list(self.neighbors.items()):
                # construir lista de vizinhos excluindo o próprio destinatário e o nó que sai
                neighbor_list = [
                    {"id": nid, "ip": info["ip"], "port": info["port"]}
                    for nid, info in self.neighbors.items()
                    if nid != v_id  # exclui destinatário
                ]
                try:
                    TCPTransport.send(v_info["socket"], LeaveMessage(self.node_id, neighbor_list).to_json().encode())
                    print(f"[Node] Avisei {v_id} com a lista de vizinhos.")
                except Exception as e:
                    print(f"[Node] Erro ao avisar {v_id}: {e}")

            # esperar alguns segundos para que os vizinhos se reconectem
            time.sleep(2)
            # fechar todas as conexões e parar
            for v_id, v_info in list(self.neighbors.items()):
                try:
                    v_info["socket"].close()
                except:
                    pass
            self.neighbors.clear()
            self.transport.stop_listening()
            print(f"[Node] {self.node_id} saiu da rede com sucesso.")

    def update_route(self, route_entry):
        """
        Atualiza a routing_table de forma atómica:
        - se já existir uma rota da mesma origin vinda do mesmo antecessor, atualiza timestamp/metric/state
        - se existir outra rota para origin com pior métrica, substitui-a (ou mantém ambas se vier de antecessores diferentes)
        """
        with self.lock:
            origin = route_entry["origin"]
            sender = route_entry["sender"]
            metric = route_entry["metric"]
            # procura rota igual (mesmo origin + antecessor)
            for r in self.routing_table:
                if r["origin"] == origin and r["sender"] == sender:
                    # actualiza se metric mudou/menor ou apenas timestamp
                    r["metric"] = metric
                    r["timestamp"] = route_entry.get("timestamp", time.time())
                    r["state"] = route_entry.get("state", r.get("state", "INACTIVE"))
                    return
            # se não encontrou, adiciona nova entrada
            self.routing_table.append(route_entry)

    def get_best_route(self, origin_id):
        with self.lock:
            candidate_routes = [route for route in self.routing_table if route["origin"] == origin_id]
            if not candidate_routes:
                return None
            # preferir menor metric; se empate, preferir mais recente
            best_route = min(candidate_routes, key=lambda r: (r["metric"], -r.get("timestamp", 0)))
            return best_route

    def handle_message(self, sock, data):
        """Callback para mensagens recebidas"""
        if not data:
            return

        try:
            msg_obj = parse_message(data.decode())

            # When we have messages that include node_id (Hello/HelloBack), we can map sock->node_id
            if isinstance(msg_obj, WelcomeMessage):
                self.node_id = msg_obj.node_id
                for n in msg_obj.neighbors:
                    n_id = n["id"]
                    self.overlay_mapping[n_id] = {"host": n["host"], "port": n["port"]}
                    # conecta se quiser já
                    peer_sock = self.transport.connect(n["host"], n["port"])
                    if peer_sock:
                        self.neighbors[n_id] = {"ip": n["host"], "port": n["port"], "socket": peer_sock, "established": False}
                        self.sock_to_id[peer_sock] = n_id
                        TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())


            elif isinstance(msg_obj, HelloMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLO de {sender_id}")

                # Se ainda não temos a conexão com esse nó, registramos
                if sender_id not in self.neighbors:
                    if sender_id in self.overlay_mapping:
                        host = self.overlay_mapping[sender_id]["host"]
                        port = self.overlay_mapping[sender_id].get("port", MY_PORT)  # fallback
                        self.neighbors[sender_id] = {"ip": host, "port": port, "socket": sock, "established": True}
                    else:
                        print(f"[Node] ERRO: sender_id {sender_id} desconhecido no overlay mapping")

                # mapear socket para node_id (útil para saber quem enviou mensagens futuras)
                self.sock_to_id[sock] = sender_id

                TCPTransport.send(sock, HelloBackMessage(self.node_id).to_json().encode())

            elif isinstance(msg_obj, HelloBackMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLOBACK de {sender_id}")
                # Marca conexão como estabelecida ou atualiza estado
                if sender_id in self.neighbors:
                    self.neighbors[sender_id]["established"] = True
                    self.sock_to_id[self.neighbors[sender_id]["socket"]] = sender_id
                else:
                    # Se chegou HELLOBACK em socket desconhecido, registamos
                    self.sock_to_id[sock] = sender_id
                    # Obtemos IP/PORT do bootstrapper topo
                    if sender_id in self.overlay_mapping:   # overlay_mapping = node_id -> {"host":..., "port": ...}
                        host = self.overlay_mapping[sender_id]["host"]
                        port = self.overlay_mapping[sender_id]["port"]
                        self.neighbors[sender_id] = {"ip": host, "port": port, "socket": sock, "established": True}
                    else:
                        print(f"[Node] ERRO: sender_id {sender_id} desconhecido no overlay mapping")
                            
            elif isinstance(msg_obj, LeaveMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi LEAVE de {sender_id}")

                # Remove o nó que saiu da tabela de vizinhos e fecha socket
                if sender_id in self.neighbors:
                    try:
                        self.neighbors[sender_id]["socket"].close()
                    except:
                        pass
                    # também limpar sock_to_id
                    try:
                        s = self.neighbors[sender_id]["socket"]
                        if s in self.sock_to_id:
                            del self.sock_to_id[s]
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
                            self.neighbors[n_id] = {"ip": n["ip"], "port": int(n["port"]), "socket": peer_sock}
                            self.sock_to_id[peer_sock] = n_id
                            TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                            print(f"[Node] Conectei-me a novo vizinho {n_id}")

            elif isinstance(msg_obj, FloodMessage):
                origin = msg_obj.origin
                sender = msg_obj.sender
                jumps = msg_obj.jumps

                new_cost = jumps + 1

                # Policing simples para evitar repropagação infinita:
                # Se já vimos uma métrica menor ou igual recentemente para essa origem,
                # e vier de mesmo sender com metric pior/igual, ignoramos reenvio.
                # Atualizamos recent_best_metric quando vemos uma melhoria.
                best = self.recent_best_metric.get(origin)
                now = time.time()
                ignore = False
                if best:
                    # se a nova métrica não melhora o melhor, e veio do mesmo sender, descartamos
                    if new_cost >= best["metric"] and sender == best.get("sender"):
                        ignore = True
                    # se veio com métrica pior e não é melhora razoável, podemos descartar propagar
                    if new_cost >= best["metric"] + 3:  # heurística: salto muito maior → ignora
                        ignore = True

                if ignore:
                    print(f"[NODE] Ignorando FLOOD de {sender} para origin {origin} (metric {new_cost}) — já temos melhor.")
                    # ainda podemos atualizar a tabela de rotas local se for nova sender
                    route_entry = {
                        "origin": origin,
                        "sender": sender,
                        "metric": new_cost,
                        "state": "INACTIVE",
                        "timestamp": now
                    }
                    self.update_route(route_entry)
                    return

                # Marca melhor métric/ultima visão
                self.recent_best_metric[origin] = {"metric": new_cost, "timestamp": now, "sender": sender}

                # Adiciona a nova rota descoberta à lista
                route_entry = {
                    "origin": origin,
                    "sender": sender,
                    "metric": new_cost,
                    "state": "INACTIVE",
                    "timestamp": now
                }
                self.update_route(route_entry)

                # Reencaminha para vizinhos (exceto quem enviou)
                with self.lock:
                    for neighbor_id, n_info in list(self.neighbors.items()):
                        if neighbor_id == sender:
                            continue
                        try:
                            TCPTransport.send(
                                n_info["socket"],
                                FloodMessage(origin=origin, sender=self.node_id, jumps=new_cost, metric=new_cost).to_json().encode()
                            )
                        except Exception as e:
                            print(f"[NODE] Falha ao reenviar FLOOD para {neighbor_id}: {e}")

                # Se eu for client, e não houver rota ACTIVE ainda, disparo Activate pela melhor rota
                if self.my_role == "client":
                    best_route = self.get_best_route(origin)
                    if best_route and best_route["state"] == "INACTIVE":
                        print(f"[CLIENT] Recebi flood de {origin}, ativando rota...")
                        activate = ActivateMessage(origin, self.node_id, reverse_path=[])
                        # envia Activate em direção à origem
                        next_hop = best_route["sender"]
                        if next_hop in self.neighbors:
                            try:
                                TCPTransport.send(
                                    self.neighbors[next_hop]["socket"],
                                    activate.to_json().encode()
                                )
                                # marca como ativa localmente
                                with self.lock:
                                    best_route["state"] = "ACTIVE"
                                print(f"[CLIENT] Enviou ACTIVATE para {next_hop}")
                            except Exception as e:
                                print(f"[CLIENT] Falha ao enviar ACTIVATE: {e}")

            elif isinstance(msg_obj, ActivateMessage):
                origin = msg_obj.origin          # servidor / origem
                client_id = msg_obj.client_id    # quem iniciou a ativação
                reverse_path = msg_obj.reverse_path or []

                # Adiciona o nó atual ao reverse_path
                if self.node_id not in reverse_path:
                    reverse_path.append(self.node_id)

                print(f"[Node] Recebi ACTIVATE para origin={origin}, cliente={client_id}, path={reverse_path}")

                # Se já estivermos no reverse_path antes, há ciclo → descarta
                if reverse_path.count(self.node_id) > 1:
                    print(f"[Node] Ciclo detectado em ACTIVATE — ignorando (path={reverse_path})")
                    return

                # Registrar quem nos enviou essa Activate como "child" do origin (se soubermos)
                prev_hop = self.sock_to_id.get(sock)
                if prev_hop:
                    with self.lock:
                        self.children.setdefault(origin, set()).add(prev_hop)
                        print(f"[Node] Registrei {prev_hop} como child de {origin}")

                # Marca a rota para origin como ativa (caso já exista)
                best = self.get_best_route(origin)
                if best:
                    with self.lock:
                        best["state"] = "ACTIVE"
                        best["timestamp"] = time.time()

                # Caso sejamos a origem (servidor), terminamos aqui
                if self.node_id == origin:
                    print(f"[Node] Sou a origem ({self.node_id}). Caminho reverso completo recebido de {client_id}: {reverse_path}")
                    # servidor pode armazenar reverse_path[1:] como caminho até o cliente
                    # e iniciar envio de dados.
                    return

                # Caso contrário, encaminhar para o próximo salto em direção à origem
                next_hop = None
                if best:
                    next_hop = best["sender"]

                if next_hop and next_hop in self.neighbors:
                    try:
                        sock_next = self.neighbors[next_hop]["socket"]
                        # Encaminhar mensagem atualizada com reverse_path
                        TCPTransport.send(sock_next, ActivateMessage(origin, client_id, reverse_path).to_json().encode())
                        print(f"[Node] Reencaminhei ACTIVATE para {next_hop}")
                    except Exception as e:
                        print(f"[Node] Erro ao reencaminhar ACTIVATE para {next_hop}: {e}")
                else:
                    print(f"[Node] Não encontrei next_hop válido para {origin} — descarto ACTIVATE.")

            else:
                print(f"[Node] Recebi mensagem desconhecida: {msg_obj}")

        except Exception as e:
            print(f"[Node] Erro ao processar mensagem: {e}")


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Over the top application")
    parser.add_argument("--role", type=str, choices=["server", "client", "middle"], default="middle", help="server, client ou middle")
    parser.add_argument("--host", default="0.0.0.0", help="IP do node Host")
    parser.add_argument("--boot", dest="boot", default="0.0.0.0", help="IP do Bootstrapper")
    args = parser.parse_args()

    # corrigido: usar args.boot e passar porta 5000
    node = Node(args.host, (args.boot, 5000), args.role)
    node.start()
