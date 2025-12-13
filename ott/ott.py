import sys
import os
import time
import argparse
import threading
import math
import json
import random
from transport.tcp_transport import TCPTransport
from transport.udp_transport import UDPTransport
from message.message_types import HelloMessage, HelloBackMessage, JoinMessage, WelcomeMessage, FloodMessage, ActivateMessage, DangerMessage, SafeOptionMessage, HeartBeatActiveMessage, ClientStreamRequest
from message.parser import parse_message
from metrics.metrics import get_latency, get_packet_loss, get_bandwidth
from streaming.VideoStream import VideoStream
from streaming.RtpPacket import RtpPacket


MY_PORT = 5000
MY_OTHER_PORT = 5001

class Node:
    def __init__(self, my_ip, bootstrapper_addr, my_role):
        self.my_ip = my_ip
        self.my_role = my_role

        self.node_id = f"{my_ip}:{MY_PORT}"
        self.bootstrapper_addr = bootstrapper_addr
        self.lock = threading.Lock()
        self.tcp_transport = TCPTransport(my_ip, MY_PORT, self.handle_message)
        self.udp_transport = UDPTransport(self.my_ip, MY_OTHER_PORT, self.handle_stream_data)

        # id do overlay -> {"host": ip, "port": port} usado para solucionar os routers com + do que um endereço
        self.overlay_mapping = {}  

        # origin -> set(dos vizinhos que dependem de mim para chegar à origem)
        self.forwarding_table = {}

        # node_id -> {"ip": ip, "port": port, "socket": sock, "established": bool}
        self.neighbors = {}

        # routing table entries: dicts with keys origin, sender, metric, state, timestamp
        self.routing_table = []

        # socket -> node_id mapping (para identificar o remetente dos pacotes)
        self.sock_to_id = {}

        # origin -> {"metric": best_metric, "timestamp": last_seen}
        self.recent_best_metric = {}

        # lista de nodos que disseram "estou em perigo"
        self.danger = []    

        # nodo seguro para onde devo fugir se o meu antecessor morrer
        self.safe_option = None 

        self.ready_after_welcome = False
        self.pending_danger = False

        self.video_player = None
        self.tk_root = None

        self.last_seen = {}   # node_id -> last heartbeat timestamp

        self.external_clients = {}  

        self.pending_any_stream_clients = set()

        # origin -> node_id do meu pai (o next-hop preferido para esse origin)
        self.parent_of = {}

        # origin -> node_id do avô (grandparent) para fallback
        self.grandparent_of = {}



    def start(self):
        self.tcp_transport.start_listening() 
        self.udp_transport.start_listening() 
        time.sleep(0.5)
        self.bootstrap()
        print(f"[Node] {self.node_id} rodando. Ctrl+C para sair.")
        time.sleep(1)


        threading.Thread(target=self.start_active_hb_sender_loop, daemon=True).start()
        threading.Thread(target=self.start_maintenance_loop, daemon=True).start()
        
        if self.my_role == "server":
            threading.Thread(target=self.periodic_flood, daemon=True).start()
            threading.Thread(target=self.start_rtp_stream, args=(self.node_id,), daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            sys.exit(0)


    def bootstrap(self):
        sock = self.tcp_transport.connect(*self.bootstrapper_addr)
        if not sock:
            print("[Bootstrapper] Falha ao conectar")
            return
        physical_id = f"{self.my_ip}:{MY_PORT}"
        TCPTransport.send(sock, JoinMessage(physical_id).to_json().encode())
        self.sock_to_id[sock] = None
        time.sleep(0.5)

    def start_active_hb_sender_loop(self):
        while True:
            try:
                time.sleep(1)

                # Iterar sobre os vizinhos
                for node_id, info in list(self.neighbors.items()):
                    sock = info.get("socket")
                    is_established = info.get("established", False)

                    if not sock or not is_established:
                        continue

                    try:
                        TCPTransport.send(
                            sock, 
                            HeartBeatActiveMessage(timestamp=time.time(), node_id=self.node_id).to_json().encode()
                        )
                    except Exception as e:
                        print(f"[HB-SEND] Falha ao enviar HB para {node_id}: {e}")

            except Exception as e:
                print(f"[ERROR] start_active_hb_sender_loop: {e}")


    def start_maintenance_loop(self):
        timeout_limit = 10

        while True:
            time.sleep(2)
            now = time.time()

            # === TIMEOUT DE HEARTBEAT ===  
            for node_id, last in list(self.last_seen.items()):
                if now - last > timeout_limit:
                    print(f"[FAILURE] Vizinho {node_id} está inativo (timeout).")
                    self.handle_neighbor_failure(node_id)

            # === DETETAR ISOLAMENTO TOTAL ===
            with self.lock:
                neighbors_copy = dict(self.neighbors)

            total = len(neighbors_copy)
            dead_count = 0

            for node_id, info in neighbors_copy.items():
                if info.get("established") is False:
                    dead_count += 1

            # Se todos estão mortos, tenta reconectar
            if total > 0 and dead_count == total:
                print(f"[{self.node_id}] ISOLADO! A tentar reconectar...")
                self.try_reconnect()


    def handle_neighbor_failure(self, failed_node_id):
        print(f"[FAILURE] Vizinho {failed_node_id} considerado morto.")
        
        affected_origins = set()
        recommended_targets = {}  # origin → grandparent

        # Remover vizinho e recolher origins afetados
        with self.lock:

            # desligar neighbor
            if failed_node_id in self.neighbors:
                try:
                    sock = self.neighbors[failed_node_id].get("socket")
                    if sock:
                        sock.close()   # FECHA A CONEXÃO TCP CORRETAMENTE
                except:
                    pass

                self.neighbors[failed_node_id]["established"] = False
                
            # remover last_seen
            if failed_node_id in self.last_seen:
                del self.last_seen[failed_node_id]

            # identificar origins cujas rotas dependiam do falhado
            for route in self.routing_table:
                if route["sender"] == failed_node_id:
                    affected_origins.add(route["origin"])

            # limpar rotas vindas do falhado
            self.routing_table = [
                r for r in self.routing_table if r["sender"] != failed_node_id
            ]

            # limpar forwarding table
            for origin, deps in self.forwarding_table.items():
                if failed_node_id in deps:
                    deps.remove(failed_node_id)

        if self.my_role != "server":
            # Para cada origin verificar alternativas
            for origin in affected_origins:

                best = self.get_best_route(origin)

                if best:
                    # rota alternativa existe → basta ativar
                    new_parent = best["sender"]
                    print(f"[RECOVERY] Para origin {origin}, rota alternativa via {new_parent}")

                    if new_parent in self.neighbors:
                        try:
                            TCPTransport.send(
                                self.neighbors[new_parent]["socket"],
                                ActivateMessage(origin, self.node_id).to_json().encode()
                            )
                        except:
                            print(f"[RECOVERY] Falha ao enviar ACTIVATE para {new_parent}")

                    continue

                gp = self.grandparent_of.get(origin)

                if not gp:
                    print(f"[FAILURE] Origin {origin} perdeu rota e não tem grandparent disponível!")
                    continue

                # Conectar ao grandparent
                if gp not in self.overlay_mapping:
                    print(f"[FAILURE] Não tenho informação de overlay para grandparent {gp}")
                    continue

                info = self.overlay_mapping[gp]
                gp_host = info.get("host")
                gp_port = info.get("port")
                print(f"[RECOVERY] A ligar ao avô {gp} para origin {origin} ({gp_host}:{gp_port})")

                gp_sock = self.tcp_transport.connect(gp_host, gp_port)
                if gp_sock:
                    with self.lock:
                        self.neighbors[gp] = {
                            "ip": gp_host,
                            "port": gp_port,
                            "socket": gp_sock,
                            "established": False
                        }
                        self.sock_to_id[gp_sock] = gp
                        self.forwarding_table.setdefault(origin, set()).add(gp)

                    TCPTransport.send(gp_sock, HelloMessage(self.node_id, self.my_ip, MY_PORT).to_json().encode())
                else:
                    print(f"[FAILURE] Não consegui ligar ao grandparent {gp}")


    def try_reconnect(self):
        with self.lock:
            overlay_copy = dict(self.overlay_mapping)

        for other_id, info in overlay_copy.items():

            if other_id == self.node_id:
                continue

            host = info["host"]
            port = info["port"]

            print(f"[{self.node_id}] A tentar reconectar a {other_id} ({host}:{port})...")

            sock = self.tcp_transport.connect(host, port)
            if sock:
                with self.lock:
                    self.neighbors[other_id] = {
                        "ip": host,
                        "port": port,
                        "socket": sock,
                        "established": False
                    }
                    self.sock_to_id[sock] = other_id

                TCPTransport.send(
                    sock,
                    HelloMessage(self.node_id, self.my_ip, MY_PORT).to_json().encode()
                )

                print(f"[{self.node_id}] Reconexão iniciada com {other_id}")
                return

        print(f"[{self.node_id}] Falha na reconexão: nenhum nó disponível.")


            
    def periodic_flood(self, interval=10):
        while True:
            start_jump = 0
            with self.lock:
                neighbors_items = list(self.neighbors.items())
            for neighbor_id, n_info in neighbors_items:
                if not n_info.get("established", False):
                    continue
                try:
                    TCPTransport.send(
                        n_info["socket"],
                        FloodMessage(origin=self.node_id, sender=self.node_id, grandparent = None, jumps=start_jump, loss=0.0, bandwidth=math.inf, latency=0.0).to_json().encode()
                    )
                    print(f"[NODE] Enviei FLOOD para {neighbor_id}")
                except Exception as e:
                    print(f"[NODE] Falha ao enviar FLOOD para {neighbor_id}: {e}")
            time.sleep(interval)


    def update_route(self, route_entry):
        with self.lock:
            origin = route_entry["origin"]
            sender = route_entry["sender"]
            # procura rota igual (mesmo origin + antecessor)
            for i, r in enumerate(self.routing_table):
                if r["origin"] == origin and r["sender"] == sender:
                    self.routing_table[i] = route_entry  # substitui métricas
                    return
            # se não encontrou, adiciona nova entrada
            self.routing_table.append(route_entry)


    def get_best_route(self, origin_id):
        with self.lock:
            candidate_routes = [
                route for route in self.routing_table
                if route["origin"] == origin_id
            ]

            if not candidate_routes:
                return None

            # prefere o menor metric; se empatar, prefere o mais recente
            best_route = min(
                candidate_routes,
                key=lambda r: (r["metric"], -r["timestamp"])
            )

            return best_route


    def compute_metric(self, latency, loss, bandwidth):
        if math.isinf(bandwidth):  # evita -inf
            bandwidth_val = 1000  # assume alta largura de banda se ainda neutra
        else:
            bandwidth_val = bandwidth

        return latency + loss*10 - bandwidth_val/10


    def start_rtp_stream(self, origin_id):
        if self.my_role != "server" or self.node_id != origin_id:
            return

        print(f"[STREAM] A iniciar stream RTP para a origem {origin_id}...")
        
        streams_dir = "streams"

        # 1. Validar se a pasta existe e tem ficheiros
        if not os.path.exists(streams_dir) or not os.path.isdir(streams_dir):
            print(f"[ERRO] A pasta '{streams_dir}' não existe na raiz do projeto.")
            return

        # Lista apenas ficheiros .Mjpeg
        valid_files = [f for f in os.listdir(streams_dir) if f.endswith(".Mjpeg") or f.endswith(".mjpeg")]
        
        if not valid_files:
            print(f"[ERRO] Nenhum ficheiro .Mjpeg encontrado na pasta '{streams_dir}'.")
            return

        try:
            # 2. Escolher o vídeo aleatoriamente UMA VEZ (antes do loop)
            selected_video = random.choice(valid_files)
            video_path = os.path.join(streams_dir, selected_video)
            print(f"[STREAM] Conteúdo selecionado para este canal: '{selected_video}'")
            
            # Carrega o vídeo selecionado
            video_stream = VideoStream(video_path)
            
            while True:
                # 3. Obter o próximo frame
                frame_data = video_stream.nextFrame()

                # 4. Se o vídeo acabou, reiniciar O MESMO vídeo
                if not frame_data:
                    print(f"[STREAM] Fim do vídeo. A reiniciar '{selected_video}'...")
                    # Recarrega o mesmo ficheiro para voltar ao início
                    video_stream = VideoStream(video_path)
                    continue

                frame_number = video_stream.frameNbr()

                with self.lock:
                    # Verifica se há clientes ativos (dependentes diretos)
                    dependents = self.forwarding_table.get(origin_id, set())

                # 5. Enviar apenas se houver alguém a ouvir
                if dependents:
                    rtp_packet = RtpPacket()
                    rtp_packet.encode(version=2, padding=0, extension=0, cc=0, seqnum=frame_number, 
                                      marker=0, pt=26, ssrc=0, payload=frame_data)
                    packet_bytes = rtp_packet.getPacket()

                    for dependent_id in dependents:
                        if dependent_id in self.neighbors:
                            n_info = self.neighbors[dependent_id]
                            self.udp_transport.send(n_info["ip"], MY_OTHER_PORT, packet_bytes)

                # Controla a taxa de frames (20 fps = 0.05s)
                time.sleep(0.05)
            
        except Exception as e:
            print(f"[ERRO] Falha no streaming RTP: {e}")
            import traceback
            traceback.print_exc()


        
    def handle_stream_data(self, data: bytes, sender_addr: tuple):
            try:
                # Esta parte do 'if self.my_role == client' já não é usada se usares o client.py separado, 
                # mas podes manter ou apagar. O importante é o que vem abaixo.

                all_dependents_overlay = set()
                all_clients_external = set()

                with self.lock:
                    # 1. Recolher dependentes da overlay (vizinhos)
                    for origin in self.forwarding_table:
                        all_dependents_overlay.update(self.forwarding_table[origin])
                    
                    # 2. Recolher clientes externos (AGORA NUM LOOP SEPARADO)
                    # Isto garante que mesmo que não haja vizinhos overlay, servimos os clientes.
                    for origin in self.external_clients:
                        all_clients_external.update(self.external_clients[origin])

                # Reencaminhar para vizinhos da Overlay
                for dependent_id in all_dependents_overlay:
                    if dependent_id in self.neighbors:
                        n_info = self.neighbors[dependent_id]
                        self.udp_transport.send(n_info["ip"], MY_OTHER_PORT, data)

                # Reencaminhar para Clientes Externos
                for client_ip in all_clients_external:
                    # Envia para a porta 5001 (MY_OTHER_PORT) do cliente
                    self.udp_transport.send(client_ip, MY_OTHER_PORT, data)
                    
            except Exception as e:
                # print(f"[{self.node_id}] Error in handle_stream_data: {e}")
                pass


    def get_grandparent(self):
        for origin in self.forwarding_table:
            best = self.get_best_route(origin)
            if best:
                parent = best["sender"]
                if parent != self.node_id:
                    return parent
        return None


    def catastrophic_warning(self):
        print(f"[{self.node_id}] Estou em perigo! A avisar o meu vizinho principal...")

        for origin, dependents in self.forwarding_table.items():
            best = self.get_best_route(origin)
            if best and best["sender"] in self.neighbors:
                parent = best["sender"]
                TCPTransport.send(
                    self.neighbors[parent]["socket"],
                    DangerMessage(self.node_id).to_json().encode()
                )
                return

        print("Não encontrei antecessor para avisar!")


    def handle_message(self, sock, data):
        if not data:
            return

        try:
            msg_obj = parse_message(data.decode())

            if isinstance(msg_obj, WelcomeMessage):
                self.node_id = msg_obj.node_id
                for n in msg_obj.neighbors:
                    n_id = n["id"]
                    self.overlay_mapping[n_id] = {"host": n["host"], "port": n["port"]}
                    peer_sock = self.tcp_transport.connect(n["host"], n["port"])
                    if peer_sock:
                        self.neighbors[n_id] = {"ip": n["host"], "port": n["port"], "socket": peer_sock, "established": False}
                        self.sock_to_id[peer_sock] = n_id
                        TCPTransport.send(peer_sock, HelloMessage(self.node_id, self.my_ip, MY_PORT ).to_json().encode())
                self.ready_after_welcome = True

                if len(msg_obj.neighbors) == 1 and self.my_role != "server":
                    print(f"[Node] Sou folha após o WELCOME — posso entrar em perigo quando HELLO estabelecer.")
                    self.pending_danger = True


            elif isinstance(msg_obj, HelloMessage):
                sender_id = msg_obj.node_id
                sender_ip = msg_obj.ip
                sender_port = msg_obj.port
                print(f"[Node] Recebi HELLO de {sender_id}")

                if sender_id == self.node_id:
                    return  

                with self.lock:
                    self.neighbors[sender_id] = {
                        "ip": sender_ip,
                        "port": sender_port,
                        "socket": sock,
                        "established": True
                    }

                    self.overlay_mapping[sender_id] = {
                        "host": sender_ip,
                        "port": sender_port
                    }

                    self.last_seen[sender_id] = time.time()

                    self.sock_to_id[sock] = sender_id

                TCPTransport.send(
                    sock,
                    HelloBackMessage(self.node_id, self.my_ip, MY_PORT).to_json().encode()
                )

                if self.ready_after_welcome and self.pending_danger:
                    self.pending_danger = False
                    self.catastrophic_warning()


            elif isinstance(msg_obj, HelloBackMessage):
                sender_id = msg_obj.node_id
                sender_ip = msg_obj.ip
                sender_port = msg_obj.port
                print(f"[Node] Recebi HELLOBACK de {sender_id}")

                with self.lock:
                    self.neighbors[sender_id] = {
                        "ip": sender_ip,
                        "port": sender_port,
                        "socket": sock,
                        "established": True
                    }

                    self.overlay_mapping[sender_id] = {
                        "host": sender_ip,
                        "port": sender_port
                    }

                    self.last_seen[sender_id] = time.time()

                    self.sock_to_id[sock] = sender_id

            
            elif isinstance(msg_obj, FloodMessage):
                origin = msg_obj.origin
                sender = msg_obj.sender
                grandparent = msg_obj.grandparent

                grandparent_id = grandparent.get("id") if grandparent else None
                
                if grandparent:
                    gp_id = grandparent["id"]
                    gp_host = grandparent["ip"]
                    gp_port = grandparent["port"]
                    
                    if gp_id not in self.overlay_mapping: 
                        self.overlay_mapping[gp_id] = {
                            "host": gp_host, 
                            "port": gp_port
                        }

                if sender in self.neighbors:
                    host = self.neighbors[sender]["ip"]
                else:
                    host = None 

                jumps = msg_obj.jumps + 1

                # Valores antigos da mensagem
                prev_latency = msg_obj.latency or 0
                prev_loss = msg_obj.loss or 0
                prev_bandwidth = msg_obj.bandwidth or math.inf

                # Métricas do link atual
                if host:
                    link_latency = get_latency(host) or 0
                    link_loss = get_packet_loss(host) or 0
                    link_bandwidth = get_bandwidth(host) or 0
                else:
                    link_latency = 0
                    link_loss = 0
                    link_bandwidth = 1000

                # Combina cumulativamente
                latency = prev_latency + link_latency
                loss = prev_loss + link_loss
                bandwidth = min(prev_bandwidth, link_bandwidth) if prev_bandwidth else link_bandwidth

                now = time.time()
                new_metric = self.compute_metric(latency, loss, bandwidth)
                route_entry = {
                    "origin": origin,
                    "sender": sender,
                    "grandparent": grandparent_id,
                    "metric": new_metric,
                    "state": "INACTIVE",
                    "timestamp": now
                }
                self.update_route(route_entry)
                print(f"[Node] Atualizei tabela para {origin} via {sender} (metric={new_metric})")


                best = self.get_best_route(origin)
                # Se a rota recém-chegada é a melhor, reenvia Flood
                if best["sender"] == sender:  
                    best["state"] = "ACTIVE" 
                    print(f"[Node] Melhor rota para {origin} via {sender} (metric={new_metric})")

                    for r in self.routing_table:
                        if r["origin"] == origin and r["sender"] != best["sender"]:
                            r["state"] = "INACTIVE"


                    # Reenvia para vizinhos, exceto quem enviou
                    with self.lock:
                        self.parent_of[origin] = sender
                        self.grandparent_of[origin] = grandparent_id

                        next_gp = sender

                        info = self.overlay_mapping[next_gp]
                        recommended_target = {
                            "id": sender,
                            "ip": info["host"],
                            "port": info["port"]
                        }

                        for neighbor_id, n_info in self.neighbors.items():
                            if neighbor_id != sender:
                                if not n_info.get("established", False):
                                    continue
                                try:
                                    TCPTransport.send(
                                        n_info["socket"],
                                        FloodMessage(origin=origin, sender=self.node_id,grandparent = recommended_target, jumps=jumps, loss=loss, bandwidth=bandwidth, latency=latency).to_json().encode()
                                    )
                                except Exception as e:
                                    print(f"[NODE] Falha ao reenviar FLOOD para {neighbor_id}: {e}")

                    # Se for client, dispara activate na melhor rota
                    with self.lock:
                        if self.pending_any_stream_clients:
                            print(f"[NODE] Tenho clientes à espera e recebi rota para {origin}. Ativando...")
                            
                            # 1. Adicionar clientes à lista deste stream
                            if origin not in self.external_clients:
                                self.external_clients[origin] = set()
                            
                            self.external_clients[origin].update(self.pending_any_stream_clients)
                            self.pending_any_stream_clients.clear()
                            
                            # 2. Enviar ACTIVATE para cima (para o sender desta mensagem)
                            try:
                                # O socket do sender está na lista de neighbors
                                if sender in self.neighbors:
                                    sender_sock = self.neighbors[sender]["socket"]
                                    TCPTransport.send(
                                        sender_sock,
                                        ActivateMessage(origin=origin, client_id=self.node_id).to_json().encode()
                                    )
                                    print(f"[NODE] Auto-Activate enviado para {sender}")
                                    
                                    # Marcar rota como ativa localmente
                                    best["state"] = "ACTIVE"
                            except Exception as e:
                                print(f"[NODE] Erro no Auto-Activate: {e}")

            elif isinstance(msg_obj, ActivateMessage):
                origin = msg_obj.origin
                client_id = msg_obj.client_id
                
                print(f"[{self.node_id}] Recebi ACTIVATE para origin={origin}, cliente={client_id}")

                prev_hop = self.sock_to_id.get(sock)
                if prev_hop:
                    with self.lock:
                        self.forwarding_table.setdefault(origin, set()).add(prev_hop)
                        print(f"    ->[AVISO] Adicionei '{prev_hop}' como dependente para '{origin}'. FWD Table: {self.forwarding_table}")
                else:
                    print(f"    -> [AVISO] Não identifiquei remetente do ACTIVATE. FWD table não foi atualizada.")
                    # Se não sabemos quem pediu, não podemos continuar a cadeia.
                    return

                if self.node_id == origin: 
                    print(f"    -> [AVISO] Sou o origin ({self.node_id}). Não reencaminho ACTIVATE.")
                    return
                
                best_route = self.get_best_route(origin)
                if best_route:
                    next_hop = best_route["sender"]
                
                if next_hop in self.neighbors:
                    print(f"    ->[AVISO] A reencaminhar ACTIVATE para o meu antecessor: {next_hop}")
                    try:
                        TCPTransport.send(self.neighbors[next_hop]["socket"], ActivateMessage(origin, msg_obj.client_id).to_json().encode())
                    except: pass


            elif isinstance(msg_obj, DangerMessage):
                danger_node = msg_obj.node_id
                print(f"[{self.node_id}] Recebi DANGER de {danger_node}")

                with self.lock:
                    # Já tenho outros ameaçados?
                    if not self.danger:
                        # Lista vazia → enviar avô
                        safe = self.get_grandparent()
                        self.danger.append(danger_node)
                        print(f"[{self.node_id}] danger vazio → safe_option para {danger_node} é {safe}")
                    else:
                        # Lista cheia → enviar o último!
                        safe = self.danger[-1]
                        self.danger.append(danger_node)
                        print(f"[{self.node_id}] danger já tem elementos → safe_option para {danger_node} é {safe}")

                # Envia a mensagem SafeOption de volta ao nodo em perigo
                try:
                    TCPTransport.send(
                        self.neighbors[danger_node]["socket"],
                        SafeOptionMessage(self.node_id, safe).to_json().encode()
                    )
                except:
                    print(f"[{self.node_id}] Falha ao enviar safe_option para {danger_node}")


            elif isinstance(msg_obj, SafeOptionMessage):
                self.safe_option = msg_obj.safe
                print(f"[{self.node_id}] safe_option atualizado: agora devo reconectar a {self.safe_option} se houver falha.")
        
            elif isinstance(msg_obj, HeartBeatActiveMessage):
                sender_id = msg_obj.node_id
                self.last_seen[sender_id] = time.time()
                return

            elif isinstance(msg_obj, ClientStreamRequest):
                stream_id = msg_obj.stream_id
                client_ip, _ = sock.getpeername()
                
                if stream_id is None:
                    print(f"[CLIENT-REQ] Cliente externo {client_ip} pediu QUALQUER stream.")
                    
                    # 1. Verificar se já temos alguma rota ativa na tabela
                    # Vamos tentar encontrar a melhor rota disponível de todas
                    best_route_found = None
                    
                    # Procura na routing table a primeira rota válida
                    # (Poderíamos melhorar para escolher a com menor métrica de todas, mas a primeira serve)
                    if self.routing_table:
                        # Ordena por métrica para dar a melhor rede disponível
                        sorted_routes = sorted(self.routing_table, key=lambda x: x['metric'])
                        best_route_found = sorted_routes[0] # Pega na melhor
                    
                    if best_route_found:
                        stream_id = best_route_found["origin"]
                        print(f"[CLIENT-REQ] Atribuindo stream '{stream_id}' ao cliente {client_ip}")
                        
                        # Adicionar aos clientes externos desse stream
                        with self.lock:
                            self.external_clients.setdefault(stream_id, set()).add(client_ip)
                        
                        # Envia ACTIVATE para cima
                        parent = best_route_found["sender"] # ou 'antecessor' na vossa estrutura antiga, verifiquem chaves
                        if parent in self.neighbors:
                            try:
                                TCPTransport.send(
                                    self.neighbors[parent]["socket"], 
                                    ActivateMessage(origin=stream_id, client_id=self.node_id).to_json().encode()
                                )
                                print(f"[CLIENT-REQ] Enviado ACTIVATE para {parent} (Stream: {stream_id})")
                            except Exception as e:
                                print(f"[CLIENT-REQ] Erro ao ativar rota: {e}")
                    else:
                        # Se não há rotas, guarda o cliente para depois
                        print(f"[CLIENT-REQ] Nenhuma rota disponível ainda. Cliente {client_ip} em espera.")
                        with self.lock:
                            self.pending_any_stream_clients.add(client_ip)

                else:
                    # LÓGICA ANTIGA (PEDIDO ESPECÍFICO)
                    print(f"[CLIENT-REQ] Cliente externo {client_ip} pediu stream {stream_id}")
                    with self.lock:
                        self.external_clients.setdefault(stream_id, set()).add(client_ip)

                    best = self.get_best_route(stream_id)
                    if best:
                        parent = best["sender"]
                        if parent in self.neighbors:
                            print(f"[CLIENT-REQ] Enviando ACTIVATE para {parent}")
                            try:
                                TCPTransport.send(
                                    self.neighbors[parent]["socket"], 
                                    ActivateMessage(origin=stream_id, client_id=self.node_id).to_json().encode()
                                )
                            except Exception as e:
                                print(f"[CLIENT-REQ] Erro ao ativar rota: {e}")
                    else:
                        print(f"[CLIENT-REQ] Erro: Não tenho rota para {stream_id}")
                
        except Exception as e:
            print(f"[Node] Erro ao processar mensagem: {e}")
            import traceback
            traceback.print_exc()

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Over the top application")
    parser.add_argument("--role", type=str, choices=["server", "middle"], default="middle", help="server ou middle")
    parser.add_argument("--host", default="0.0.0.0", help="IP do node Host")
    parser.add_argument("--boot", dest="boot", default="0.0.0.0", help="IP do Bootstrapper")
    args = parser.parse_args()

    node = Node(args.host, (args.boot, 5000), args.role)
    node.start()