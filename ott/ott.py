import sys
import os
import time
import argparse
import threading
import math
from transport.tcp_transport import TCPTransport
from transport.udp_transport import UDPTransport
from message.message_types import HelloMessage, HelloBackMessage, JoinMessage, WelcomeMessage, LeaveMessage, FloodMessage, ActivateMessage
from message.parser import parse_message
from metrics.metrics import get_latency, get_packet_loss, get_bandwidth

# imports para stream
from streaming.VideoStream import VideoStream
from streaming.RtpPacket import RtpPacket
from tkinter import Tk, Label
from PIL import Image, ImageTk
import io
import base64

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

        # Tabela de forwarding: origin -> set(dos vizinhos que dependem de mim para chegar à origem)
        self.forwarding_table = {}

        # node_id -> {"ip": ip, "port": port, "socket": sock, "established": bool}
        self.neighbors = {}

        # routing table entries: dicts with keys origin, sender, metric, state, timestamp
        self.routing_table = []

        # socket -> node_id mapping (para identificar o remetente dos pacotes)
        self.sock_to_id = {}

        # Track best metric seen recently per origin (to help cortar repropagação/ciclos)
        # origin -> {"metric": best_metric, "timestamp": last_seen}
        self.recent_best_metric = {}

        # PARTE UDP
        self.udp_port = MY_PORT + 1
        self.udp_transport = UDPTransport(self.my_ip, self.udp_port, self.handle_stream_data)

        self.video_player = None
        self.tk_root = None
        if self.my_role == "client":
            try:
                # Inicia a parte gráfica numa thread separada para não bloquear a rede
                self.tk_root = Tk()
                self.video_player = VideoClientGUI(self.tk_root, self.graceful_leave)
            except Exception as e:
                print(f"[ERRO CRÍTICO NA GUI] Falha ao iniciar o Tkinter: {e}")
                import traceback
                traceback.print_exc()
                # Se a GUI falhar, definimos tk_root como None para não haver mais erros
                self.tk_root = None

    def _update_tkinter(self):
        try:
            if self.video_player:
                self.tk_root.update()
                self.tk_root.after(100, self._update_tkinter)
        except: # A janela pode ter sido fechada
            pass

    def start(self):
        self.transport.start_listening() # iniciar o tcp
        self.udp_transport.start_listening() # iniciar o udp
        time.sleep(0.5)
        self.bootstrap()
        print(f"[Node] {self.node_id} rodando. Ctrl+C para sair.")
        time.sleep(1)
      
        if self.my_role == "server":
            t = threading.Thread(target=self.periodic_flood, daemon=True)
            t.start()

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

    def periodic_flood(self, interval=20):
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
                        FloodMessage(origin=self.node_id, sender=self.node_id, jumps=start_jump, loss=0.0, bandwidth=math.inf, latency=0.0).to_json().encode()
                    )
                    print(f"[NODE] Enviei FLOOD para {neighbor_id}")
                except Exception as e:
                    print(f"[NODE] Falha ao enviar FLOOD para {neighbor_id}: {e}")
            time.sleep(interval)

    def graceful_leave(self):
        print(f"[Node] {self.node_id} iniciando graceful leave...")

        for neighbor_id, info in list(self.neighbors.items()):

            # Determinar se este vizinho depende de mim (forwarding_table)
            depends = False
            recommended_target = None

            for origin, dependents in self.forwarding_table.items():
                if neighbor_id in dependents:
                    depends = True

                    # Encontrar o "avô" para esta origem
                    best = self.get_best_route(origin)
                    if best:
                        avo_id = best["sender"]
                        if avo_id in self.neighbors:
                            avo_info = self.neighbors[avo_id]
                            recommended_target = {
                                "id": avo_id,
                                "ip": avo_info["ip"],
                                "port": avo_info["port"]
                            }
                            print(f"[Node] Recomendo {neighbor_id} → conectar ao avô {avo_id} (origin={origin})")

            if depends:
                print(f"[Node] {neighbor_id} depende de mim — enviando recommended avô")
                msg = LeaveMessage(self.node_id, recommended_target)
            else:
                print(f"[Node] {neighbor_id} NÃO depende de mim — enviando leave simples")
                msg = LeaveMessage(self.node_id, None)

            try:
                TCPTransport.send(info["socket"], msg.to_json().encode())
            except:
                pass

            try:
                info["socket"].close()
            except:
                pass

            del self.neighbors[neighbor_id]

        self.routing_table.clear()
        self.forwarding_table.clear()
        self.sock_to_id.clear()

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
    
    # ott.py -> Adicionar este novo método à classe Node

    def start_rtp_stream(self, origin_id, video_file="movie.Mjpeg"):
        """
        Inicia o streaming de vídeo. Lê frames, cria pacotes RTP binários
        e envia-os diretamente via UDP para os dependentes.
        """
        if self.my_role != "server" or self.node_id != origin_id:
            return

        print(f"[STREAM] A iniciar stream RTP para a origem {origin_id}...")
        
        try:
            video_stream = VideoStream(video_file)
            
            while True:
                with self.lock:
                    # Verifica se há clientes ativos (dependentes diretos)
                    dependents = self.forwarding_table.get(origin_id, set())
                
                if not dependents:
                    print("[STREAM] Nenhum cliente ativo. Stream em pausa.")
                    time.sleep(1) # Espera por um cliente
                    continue

                # 1. Obter o próximo frame do ficheiro de vídeo
                frame_data = video_stream.nextFrame()
                if not frame_data:
                    print("[STREAM] Fim do ficheiro de vídeo.")
                    break # Termina o loop de streaming

                frame_number = video_stream.frameNbr()

                # 2. Criar o pacote RTP binário
                rtp_packet = RtpPacket()
                rtp_packet.encode(version=2, padding=0, extension=0, cc=0, seqnum=frame_number, 
                                  marker=0, pt=26, ssrc=0, payload=frame_data)
                packet_bytes = rtp_packet.getPacket() # Estes são os bytes que vamos enviar

                # 3. Enviar os bytes do pacote RTP DIRETAMENTE via UDP para todos os dependentes
                for dependent_id in dependents:
                    if dependent_id in self.neighbors:
                        n_info = self.neighbors[dependent_id]
                        print(f"    -> Enviando frame RTP {frame_number} ({len(packet_bytes)} bytes) para {dependent_id}")
                        self.udp_transport.send(n_info["ip"], self.udp_port, packet_bytes)
                
                # Controla a taxa de frames (20 fps = 0.05s)
                time.sleep(0.05)
            
            print(f"[STREAM] Stream RTP terminado para {origin_id}.")

        except FileNotFoundError:
            print(f"[ERRO] Ficheiro de vídeo '{video_file}' não encontrado.")
        except Exception as e:
            print(f"[ERRO] Falha no streaming RTP: {e}")

    def handle_stream_data(self, data: bytes, sender_addr: tuple):
        """Lida com pacotes RTP binários recebidos via UDP."""
        try:
            if self.my_role == "client" and self.video_player:
                rtp_packet = RtpPacket()
                rtp_packet.decode(data)
                frame_payload = rtp_packet.getPayload()
                if frame_payload:                   
                    self.video_player.update_frame(frame_payload)
                return

            all_dependents = set()
            with self.lock:
                for origin in self.forwarding_table:
                    all_dependents.update(self.forwarding_table[origin])

            if not all_dependents:
                # Se ninguém depende de nós para NADA, descartamos o pacote.
                # Podemos adicionar um log para ver se isto acontece
                print(f"[{self.node_id}] Descartei pacote UDP, FWD table vazia.")
                return

            # Reencaminha os bytes do pacote RTP original para todos os dependentes
            for dependent_id in all_dependents:
                if dependent_id in self.neighbors:
                    n_info = self.neighbors[dependent_id]
                    self.udp_transport.send(n_info["ip"], self.udp_port, data)
                
        except Exception as e:
            pass

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
                    host = None
                    port = None

                    # Se estiver no overlay mapping, usamos info de lá
                    if sender_id in self.overlay_mapping:
                        host = self.overlay_mapping[sender_id]["host"]
                        port = self.overlay_mapping[sender_id].get("port", MY_PORT)

                    # Se não estiver, usamos o socket diretamente (IP/PORT remoto)
                    else:
                        host, port = sock.getpeername()  # pega IP/PORT do socket TCP

                    self.neighbors[sender_id] = {
                        "ip": host,
                        "port": port,
                        "socket": sock,
                        "established": True
                    }
                    print(f"[Node] Registrado novo vizinho {sender_id} via socket")

                # Mapear socket para node_id
                self.sock_to_id[sock] = sender_id

                TCPTransport.send(sock, HelloBackMessage(self.node_id).to_json().encode())


            elif isinstance(msg_obj, HelloBackMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLOBACK de {sender_id}")

                # Marca conexão como estabelecida ou cria entrada se não existir
                if sender_id not in self.neighbors:
                    host, port = sock.getpeername()
                    self.neighbors[sender_id] = {"ip": host, "port": port, "socket": sock, "established": True}
                    print(f"[Node] Registrado novo vizinho HELLOBACK {sender_id}")
                else:
                    self.neighbors[sender_id]["established"] = True

                self.sock_to_id[sock] = sender_id


            elif isinstance(msg_obj, LeaveMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi LEAVE de {sender_id}")

                with self.lock:
                    # 1. Remover o vizinho da tabela de neighbors
                    if sender_id in self.neighbors:
                        try:
                            self.neighbors[sender_id]["socket"].close()
                        except:
                            pass
                        del self.neighbors[sender_id]
                        print(f"[Node] Removi {sender_id} dos neighbors.")

                    # 2. Remover rotas na routing_table que passam pelo sender_id
                    before = len(self.routing_table)
                    self.routing_table = [
                        r for r in self.routing_table if r["sender"] != sender_id
                    ]
                    after = len(self.routing_table)
                    if before != after:
                        print(f"[Node] Limpei {before - after} rotas que dependiam de {sender_id}")

                    # 3. Remover dependenprint(f"    -> [ERRO DE LÓGICA] Recebi ACTIVATE mas não consegui identificar o remetente do socket! Tabela de FWD não atualizada.")tes da forwarding_table
                    for origin, deps in self.forwarding_table.items():
                        if sender_id in deps:
                            deps.remove(sender_id)
                            print(f"[Node] Removi {sender_id} de forwarding_table[{origin}]")

                    # 4. Remover best_route se dependia dele
                    for origin, best in list(self.recent_best_metric.items()):
                        if best["sender"] == sender_id:
                            print(f"[Node] best_route para {origin} era via {sender_id}. Invalidando.")
                            del self.recent_best_metric[origin]

                    # 5. Reconexão ao nó recomendado (avô), se algum vizinho dependia de ti
                    rec = msg_obj.recommended
                    if rec is not None:
                        rec_id = rec["id"]
                        if rec_id not in self.neighbors and rec_id != self.node_id:
                            print(f"[Node] Tentando conectar-me ao nó recomendado {rec_id}...")
                            peer_sock = self.transport.connect(rec["ip"], rec["port"])
                            if peer_sock:
                                self.neighbors[rec_id] = {
                                    "ip": rec["ip"],
                                    "port": int(rec["port"]),
                                    "socket": peer_sock,
                                    "established": False
                                }
                                self.sock_to_id[peer_sock] = rec_id
                                TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                                print(f"[Node] Conectado ao avô recomendado {rec_id}")
                            else:
                                print(f"[Node] Falha ao conectar ao recomendado {rec_id}")
                    else:
                        print(f"[Node] Nenhuma reconexão necessária ou nenhum vizinho dependia de {sender_id}.")


            elif isinstance(msg_obj, FloodMessage):
                origin = msg_obj.origin
                sender = msg_obj.sender
                host = self.overlay_mapping[sender]["host"]
                jumps = msg_obj.jumps + 1

                # Valores antigos da mensagem
                prev_latency = msg_obj.latency or 0
                prev_loss = msg_obj.loss or 0
                prev_bandwidth = msg_obj.bandwidth or math.inf

                # Métricas do link atual
                link_latency = get_latency(host) or 0
                link_loss = get_packet_loss(host) or 0
                link_bandwidth = get_bandwidth(host) or 0

                # Combina cumulativamente
                latency = prev_latency + link_latency
                loss = prev_loss + link_loss
                bandwidth = min(prev_bandwidth, link_bandwidth) if prev_bandwidth else link_bandwidth

                now = time.time()
                new_metric = self.compute_metric(latency, loss, bandwidth)
                route_entry = {
                    "origin": origin,
                    "sender": sender,
                    "metric": new_metric,
                    "state": "INACTIVE",
                    "timestamp": now
                }
                self.update_route(route_entry)
                print(f"[Node] Atualizei tabela para {origin} via {sender} (metric={new_metric})")

                best = self.get_best_route(origin)
                # Se a rota recém-chegada é a melhor, reenvia Flood
                if best["sender"] == sender:   
                    print(f"[Node] Melhor rota para {origin} via {sender} (metric={new_metric})")
                    # Reenvia para vizinhos, exceto quem enviou
                    with self.lock:
                        for neighbor_id, n_info in self.neighbors.items():
                            if neighbor_id != sender:
                                try:
                                    TCPTransport.send(
                                        n_info["socket"],
                                        FloodMessage(origin=origin, sender=self.node_id, jumps=jumps, loss=loss, bandwidth=bandwidth, latency=latency).to_json().encode()
                                    )
                                except Exception as e:
                                    print(f"[NODE] Falha ao reenviar FLOOD para {neighbor_id}: {e}")

                    # Se for client, dispara activate na melhor rota
                    if self.my_role == "client":
                        next_hop = sender  # reenvia para quem enviou o flood
                        if next_hop in self.neighbors:
                            try:
                                TCPTransport.send(
                                    self.neighbors[next_hop]["socket"],
                                    ActivateMessage(origin, self.node_id).to_json().encode()
                                )
                                print(f"[CLIENT] Enviei ACTIVATE para {next_hop}")
                                # Marca localmente como ACTIVE
                                if best:
                                    with self.lock:
                                        best["state"] = "ACTIVE"
                            except Exception as e:
                                print(f"[CLIENT] Falha ao enviar ACTIVATE: {e}")


            elif isinstance(msg_obj, ActivateMessage):
                origin = msg_obj.origin
                client_id = msg_obj.client_id

                print(f"[{self.node_id}] Recebi ACTIVATE para origin={origin}, cliente={client_id}")

                prev_hop = self.sock_to_id.get(sock)
                if prev_hop:
                    with self.lock:
                        self.forwarding_table.setdefault(origin, set()).add(prev_hop)
                        print(f"    -> Adicionei '{prev_hop}' como dependente para '{origin}'. FWD Table: {self.forwarding_table}")
                else:
                    print(f"    -> [AVISO] Não identifiquei remetente do ACTIVATE. FWD table não foi atualizada.")
                    # Se não sabemos quem pediu, não podemos continuar a cadeia.
                    return

                if self.node_id == origin:
                    print(f"    -> Sou a origem. A iniciar stream.")
                    stream_thread = threading.Thread(target=self.start_rtp_stream, args=(origin,))
                    stream_thread.start()
                    return # O fluxo de controlo para aqui para o servidor.
                
                next_hop = None
                best_route = self.get_best_route(origin)
                if best_route:
                    next_hop = best_route["sender"]
                
                if next_hop and next_hop in self.neighbors:
                    print(f"    -> A reencaminhar ACTIVATE para o meu antecessor: {next_hop}")
                    try:
                        sock_next = self.neighbors[next_hop]["socket"]
                        # Reenvia a mensagem original, preservando o client_id
                        TCPTransport.send(sock_next, ActivateMessage(origin, client_id).to_json().encode())
                    except Exception as e:
                        print(f"    -> Erro ao reencaminhar ACTIVATE para {next_hop}: {e}")
                else:
                    print(f"    -> Não reencaminhei ACTIVATE. Motivo: Não tenho uma rota válida ou o antecessor já não é meu vizinho.")

        except Exception as e:
            print(f"[Node] Erro ao processar mensagem: {e}")


class VideoClientGUI:
    def __init__(self, tk_root, on_close_callback):
        self.root = tk_root
        self.root.title("OTT Video Player")
        self.on_close_callback = on_close_callback
        self.root.protocol("WM_DELETE_WINDOW", self.handler)
        self.label = Label(self.root)
        self.label.pack()

    def update_frame(self, frame_data):
        """Recebe os bytes de um frame JPEG e exibe-os."""
        try:
            image = Image.open(io.BytesIO(frame_data))
            photo = ImageTk.PhotoImage(image)
            self.label.config(image=photo)
            self.label.image = photo
        except Exception:
            # Ignora frames corrompidos, o que é comum em UDP
            pass

    def handler(self):
        """Chamado quando a janela é fechada."""
        # Chama a função de graceful_leave do nó principal
        self.on_close_callback()
        # Fecha a janela gráfica
        self.root.destroy()

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
    threading.Thread(target=node.start, daemon=True).start()

    if node.my_role == "client":
        # Se for um cliente, e se a inicialização da GUI tiver sido bem sucedida
        if node.tk_root:
            print("[DEBUG] A entregar o controlo ao mainloop do Tkinter...")
            try:
                # Esta função bloqueia e só retorna quando a janela é fechada
                node.tk_root.mainloop()
            finally:
                # Garante que o graceful_leave é chamado quando a janela fecha
                node.graceful_leave()
        else:
            print("[AVISO] GUI não foi iniciada. O nó cliente vai terminar pois não tem nada para fazer.")
    else:
        # Se for um servidor ou nó intermediário, esperamos com um loop simples
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[MAIN] Ctrl+C detectado. A iniciar saída graciosa...")
            node.graceful_leave()
