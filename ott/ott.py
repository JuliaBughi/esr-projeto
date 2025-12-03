import sys
import os
import time
import argparse
import threading
import math
from transport.tcp_transport import TCPTransport
from transport.udp_transport import UDPTransport
from message.message_types import HelloMessage, HelloBackMessage, JoinMessage, WelcomeMessage, LeaveMessage, FloodMessage, ActivateMessage, DangerMessage, SafeOptionMessage
from message.parser import parse_message
from metrics.metrics import get_latency, get_packet_loss, get_bandwidth
from streaming.VideoStream import VideoStream
from streaming.RtpPacket import RtpPacket
from tkinter import Tk, Label
from PIL import Image, ImageTk
import io
import base64

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


    def start(self):
        self.tcp_transport.start_listening() 
        self.udp_transport.start_listening() 
        time.sleep(0.5)
        self.bootstrap()
        print(f"[Node] {self.node_id} rodando. Ctrl+C para sair.")
        time.sleep(1)
      
        if self.my_role == "server":
            threading.Thread(target=self.periodic_flood, daemon=True).start()
            threading.Thread(target=self.start_rtp_stream, args=(self.node_id,), daemon=True).start()


    def bootstrap(self):
        sock = self.tcp_transport.connect(*self.bootstrapper_addr)
        if not sock:
            print("[Bootstrapper] Falha ao conectar")
            return
        physical_id = f"{self.my_ip}:{MY_PORT}"
        TCPTransport.send(sock, JoinMessage(physical_id).to_json().encode())
        self.sock_to_id[sock] = None
        time.sleep(0.5)


    def periodic_flood(self, interval=10):
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

        print(f"[LEAVE] Iniciando graceful leave do nó {self.node_id}")

        # snapshot para evitar mutações enquanto iteramos
        neighbors_snapshot = list(self.neighbors.keys())

        for neighbor_id in neighbors_snapshot:
            print(f"\n[LEAVE] Processando neighbor {neighbor_id}")

            # 1. Determinar todos os origins para os quais o neighbor DEPENDE de mim
            child_origins = []
            for origin, next_hops in self.forwarding_table.items():
                if neighbor_id in next_hops:
                    child_origins.append(origin)

            print(f"[LEAVE] {neighbor_id} depende de mim para origins: {child_origins}")

            # 2. Encontrar o avô (meu pai no routing tree) para cada origin
            recommended_target = None

            for origin in child_origins:
                best = self.get_best_route(origin)
                if not best:
                    print(f"[LEAVE] Sem best route para origin {origin}")
                    continue

                parent_id = best["sender"]

                # Se o meu pai sou eu próprio, não existe "avô"
                if parent_id == self.node_id:
                    print(f"[LEAVE] Para origin {origin}, eu sou root. Não há avô para recomendar.")
                    continue

                info = self.overlay_mapping[parent_id]
                recommended_target = {
                    "id": parent_id,
                    "ip": info["host"],
                    "port": info["port"]
                }

                print(f"[LEAVE] Recomendando ao filho {neighbor_id} ligar ao avô {parent_id} "
                    f"({info['host']}:{info['port']})")
                break

            # 3. Construir mensagem LEAVE
            msg = LeaveMessage(
                node_id=self.node_id,
                recommended=recommended_target
            )

            # 4. Enviar a mensagem ao neighbor
            try:
                TCPTransport.send(
                    self.neighbors[neighbor_id]["socket"],
                    msg.to_json().encode()
                )
                if recommended_target:
                    print(f"[LEAVE] LEAVE enviado a {neighbor_id} com recomendação.")
                else:
                    print(f"[LEAVE] LEAVE enviado a {neighbor_id} (simples).")
            except Exception as e:
                print(f"[LEAVE] Erro ao enviar LEAVE para {neighbor_id}: {e}")

        # 5. Finalmente, fechar servidor e parar o loop principal
        print("[LEAVE] Removendo nó da overlay e fechando conexões.")
        self.running = False

        # Remover as próprias referências
        self.neighbors.clear()
        self.forwarding_table.clear()
        self.routing_table.clear()

        print("[LEAVE] Finalizado.")


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


    def start_rtp_stream(self, origin_id, video_file="movie.Mjpeg"):
        if self.my_role != "server" or self.node_id != origin_id:
            return

        print(f"[STREAM] A iniciar stream RTP para a origem {origin_id}...")
        
        try:
            video_stream = VideoStream(video_file)
            
            while True:
                # 1. Obter o próximo frame do ficheiro de vídeo
                frame_data = video_stream.nextFrame()
                if not frame_data:
                    video_stream = VideoStream(video_file)  # reinicia o video
                    continue

                frame_number = video_stream.frameNbr()
                # 2. Criar o pacote RTP binário
                rtp_packet = RtpPacket()
                rtp_packet.encode(version=2, padding=0, extension=0, cc=0, seqnum=frame_number, 
                                  marker=0, pt=26, ssrc=0, payload=frame_data)
                packet_bytes = rtp_packet.getPacket() # Estes são os bytes que vamos enviar

                with self.lock:
                    # Verifica se há clientes ativos (dependentes diretos)
                    dependents = self.forwarding_table.get(origin_id, set())

                # 3. Enviar os bytes do pacote RTP DIRETAMENTE via UDP para todos os dependentes
                for dependent_id in dependents:
                    if dependent_id in self.neighbors:
                        n_info = self.neighbors[dependent_id]
                        print(f"    -> Enviando frame RTP {frame_number} ({len(packet_bytes)} bytes) para {dependent_id}")
                        self.udp_transport.send(n_info["ip"], MY_OTHER_PORT, packet_bytes)

                # Controla a taxa de frames (20 fps = 0.05s)
                time.sleep(0.05)
            
            print(f"[STREAM] Stream RTP terminado para {origin_id}.")

        except Exception as e:
            print(f"[ERRO] Falha no streaming RTP: {e}")


    def handle_stream_data(self, data: bytes, sender_addr: tuple):
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
                    self.udp_transport.send(n_info["ip"], MY_OTHER_PORT, data)
                
        except Exception as e:
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
                        TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                self.ready_after_welcome = True

                if len(msg_obj.neighbors) == 1 and self.my_role != "server":
                    print(f"[Node] Sou folha após o WELCOME — posso entrar em perigo quando HELLO estabelecer.")
                    self.pending_danger = True


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

                if self.ready_after_welcome and self.pending_danger:
                    print(f"[Node] Handshake com {sender_id} concluído e sou folha — enviando catastrophic_warning()")
                    self.pending_danger = False
                    self.catastrophic_warning()


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
                rec = msg_obj.recommended

                print(f"[{self.node_id}] Recebi LEAVE de {sender_id} (recomendação: {rec})")

                with self.lock:
                    affected_origins = [
                        route["origin"]
                        for route in self.routing_table
                        if route["sender"] == sender_id
                    ]
                    print(f"[LEAVE] {sender_id} era responsável pelos origins: {affected_origins}")

                with self.lock:
                    # Fechar socket do sender
                    if sender_id in self.neighbors:
                        try:
                            self.neighbors[sender_id]["socket"].close()
                        except:
                            pass
                        del self.neighbors[sender_id]
                        print(f"[LEAVE] Removido {sender_id} dos neighbors")

                    # Limpar rotas do sender
                    before = len(self.routing_table)
                    self.routing_table = [r for r in self.routing_table if r["sender"] != sender_id]
                    after = len(self.routing_table)
                    if before != after:
                        print(f"[LEAVE] Removidas {before - after} rotas que dependiam de {sender_id}")

                    # Remover sender da forwarding_table
                    for origin, deps in list(self.forwarding_table.items()):
                        if sender_id in deps:
                            deps.remove(sender_id)
                            print(f"[LEAVE] Removido {sender_id} de forwarding_table[{origin}]")

                # Se houver recomendação
                if rec:
                    rec_id = rec.get("id")
                    rec_ip = rec.get("ip")
                    rec_port = rec.get("port")

                    for origin in affected_origins:
                        # Verifica se há rotas alternativas para este origin
                        alt = self.get_best_route(origin)
                        if alt:
                            with self.lock:
                                self.forwarding_table[origin] = [alt["sender"]]
                            print(f"[LEAVE] Usando rota alternativa para origin {origin} via {alt['sender']}")
                            continue

                        # Sem rota alternativa → conectar ao recomendado diretamente (IP/Port do LEAVE)
                        peer_sock = self.tcp_transport.connect(rec_ip, rec_port)
                        if peer_sock:
                            with self.lock:
                                self.neighbors[rec_id] = {
                                    "ip": rec_ip,
                                    "port": rec_port,
                                    "socket": peer_sock,
                                    "established": False
                                }
                                self.sock_to_id[peer_sock] = rec_id
                                self.forwarding_table.setdefault(origin, set()).add(rec_id)

                            TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                            print(f"[LEAVE] Conectado ao recomendado {rec_id} para origin {origin} ({rec_ip}:{rec_port})")
                        else:
                            print(f"[LEAVE] Falha ao conectar ao recomendado {rec_id} para origin {origin} ({rec_ip}:{rec_port})")


            elif isinstance(msg_obj, FloodMessage):
                origin = msg_obj.origin
                sender = msg_obj.sender
                host = self.neighbors[sender]["ip"]
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

                # Enviar mensagem SafeOption de volta ao nodo em perigo
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
