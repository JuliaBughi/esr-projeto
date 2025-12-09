import sys
import argparse
import socket
import threading
import io
import time
from tkinter import Tk, Label
from PIL import Image, ImageTk

# Imports do pacote ott
from message.message_types import ClientStreamRequest
from transport.tcp_transport import TCPTransport
from transport.udp_transport import UDPTransport
from streaming.RtpPacket import RtpPacket

# Porta onde o cliente vai escutar UDP
CLIENT_UDP_PORT = 5001

class VideoClientGUI:
    def __init__(self, tk_root):
        self.root = tk_root
        self.root.title("OTT Client - Viewer")
        self.label = Label(self.root)
        self.label.pack()

    def update_frame(self, frame_data):
        try:
            image = Image.open(io.BytesIO(frame_data))
            photo = ImageTk.PhotoImage(image)
            self.label.config(image=photo)
            self.label.image = photo
        except Exception:
            pass

class ClientApp:
    def __init__(self, overlay_node_ip, overlay_node_port, stream_id=None):
        self.overlay_ip = overlay_node_ip
        self.overlay_port = overlay_node_port
        self.stream_id = stream_id  # Pode ser None
        
        self.tk_root = Tk()
        self.gui = VideoClientGUI(self.tk_root)
        
        # Socket TCP para enviar o pedido
        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # UDP Transport para receber dados
        self.udp_transport = UDPTransport("0.0.0.0", CLIENT_UDP_PORT, self.handle_udp_data)
        
        # Controlo de monitorização
        self.last_packet_time = time.time()
        self.running = True

    def start(self):
        # 1. Iniciar escuta UDP
        self.udp_transport.start_listening()
        
        # 2. Conectar TCP ao nodo Overlay
        print(f"Conectando a {self.overlay_ip}:{self.overlay_port}...")
        try:
            self.tcp_sock.connect((self.overlay_ip, self.overlay_port))
            print("Conectado TCP.")
            
            # 3. Enviar o primeiro pedido
            self.send_stream_request()
            
            # 4. Iniciar Thread de Monitorização (Retry)
            threading.Thread(target=self.monitor_connection, daemon=True).start()
            
        except Exception as e:
            print(f"Erro ao conectar inicial: {e}")
            return

        # 5. Iniciar GUI Loop (bloqueante)
        try:
            self.tk_root.mainloop()
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            self.tcp_sock.close()

    def send_stream_request(self):
        """Envia o pedido CLIENT_REQ via TCP"""
        try:
            req = ClientStreamRequest(self.stream_id)
            # Usa o método estático do TCPTransport para garantir o formato correto (length-prefixed)
            TCPTransport.send(self.tcp_sock, req.to_json().encode())
            
            if self.stream_id:
                print(f"[TCP] Pedido enviado para stream '{self.stream_id}'")
            else:
                print(f"[TCP] Pedido enviado para QUALQUER stream disponível")
        except Exception as e:
            print(f"[TCP] Erro ao enviar pedido: {e}")
            # Tentar reconectar se o socket tiver morrido
            try:
                self.tcp_sock.close()
                self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.tcp_sock.connect((self.overlay_ip, self.overlay_port))
                TCPTransport.send(self.tcp_sock, req.to_json().encode())
                print("[TCP] Reconectado e pedido reenviado.")
            except:
                pass

    def monitor_connection(self):
        """Verifica se estamos a receber dados. Se não, reenvia o pedido."""
        print("[MONITOR] Iniciado. Vou reenviar o pedido se o vídeo não chegar.")
        
        # Espera inicial para dar tempo à rede convergir
        time.sleep(3)
        
        while self.running:
            now = time.time()
            # Se passaram mais de 3 segundos desde o último pacote
            if now - self.last_packet_time > 3:
                print(f"[MONITOR] Sem vídeo há {int(now - self.last_packet_time)}s. Reenviando pedido...")
                self.send_stream_request()
                
                # Espera um pouco antes de tentar de novo para não fazer spam
                time.sleep(3)
            else:
                time.sleep(1)

    def handle_udp_data(self, data, sender_addr):
        """Callback quando chega um pacote UDP"""
        self.last_packet_time = time.time()
        try:
            rtp_packet = RtpPacket()
            rtp_packet.decode(data)
            payload = rtp_packet.getPayload()
            if payload:
                self.gui.update_frame(payload)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OTT Client")
    parser.add_argument("--node", required=True, help="IP do nodo overlay a conectar")
    parser.add_argument("--port", type=int, default=5000, help="Porta TCP do nodo overlay")
    parser.add_argument("--stream", help="ID da stream (Opcional. Se vazio, pede a primeira disponível)", default=None)
    
    args = parser.parse_args()
    
    app = ClientApp(args.node, args.port, args.stream)
    app.start()