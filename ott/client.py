import sys
import argparse
import socket
import threading
import io
import time
from tkinter import Tk, Label
from PIL import Image, ImageTk

from message.message_types import ClientStreamRequest
from transport.tcp_transport import TCPTransport
from transport.udp_transport import UDPTransport
from streaming.RtpPacket import RtpPacket

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
    def __init__(self, overlay_node_ip, overlay_node_port, stream_id):
        self.overlay_ip = overlay_node_ip
        self.overlay_port = overlay_node_port
        self.stream_id = stream_id
        
        self.tk_root = Tk()
        self.gui = VideoClientGUI(self.tk_root)
        
        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.udp_transport = UDPTransport("0.0.0.0", CLIENT_UDP_PORT, self.handle_udp_data)
        
        self.last_packet_time = 0
        self.running = True

    def start(self):
        self.udp_transport.start_listening()
        
        print(f"Conectando a {self.overlay_ip}:{self.overlay_port}...")
        try:
            self.tcp_sock.connect((self.overlay_ip, self.overlay_port))
            print("Conectado TCP.")
            
            # Envia o primeiro pedido
            self.send_stream_request()
            
            threading.Thread(target=self.monitor_connection, daemon=True).start()
            
        except Exception as e:
            print(f"Erro ao conectar inicial: {e}")
            return

        self.tk_root.mainloop()
        self.running = False # Quando a janela fecha, paramos o monitor

    def send_stream_request(self):
        """Envia o pedido CLIENT_REQ via TCP"""
        try:
            req = ClientStreamRequest(self.stream_id)
            TCPTransport.send(self.tcp_sock, req.to_json().encode())
            print(f"[TCP] Pedido enviado para stream '{self.stream_id}'")
        except Exception as e:
            print(f"[TCP] Erro ao enviar pedido (tentando reconectar...): {e}")
            # Opcional: Lógica de reconexão TCP se o socket tiver morrido
            try:
                self.tcp_sock.close()
                self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.tcp_sock.connect((self.overlay_ip, self.overlay_port))
                TCPTransport.send(self.tcp_sock, req.to_json().encode())
            except:
                pass

    def monitor_connection(self):
        """Verifica se estamos a receber dados. Se não, reenvia o pedido."""
        print("[MONITOR] Iniciado. Vou reenviar o pedido se o vídeo não chegar.")
        
        # Espera inicial para dar tempo à rede
        time.sleep(2)
        
        while self.running:
            now = time.time()
            if now - self.last_packet_time > 10:
                print(f"[MONITOR] Sem vídeo. Reenviando pedido...")
                self.send_stream_request()
                
                # Espera um pouco antes de tentar de novo para não fazer spam
                time.sleep(7)
            else:
                # Verifica a cada 1 segundo
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
    parser.add_argument("--stream", required=True, help="ID da stream (ex: S1)")
    
    args = parser.parse_args()
    
    app = ClientApp(args.node, args.port, args.stream)
    app.start()