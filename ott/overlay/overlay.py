import socket
import threading
from ott.message import message_types, parser

class OverlayManager:
    def __init__(self, my_ip, my_port):
        self.my_ip = my_ip
        self.my_port = my_port
        self.neighbors = {}
        self.lock = threading.Lock()
    
    def bootstrap(self, bootstrapper_addr):
        print(f"[Overlay] A contactar o bootstrapper em {bootstrapper_addr}...")
        try:
            boot_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            boot_socket.connect(bootstrapper_addr)
            
            join_msg = parser.create_message(message_types.JOIN_REQUEST, {"ip": self.my_ip, "port": self.my_port})
            boot_socket.sendall(join_msg)
            
            response_data = boot_socket.recv(4096)
            response = parser.parse_message(response_data)
            
            if response and response["type"] == message_types.NEIGHBOR_LIST:
                neighbor_list = response["payload"]
                print(f"[Overlay] Recebida lista de {len(neighbor_list)} vizinhos do bootstrapper.")
                for ip, port in neighbor_list:
                    self.transport.connect_and_listen(ip, port)
            
            boot_socket.close()
        except Exception as e:
            print(f"[Overlay] Erro no bootstrap: {e}")

    def handle_message(self, peer_socket, data, event_type):
        if event_type == "NEW_CONNECTION":
            peer_ip, peer_port = peer_socket.getpeername()
            print(f"[Overlay] Nova conexão com {peer_ip}:{peer_port}. A enviar HELLO.")
            
            with self.lock:
                self.neighbors[peer_socket] = (peer_ip, peer_port)
            
            self.transport.start_receiving_thread(peer_socket, self.handle_message)
            
            hello_msg = parser.create_message(message_types.HELLO, {"info": f"Olá de {self.my_ip}:{self.my_port}"})
            self.transport.send(peer_socket, hello_msg)
            return

        if event_type == "CONNECTION_CLOSED":
            with self.lock:
                if peer_socket in self.neighbors:
                    peer_addr = self.neighbors.pop(peer_socket)
                    print(f"[Overlay] Conexão com {peer_addr} fechada.")
            return
        
        if event_type == "MESSAGE":
            message = parser.parse_message(data)
            if not message:
                return

            if message["type"] == message_types.HELLO:
                peer_addr = self.neighbors.get(peer_socket, "Vizinho Desconhecido")
                print(f"[Overlay] Recebido HELLO de {peer_addr}: '{message['payload']['info']}'")