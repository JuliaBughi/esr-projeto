# oot/transport/tcp_transport.py
import socket
import threading

class TCPTransport:
    def __init__(self, my_ip, my_port, on_message_callback):
        self.my_ip = my_ip
        self.my_port = my_port
        self.on_message_callback = on_message_callback # Função a ser chamada quando uma mensagem chega
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def start_listening(self):
        """Inicia a thread do LADO SERVIDOR para aceitar novas conexões."""
        self.server_socket.bind((self.my_ip, self.my_port))
        self.server_socket.listen(10)
        print(f"[Transport] A escutar em {self.my_ip}:{self.my_port}")
        
        listen_thread = threading.Thread(target=self._accept_connections, daemon=True)
        listen_thread.start()

    def _accept_connections(self):
        while True:
            client_socket, addr = self.server_socket.accept()
            # Quando uma conexão é aceite, o callback é chamado para que a camada de overlay a possa gerir
            self.on_message_callback(client_socket, None, "NEW_CONNECTION")

    def connect_and_listen(self, peer_ip, peer_port):
        """Função do LADO CLIENTE: Conecta-se a um peer e retorna o socket."""
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect((peer_ip, peer_port))
            # Após conectar, o callback é chamado para a camada de overlay gerir a nova conexão
            self.on_message_callback(peer_socket, None, "NEW_CONNECTION")
            return peer_socket
        except Exception as e:
            print(f"[Transport] Falha ao conectar a {peer_ip}:{peer_port}. Erro: {e}")
            return None

    @staticmethod
    def send(peer_socket, message_bytes):
        try:
            peer_socket.sendall(message_bytes)
        except Exception as e:
            print(f"[Transport] Erro ao enviar mensagem: {e}")

    @staticmethod
    def start_receiving_thread(peer_socket, callback):
        """Inicia uma thread para receber mensagens de um socket específico."""
        recv_thread = threading.Thread(target=TCPTransport._receive_messages, args=(peer_socket, callback), daemon=True)
        recv_thread.start()

    @staticmethod
    def _receive_messages(peer_socket, callback):
        while True:
            try:
                data = peer_socket.recv(4096)
                if not data:
                    callback(peer_socket, None, "CONNECTION_CLOSED")
                    break
                callback(peer_socket, data, "MESSAGE")
            except ConnectionResetError:
                callback(peer_socket, None, "CONNECTION_CLOSED")
                break