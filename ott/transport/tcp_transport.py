import socket
import threading

class TCPTransport:
    def __init__(self, my_ip, my_port, on_message_callback):
        self.my_ip = my_ip
        self.my_port = my_port
        self.on_message_callback = on_message_callback
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False
        self.listener_thread = None

    def start_listening(self):
        """Inicia servidor TCP em thread separada"""
        self.server_socket.bind((self.my_ip, self.my_port))
        self.server_socket.listen(10)
        self.running = True
        self.listener_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.listener_thread.start()
        print(f"[TCP] Escutando em {self.my_ip}:{self.my_port}")

    def _accept_loop(self):
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()
            except OSError:
                # Socket fechado — parar o loop
                break
            except Exception as e:
                print(f"[TCP] Erro no accept_loop: {e}")
                break

    def _handle_client(self, sock):
        """Recebe mensagens e chama callback"""
        while True:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                self.on_message_callback(sock, data)
            except ConnectionResetError:
                break
            except OSError:
                break
        sock.close()

    def connect(self, peer_ip, peer_port):
        """Conecta a outro nó e retorna socket"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_ip, peer_port))
            threading.Thread(target=self._handle_client, args=(sock,), daemon=True).start()
            return sock
        except Exception as e:
            print(f"[TCP] Falha ao conectar {peer_ip}:{peer_port}: {e}")
            return None

    @staticmethod
    def send(sock, message_bytes):
        try:
            sock.sendall(message_bytes)
        except Exception as e:
            print(f"[TCP] Erro ao enviar: {e}")

    def stop_listening(self):
        """Fecha o socket do servidor e encerra o loop de aceitação"""
        if not self.running:
            return
        print(f"[TCP] Encerrando servidor em {self.my_ip}:{self.my_port}...")
        self.running = False
        try:
            self.server_socket.close()
        except Exception as e:
            print(f"[TCP] Erro ao fechar socket: {e}")

