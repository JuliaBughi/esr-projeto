import socket
import threading
import struct

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
        """Recebe mensagens length-prefixed e chama callback"""
        buffer = b''  # b'' -> byte string para mandar as mensagens pela rede 
        while True:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                buffer += data

                while len(buffer) >= 4: # lemos o prefixo da msg que nos diz o tamanho da mensagem
                    msg_length = struct.unpack('!I', buffer[:4])[0]
                    if len(buffer) < 4 + msg_length: # esperar que a mensagem acabe
                        break
                    msg_bytes = buffer[4:4+msg_length]
                    buffer = buffer[4+msg_length:]  # remove a mensagem processada do buffer
                    self.on_message_callback(sock, msg_bytes)

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
        try:                                                      # como message_bytes vem no tipo bytes entao len da nos o tamanho em bytes 
            length_prefix = struct.pack('!I', len(message_bytes)) # ve o tamanho da mensagem e transforma em 4 bytes  
            sock.sendall(length_prefix + message_bytes)           # (se o tamanho da mensagem fosse 15 bytes ficava 00 00 00 0F)
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