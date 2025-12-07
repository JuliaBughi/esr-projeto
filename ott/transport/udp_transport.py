import socket
import threading

class UDPTransport:
    def __init__(self, my_ip, my_port, on_message_callback):
        self.my_ip = my_ip
        self.my_port = my_port
        self.on_message_callback = on_message_callback
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def start_listening(self):
        self.udp_socket.bind((self.my_ip, self.my_port))
        listen_thread = threading.Thread(target=self._listen_loop, daemon=True)
        listen_thread.start()
        print(f"[UDP] A escutar por dados de stream na porta {self.my_port}")

    def _listen_loop(self):
        while True:
            try:
                # Aumentar o tamanho do buffer para acomodar frames de vídeo grandes.
                # 65535 é o tamanho máximo teórico de um datagrama UDP.
                data, sender_addr = self.udp_socket.recvfrom(65535) 

                if data:
                    self.on_message_callback(data, sender_addr)
            except Exception as e:
                print(f"[UDP] Erro a receber dados: {e}")

    def send(self, target_ip, target_port, message_bytes):
        try:
            self.udp_socket.sendto(message_bytes, (target_ip, target_port))
        except Exception as e:
            print(f"[UDP] Erro ao enviar dados para {target_ip}:{target_port}: {e}")