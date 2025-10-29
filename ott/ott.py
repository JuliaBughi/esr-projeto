import sys, os
import time
from transport.tcp_transport import TCPTransport
from message.message_types import HelloMessage, HelloBackMessage, JoinMessage, WelcomeMessage
from message.parser import parse_message

MY_PORT = 5000

class Node:
    def __init__(self, my_ip, bootstrapper_addr):
        self.my_ip = my_ip
        self.node_id = f"{my_ip}:{MY_PORT}"
        self.bootstrapper_addr = bootstrapper_addr
        self.transport = TCPTransport(my_ip, MY_PORT, self.handle_message)
        self.neighbors = {}  # node_id -> {"ip": ip, "port": port, "socket": sock}

    def start(self):
        self.transport.start_listening()
        time.sleep(0.5)
        self.bootstrap()
        print(f"[Node] {self.node_id} rodando. Ctrl+C para sair.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[Node] Desligando...")

    def bootstrap(self):
        """Conecta ao bootstrapper e recebe vizinhos"""
        sock = self.transport.connect(*self.bootstrapper_addr)
        if not sock:
            print("[Bootstrapper] Falha ao conectar")
            return
        # Envia JOIN
        TCPTransport.send(sock, JoinMessage(self.node_id).to_json().encode())
        # Aguarda WelcomeMessage
        time.sleep(0.5)  # espera simples
        # Se houver resposta, a callback handle_message tratará
        # (recebe WelcomeMessage e inicia handshake)


    def handle_message(self, sock, data):
        """Callback para mensagens recebidas"""
        if not data:
            return

        try:
            msg_obj = parse_message(data.decode())

            if isinstance(msg_obj, WelcomeMessage):
                print(f"[Node] Recebi vizinhos: {msg_obj.neighbors}")
                for n in msg_obj.neighbors:
                    peer_sock = self.transport.connect(n["host"], n["port"])
                    if peer_sock:
                        # Envia HELLO para iniciar handshake 1:1
                        TCPTransport.send(peer_sock, HelloMessage(self.node_id).to_json().encode())
                        # Armazena vizinho e socket
                        self.neighbors[n["id"]] = {"ip": n["host"], "port": n["port"], "socket": peer_sock}

            elif isinstance(msg_obj, HelloMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLO de {sender_id}")

                # Se ainda não temos a conexão com esse nó, registramos
                if sender_id not in self.neighbors:
                    self.neighbors[sender_id] = {"socket": sock}

                # Responde com HELLO (ou HELLOBACK, se quiser diferenciar)
                TCPTransport.send(sock, HelloMessage(self.node_id).to_json().encode())

            elif isinstance(msg_obj, HelloBackMessage):
                sender_id = msg_obj.node_id
                print(f"[Node] Recebi HELLOBACK de {sender_id}")
                # Marca conexão como estabelecida ou atualiza estado
                if sender_id in self.neighbors:
                    self.neighbors[sender_id]["established"] = True

            else:
                print(f"[Node] Recebi mensagem desconhecida: {msg_obj}")

        except Exception as e:
            print(f"[Node] Erro ao processar mensagem: {e}")

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python ott.py <IP do node> <IP do bootstrapper>")
        sys.exit(1)

    print(len(sys.argv))
    bootstrap_ip = sys.argv[2]
    node = Node(sys.argv[1], (bootstrap_ip, 5000)) # trocar para ip do node
    node.start()