# ott/node.py
import argparse
import time
# As duas linhas seguintes foram corrigidas de 'oot' para 'ott'
from ott.transport.tcp_transport import TCPTransport
from ott.overlay.overlay import OverlayManager

class Node:
    def __init__(self, my_ip, my_port, bootstrapper_addr=None):
        self.overlay = OverlayManager(my_ip, my_port)
        self.transport = TCPTransport(my_ip, my_port, self.overlay.handle_message)
        
        self.overlay.transport = self.transport
        
        self.bootstrapper_addr = bootstrapper_addr

    def start(self):
        self.transport.start_listening()
        
        if self.bootstrapper_addr:
            time.sleep(1)
            self.overlay.bootstrap(self.bootstrapper_addr)
        
        print("[Node] Nó está a correr. Pressione Ctrl+C para sair.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[Node] A desligar...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nó da rede OTT")
    parser.add_argument('--ip', required=True, help="IP do nó")
    parser.add_argument('--port', type=int, required=True, help="Porta TCP para escutar")
    parser.add_argument('--bootstrapper', help="Endereço do bootstrapper (IP:PORTA)")
    
    args = parser.parse_args()
    
    bs_addr = None
    if args.bootstrapper:
        ip, port_str = args.bootstrapper.split(':')
        bs_addr = (ip, int(port_str))

    node = Node(args.ip, args.port, bs_addr)
    node.start()