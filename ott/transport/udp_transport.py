import socket
import threading
import time
import json

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
        print(f"[UDP] Listening on {self.my_ip}:{self.my_port}")
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(65535)
                
                # 1. Extract Header Length (First byte)
                if len(data) < 1: continue
                header_len = data[0]

                # 2. Extract Header JSON
                if len(data) < 1 + header_len: continue
                header_bytes = data[1 : 1 + header_len]
                
                try:
                    header_str = header_bytes.decode("utf-8")
                    piggyback_header = json.loads(header_str)
                except:
                    print("[UDP] Malformed header, dropping.")
                    continue

                # 3. Extract Real Payload (RTP Video)
                payload = data[1 + header_len :]

                # 4. Pass to Node logic
                # callback(payload_bytes, sender_tuple, header_dict)
                self.on_message_callback(payload, addr, piggyback_header)

            except Exception as e:
                print(f"[UDP LISTEN ERROR] {e}")

    def send(self, target_ip, target_port, message_bytes, piggyback_header):
        try:
            header = {
                "node": self.my_ip + ":" + str(self.my_port),
                "hb": time.time()    # heartbeat timestamp
            }
            
            header_json = json.dumps(header).encode("utf-8")
            header_len = len(header_json)

            if header_len > 255:
                raise ValueError("Piggyback header too large!")

            # Final packet:
            # [1 byte length][header JSON][RTP bytes]
            packet = bytes([header_len]) + header_json + message_bytes

            # ---- SEND ----
            self.udp_socket.sendto(packet, (target_ip, target_port))

        except Exception as e:
            print(f"[UDP] Erro ao enviar dados para {target_ip}:{target_port}: {e}")
