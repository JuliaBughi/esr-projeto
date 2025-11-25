# ott/streaming/RtpPacket.py

from time import time

HEADER_SIZE = 12

class RtpPacket:
    """
    Representa um pacote RTP.
    Esta classe sabe como construir (encode) e ler (decode) pacotes RTP,
    incluindo o seu cabeçalho e payload.
    """
    header = bytearray(HEADER_SIZE)

    def __init__(self):
        pass

    def encode(self, version, padding, extension, cc, seqnum, marker, pt, ssrc, payload):
        """
        Codifica o pacote RTP com os campos do cabeçalho e o payload.
        
        Argumentos:
        version, padding, extension, cc, seqnum, marker, pt, ssrc: Campos do cabeçalho RTP.
        payload: Os dados a serem transportados (neste caso, os bytes de um frame de vídeo).
        """
        timestamp = int(time())
        header = bytearray(HEADER_SIZE)

        # Preenchimento do cabeçalho byte a byte, conforme a especificação RTP (RFC 3550).
        # Este código usa operações bit a bit (<<, |, &) para colocar cada campo
        # na sua posição correta dentro dos bytes do cabeçalho.
        
        # Byte 0: Version (2 bits), Padding (1 bit), Extension (1 bit), CC (4 bits)
        header[0] = (version << 6) | (padding << 5) | (extension << 4) | cc
        
        # Byte 1: Marker (1 bit), Payload Type (7 bits)
        header[1] = (marker << 7) | pt
        
        # Bytes 2-3: Sequence Number (16 bits)
        header[2] = (seqnum >> 8) & 0xFF  # High byte
        header[3] = seqnum & 0xFF         # Low byte
        
        # Bytes 4-7: Timestamp (32 bits)
        header[4] = (timestamp >> 24) & 0xFF
        header[5] = (timestamp >> 16) & 0xFF
        header[6] = (timestamp >> 8) & 0xFF
        header[7] = timestamp & 0xFF
        
        # Bytes 8-11: SSRC (32 bits)
        header[8] = (ssrc >> 24) & 0xFF
        header[9] = (ssrc >> 16) & 0xFF
        header[10] = (ssrc >> 8) & 0xFF
        header[11] = ssrc & 0xFF

        # Guarda o cabeçalho e o payload construídos.
        self.header = header
        self.payload = payload

    def decode(self, byteStream):
        """Descodifica um pacote RTP a partir de uma stream de bytes."""
        # Os primeiros HEADER_SIZE bytes são o cabeçalho.
        self.header = bytearray(byteStream[:HEADER_SIZE])
        # O resto é o payload.
        self.payload = byteStream[HEADER_SIZE:]

    def version(self) -> int:
        """Retorna a versão do RTP (deve ser 2)."""
        return int(self.header[0] >> 6)

    def seqNum(self) -> int:
        """Retorna o número de sequência (número do frame)."""
        seqNum = (self.header[2] << 8) | self.header[3]
        return int(seqNum)

    def timestamp(self) -> int:
        """Retorna o timestamp do pacote."""
        timestamp = (self.header[4] << 24) | (self.header[5] << 16) | (self.header[6] << 8) | self.header[7]
        return int(timestamp)

    def payloadType(self) -> int:
        """Retorna o tipo de payload (ex: 26 para MJPEG)."""
        pt = self.header[1] & 127
        return int(pt)

    def getPayload(self) -> bytes:
        """Retorna o payload (os dados brutos do frame)."""
        return self.payload

    def getPacket(self) -> bytes:
        """Retorna o pacote RTP completo (cabeçalho + payload) como bytes."""
        return self.header + self.payload