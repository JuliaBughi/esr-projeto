# ott/streaming/VideoStream.py

class VideoStream:
    """
    Lê um ficheiro de vídeo no formato MJPEG.
    O formato esperado é uma sequência de frames JPEG, onde cada frame é
    precedido por um cabeçalho de 5 bytes em ASCII que indica o seu tamanho.
    """
    def __init__(self, filename: str):
        self.filename = filename
        try:
            self.file = open(filename, 'rb')
        except FileNotFoundError:
            raise IOError(f"Ficheiro de vídeo '{filename}' não encontrado.")
        self.frameNum = 0

    def nextFrame(self) -> bytes or None:
        """
        Lê e retorna o próximo frame do ficheiro.
        Retorna os bytes do frame, ou None se o ficheiro terminou.
        """
        # Lê os 5 bytes do cabeçalho que contêm o tamanho do frame seguinte.
        data = self.file.read(5)
        
        if data:
            try:
                framelength = int(data)
            except ValueError:
                # Se os 5 bytes não forem um número, pode ser o fim do ficheiro
                # ou um ficheiro corrompido.
                return None
            
            # Lê a quantidade de bytes correspondente ao tamanho do frame.
            frame_data = self.file.read(framelength)
            self.frameNum += 1
            return frame_data
        else:
            # Não há mais dados para ler, fim do ficheiro.
            return None

    def frameNbr(self) -> int:
        """Retorna o número do frame atual."""
        return self.frameNum