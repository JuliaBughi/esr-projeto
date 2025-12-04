import cv2
import argparse
import os

def convert_mp4_to_mjpeg(input_path, output_path):
    """
    Converte um ficheiro de vídeo (ex: .mp4) para o formato .Mjpeg customizado
    usado pelo projeto, com um cabeçalho de 5 bytes ASCII para o tamanho de cada frame.
    """
    # Verifica se o ficheiro de entrada existe
    if not os.path.exists(input_path):
        print(f"Erro: Ficheiro de entrada não encontrado em '{input_path}'")
        return

    # Abre o vídeo de entrada
    video_capture = cv2.VideoCapture(input_path)
    if not video_capture.isOpened():
        print(f"Erro: Não foi possível abrir o ficheiro de vídeo '{input_path}'")
        return

    frame_count = 0
    
    print(f"A converter '{input_path}' para '{output_path}'...")

    # Abre o ficheiro de saída em modo de escrita binária ('wb')
    with open(output_path, 'wb') as output_file:
        while True:
            # Lê um frame do vídeo
            success, frame = video_capture.read()

            # Se não conseguiu ler mais frames, o vídeo terminou
            if not success:
                break
            
            # Codifica o frame lido para o formato JPEG em memória
            # O resultado é um buffer de bytes
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90] # Qualidade do JPEG (0-100)
            result, encoded_jpeg = cv2.imencode('.jpg', frame, encode_param)

            if not result:
                print(f"Aviso: Falha ao codificar o frame {frame_count}. A saltar.")
                continue

            # Obtém o tamanho do frame JPEG em bytes
            frame_size = len(encoded_jpeg)
            
            # Cria o cabeçalho de 5 bytes em ASCII
            # Ex: se o tamanho for 12345, o cabeçalho será "12345"
            # Ex: se o tamanho for 987, o cabeçalho será "00987"
            header = str(frame_size).zfill(5)

            # Escreve o cabeçalho (convertido para bytes) no ficheiro de saída
            output_file.write(header.encode('ascii'))
            
            # Escreve os dados do frame JPEG no ficheiro de saída
            output_file.write(encoded_jpeg.tobytes())
            
            frame_count += 1
            if frame_count % 30 == 0: # Imprime um status a cada 30 frames
                print(f"Processados {frame_count} frames...")

    # Liberta o recurso do vídeo
    video_capture.release()
    print(f"\nConversão concluída! {frame_count} frames foram escritos em '{output_path}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Conversor de MP4 para o formato MJPEG do projeto.")
    parser.add_argument("--input", required=True, help="Caminho para o ficheiro de vídeo de entrada (ex: video.mp4).")
    parser.add_argument("--output", default="movie.Mjpeg", help="Caminho para o ficheiro de saída .Mjpeg (padrão: movie.Mjpeg).")
    
    args = parser.parse_args()

    convert_mp4_to_mjpeg(args.input, args.output)