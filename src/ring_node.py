import socket
import threading
import time
import queue
import zlib
import random
import os

TOKEN_MSG = "9000"
DATA_MSG_PREFIX = "7777:"

class RingNode:
    def __init__(self, config_file, listen_port):
        self.read_config(config_file)
        self.listen_port = listen_port
        self.msg_queue = queue.Queue(maxsize=10)
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", self.listen_port))
        self.receiver_thread = threading.Thread(target=self.receiver_loop, daemon=True)
        self.user_thread = threading.Thread(target=self.user_loop, daemon=True)
        self.awaiting_ack = False
        self.last_sent_msg = None
        self.peers = []
        self.ultimo_token = time.time()  # Marca o tempo do último token recebido
        self.token_timeout = 30  # Timeout em segundos para considerar o token perdido
        self.token_monitor_thread = threading.Thread(target=self.token_monitor_loop, daemon=True)
        self.token_gerado = False  # Para evitar múltiplos tokens

    def read_config(self, config_file):
        with open(config_file, 'r') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        # <ip_destino_do_token>:porta
        ip_port = lines[0].split(":")
        self.next_ip = ip_port[0]
        self.next_port = int(ip_port[1])
        self.nickname = lines[1]
        self.token_time = int(lines[2])
        self.is_token_creator = lines[3].lower() == 'true'

    def receiver_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
                msg = data.decode(errors='ignore')
                if msg == TOKEN_MSG:
                    self.ultimo_token = time.time()  # Atualiza o tempo do último token recebido
                    self.token_gerado = False        # Permite gerar novo token se sumir de novo
                    print("[TOKEN] Token recebido!")
                    self.handle_token()
                elif msg.startswith(DATA_MSG_PREFIX):
                    self.handle_data_message(msg)
                elif msg.startswith("ACK;"):
                    self.handle_ack(msg)
                elif msg.startswith("NACK;"):
                    self.handle_nack(msg)
            except Exception as e:
                if self.running:
                    pass  # Silencia erros de recepção

    def user_loop(self):
        while self.running:
            try:
                entrada = input()
                if not entrada.strip():
                    continue
                parts = entrada.strip().split(' ', 1)
                if len(parts) != 2:
                    continue
                destino, mensagem = parts
                if self.msg_queue.full():
                    continue
                else:
                    self.msg_queue.put((destino, mensagem))
            except Exception as e:
                pass  # Silencia erros de entrada do usuário

    def inserir_falha(self, mensagem, prob=0.2):
        """Com probabilidade 'prob', altera um caractere aleatório da mensagem."""
        if random.random() < prob and len(mensagem) > 0:
            idx = random.randint(0, len(mensagem)-1)
            # Troca o caractere por outro qualquer
            novo_char = chr((ord(mensagem[idx]) + 1) % 256)
            return mensagem[:idx] + novo_char + mensagem[idx+1:]
        return mensagem

    def handle_token(self):
        # Retransmissão após NACK
        if hasattr(self, 'retransmitir') and self.retransmitir:
            destino, mensagem = self.retransmitir
            crc = zlib.crc32(mensagem.encode())
            pacote = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{destino};{crc};{mensagem}"
            # Não insere falha na retransmissão
            self.sock.sendto(pacote.encode(), (self.next_ip, self.next_port))
            self.awaiting_ack = True
            self.last_sent_msg = pacote
            print(f"[RETRANSMISSÃO] Mensagem para {destino} retransmitida.")
            self.retransmitir = None
            return
        if not self.msg_queue.empty() and not self.awaiting_ack:
            destino, mensagem = self.msg_queue.get()
            # Broadcast para TODOS
            if destino == "TODOS":
                # Envia para todos os nós do anel, exceto si mesmo
                for peer in self.peers:
                    if peer != self.nickname:
                        msg_broadcast = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{peer};0;{mensagem}"
                        # Insere falha
                        mensagem_falha = self.inserir_falha(mensagem)
                        crc = zlib.crc32(mensagem_falha.encode())
                        pacote = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{peer};{crc};{mensagem_falha}"
                        self.sock.sendto(pacote.encode(), (self.next_ip, self.next_port))
                self.awaiting_ack = True
                self.last_sent_msg = None
                print(f"[BROADCAST] Mensagem enviada para TODOS.")
                return
            # Insere falha na mensagem com probabilidade
            mensagem_falha = self.inserir_falha(mensagem)
            crc = zlib.crc32(mensagem_falha.encode())
            pacote = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{destino};{crc};{mensagem_falha}"
            self.sock.sendto(pacote.encode(), (self.next_ip, self.next_port))
            self.awaiting_ack = True
            self.last_sent_msg = (destino, mensagem)
        elif not self.awaiting_ack:
            time.sleep(5)  # Delay de 5 segundos antes de repassar o token
            self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))

    def handle_data_message(self, msg):
        try:
            payload = msg[len(DATA_MSG_PREFIX):]
            campos = payload.split(';', 4)
            if len(campos) != 5:
                return
            status, origem, destino, crc, mensagem = campos
            crc_calculado = str(zlib.crc32(mensagem.encode()))
            if destino == self.nickname:
                if status == "naoexiste" and origem == self.nickname:
                    print(f"[FALHA] Mensagem para {destino} não foi entregue. Destinatário não existe ou está offline.")
                    return
                if crc != crc_calculado:
                    print(f"[ERRO] CRC inválido! Esperado {crc_calculado}, recebido {crc}. Enviando NACK.")
                    nack_msg = f"NACK;{origem};{self.nickname};{mensagem}"
                    self.sock.sendto(nack_msg.encode(), (self.next_ip, self.next_port))
                else:
                    print(f"[RECEBIDA] Mensagem de {origem}: {mensagem}")
                    ack_msg = f"ACK;{origem};{self.nickname};{mensagem}"
                    self.sock.sendto(ack_msg.encode(), (self.next_ip, self.next_port))
            else:
                # Se a mensagem der a volta e voltar para a origem com status 'naoexiste'
                if status == "naoexiste" and origem == self.nickname:
                    print(f"[FALHA] Mensagem para {destino} não foi entregue. Destinatário não existe ou está offline.")
                    return
                self.sock.sendto(msg.encode(), (self.next_ip, self.next_port))
        except Exception as e:
            pass

    def handle_ack(self, msg):
        try:
            _, origem, destino, mensagem = msg.split(';', 3)
            if origem == self.nickname:
                print(f"[CONFIRMADA] Sua mensagem '{mensagem}' foi recebida por {destino}!")
                self.awaiting_ack = False
                self.last_sent_msg = None
                self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
            else:
                self.sock.sendto(msg.encode(), (self.next_ip, self.next_port))
        except Exception as e:
            pass

    def handle_nack(self, msg):
        try:
            _, origem, destino, mensagem = msg.split(';', 3)
            if origem == self.nickname:
                print(f"[NACK] Sua mensagem '{mensagem}' foi recebida com erro por {destino}! Será retransmitida uma vez.")
                # Marca para retransmitir na próxima passagem do token
                self.retransmitir = (destino, mensagem)
                self.awaiting_ack = False
                self.last_sent_msg = None
            else:
                self.sock.sendto(msg.encode(), (self.next_ip, self.next_port))
        except Exception as e:
            pass

    def token_monitor_loop(self):
        while self.running:
            if (time.time() - self.ultimo_token > self.token_timeout) and not self.token_gerado:
                print('[ALERTA] Token perdido! Gerando novo token...')
                self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
                self.ultimo_token = time.time()
                self.token_gerado = True  # Evita gerar múltiplos tokens
            time.sleep(1)

    def start(self):
        # Monta a lista de peers automaticamente lendo os arquivos de configuração
        config_dir = os.path.dirname(__file__)
        arquivos = [f for f in os.listdir(config_dir) if f.startswith('config_') and f.endswith('.txt')]
        peers = []
        for arq in arquivos:
            with open(os.path.join(config_dir, arq), 'r') as f:
                linhas = [linha.strip() for linha in f.readlines() if linha.strip()]
                if len(linhas) >= 2:
                    nickname = linhas[1]
                    if nickname != self.nickname:
                        peers.append(nickname)
        self.peers = peers
        print("==== INICIANDO NÓ ====")
        print(f"Apelido: {self.nickname}")
        print(f"Escutando na porta: {self.listen_port}")
        print(f"Enviando para: {self.next_ip}:{self.next_port}")
        print(f"Peers detectados: {self.peers}")
        print("======================")
        self.receiver_thread.start()
        self.user_thread.start()
        self.token_monitor_thread.start()
        if self.is_token_creator:
            print("Aguardando 10 segundos antes de enviar o token inicial... (adicione sua mensagem agora!)")
            time.sleep(10)
            self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
            print("[TOKEN] Token inicial enviado!")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Encerrando...")
            self.running = False
        finally:
            self.sock.close() 