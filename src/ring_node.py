import socket
import threading
import time
import queue
import zlib
import random
import os

TOKEN_MSG = "9000"
DATA_MSG_PREFIX = "7777:"
# ---------------------------------------------------
# Tempo (em segundos) que cada nó "segura" um pacote
DEFAULT_DATA_TIME = 2
# ---------------------------------------------------

class RingNode:
    def __init__(self, config_file, listen_port):
        # lê config de 4 linhas
        self.read_config(config_file)
        # tempo de dados fixo
        self.data_time      = DEFAULT_DATA_TIME
        self.listen_port    = listen_port
        self.msg_queue      = queue.Queue(maxsize=10)
        self.running        = True
        self.awaiting_ack   = False
        self.last_sent_msg  = None
        self.retransmit     = None
        self.retransmit_done = False
        self.current_msg    = None  # Armazena a mensagem em processamento

        # socket UDP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", self.listen_port))

        # threads
        self.receiver_thread      = threading.Thread(target=self.receiver_loop,      daemon=True)
        self.user_thread          = threading.Thread(target=self.user_loop,          daemon=True)
        self.token_monitor_thread = threading.Thread(target=self.token_monitor_loop, daemon=True)

        # controle de token
        self.ultimo_token  = time.time()
        self.token_timeout = 30
        self.token_gerado  = False

        # peers (apenas nomes, para broadcast)
        self.peers = []
        self.discover_peers()

    def read_config(self, config_file):
        """Lê 4 linhas:
           1. ip:porta do próximo
           2. apelido
           3. tempo_token (s)
           4. gera_token_inicial (true|false)
        """
        with open(config_file, 'r') as f:
            lines = [l.strip() for l in f if l.strip()]
        ip, port = lines[0].split(":")
        self.next_ip          = ip
        self.next_port        = int(port)
        self.nickname         = lines[1]
        self.token_time       = int(lines[2])
        self.is_token_creator = lines[3].lower() == 'true'

    def discover_peers(self):
        cfg_dir = os.path.dirname(__file__)
        for fn in os.listdir(cfg_dir):
            if fn.startswith("config_") and fn.endswith(".txt"):
                with open(os.path.join(cfg_dir, fn)) as f:
                    lns = [l.strip() for l in f if l.strip()]
                if len(lns) >= 2 and lns[1] != self.nickname:
                    self.peers.append(lns[1])

    def receiver_loop(self):
        while self.running:
            try:
                data, _ = self.sock.recvfrom(4096)
                msg = data.decode(errors='ignore')
                if msg == TOKEN_MSG:
                    tempo_desde_ultimo = time.time() - self.ultimo_token
                    if tempo_desde_ultimo < self.token_time * 0.8:  # ou outro fator de segurança
                        print(f"[ALERTA] Token duplicado detectado! Ignorando token recebido cedo demais.")
                        continue  # descarta o token extra
                    self.ultimo_token = time.time()
                    self.token_gerado  = False
                    print(f"[TOKEN] recebido em {self.nickname}")
                    self.handle_token()
                elif msg.startswith(DATA_MSG_PREFIX):
                    self.handle_data(msg)
            except:
                pass

    def user_loop(self):
        while self.running:
            try:
                linha = input().strip()
                if not linha: continue
                dest, texto = linha.split(" ", 1)
                if self.msg_queue.full():
                    print("[FILA] cheia, descartei mensagem.")
                else:
                    self.msg_queue.put((dest, texto))
            except:
                pass

    def inserir_falha(self, mensagem, prob=0.2):
        if random.random() < prob and mensagem:
            i = random.randint(0, len(mensagem)-1)
            c = chr((ord(mensagem[i]) + 1) % 256)
            return mensagem[:i] + c + mensagem[i+1:]
        return mensagem

    def handle_token(self):
        # delay após receber token
        print(f"[TOKEN] aguardando {self.token_time}s...")
        time.sleep(self.token_time)

        # retransmissão de NACK pendente
        if self.retransmit and not self.retransmit_done:
            dest, msg = self.retransmit
            crc = zlib.crc32(msg.encode())
            pkt = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{dest};{crc};{msg}"
            self.sock.sendto(pkt.encode(), (self.next_ip, self.next_port))
            print(f"[RETRANSMISSÃO] enviado: {pkt}")
            self.awaiting_ack = True
            self.retransmit_done = True  # Marca que já retransmitiu uma vez
            self.current_msg = (dest, msg)  # Mantém a mensagem até confirmação
            return
        elif self.retransmit and self.retransmit_done:
            print(f"[DESCARTADA] Mensagem '{msg}' para {dest} após segunda falha (NACK duplo).")
            self.retransmit = None
            self.retransmit_done = False
            self.awaiting_ack = False
            self.current_msg = None
            if not self.msg_queue.empty():
                self.msg_queue.get()  # Remove da fila
            # Libera o token normalmente
            time.sleep(5)
            self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
            print(f"[TOKEN] repassado por {self.nickname}")
            return

        # nova mensagem da fila?
        if self.current_msg is None and not self.msg_queue.empty() and not self.awaiting_ack:
            self.current_msg = self.msg_queue.queue[0]  # Pega a próxima mensagem sem remover

        if self.current_msg and not self.awaiting_ack:
            dest, msg = self.current_msg
            print(f"[DADOS] preparando para {dest}, aguardando {self.data_time}s...")
            time.sleep(self.data_time)

            if dest == "TODOS":
                for peer in self.peers:
                    crc = zlib.crc32(msg.encode())
                    msg_falha = self.inserir_falha(msg)
                    pkt = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{peer};{crc};{msg_falha}"
                    self.sock.sendto(pkt.encode(), (self.next_ip, self.next_port))
                    print(f"[BROADCAST] enviado: {pkt}")
                self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
                self.msg_queue.get()  # Remove a mensagem da fila após broadcast
                self.current_msg = None
                return

            crc = zlib.crc32(msg.encode())
            msg_falha = self.inserir_falha(msg)
            pkt = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{dest};{crc};{msg_falha}"
            self.sock.sendto(pkt.encode(), (self.next_ip, self.next_port))
            print(f"[DADOS] enviado: {pkt}")
            self.awaiting_ack  = True
            # self.last_sent_msg = (dest, msg)  # Não precisa mais
            return

        # sem nada p/ fazer: repassa token
        time.sleep(5)
        self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
        print(f"[TOKEN] repassado por {self.nickname}")

    def handle_data(self, msg):
        # msg = "7777:status;origem;destino;crc;texto"
        payload = msg[len(DATA_MSG_PREFIX):]
        status, origem, destino, crc, texto = payload.split(";", 4)
        crc_calc = str(zlib.crc32(texto.encode()))

        # 1) Pacote não é para mim?
        if destino != self.nickname:
            # detectar timeout de dest que não existe
            if origem == self.nickname and status == "naoexiste":
                print(f"[FALHA] destinatário '{destino}' não existe ou está offline. Pacote: {msg}")
                self.awaiting_ack  = False
                self.last_sent_msg = None
                if self.current_msg:
                    self.msg_queue.get()  # Remove a mensagem da fila após falha
                    self.current_msg = None
                self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
                return
            # só repassa adiante
            self.sock.sendto(msg.encode(), (self.next_ip, self.next_port))
            return

        # 2) Chegou em mim
        if status == "naoexiste":
            # entrega inicial
            if crc != crc_calc:
                print(f"[ERRO] CRC inválido! Esperado {crc_calc}, recebido {crc}. Pacote: {msg}")
                status2 = "NACK"
            else:
                print(f"[RECEBIDA] {msg}")
                status2 = "ACK"
            # envia retorno (origem/destino trocados)
            retorno = f"{DATA_MSG_PREFIX}{status2};{self.nickname};{origem};{crc_calc};{texto}"
            self.sock.sendto(retorno.encode(), (self.next_ip, self.next_port))
            print(f"[RETORNO] enviado: {retorno}")
            return

        # 3) Retorno de ACK/NACK para o originador
        if status == "ACK":
            print(f"[CONFIRMADA] {msg}")
            self.awaiting_ack  = False
            self.last_sent_msg = None
            if self.current_msg:
                self.msg_queue.get()  # Remove a mensagem da fila após ACK
                self.current_msg = None
        else:  # NACK
            print(f"[NACK] {msg} será retransmitida.")
            self.retransmit    = (origem, texto)
            self.retransmit_done = False
            self.awaiting_ack  = False
            self.last_sent_msg = None

        # após processar retorno, libera token
        self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))

    def token_monitor_loop(self):
        while self.running:
            if not self.is_token_creator:
                time.sleep(1)
                continue
            if (time.time() - self.ultimo_token > self.token_timeout) and not self.token_gerado:
                print("[ALERTA] Token perdido! Gerando novo token...")
                self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
                self.ultimo_token = time.time()
                self.token_gerado  = True
            time.sleep(1)

    def start(self):
        print("=== INICIANDO NÓ ===")
        print(f"{self.nickname} | porta {self.listen_port} | próximo {self.next_ip}:{self.next_port}")
        print(f"Peers: {self.peers}")
        print("====================")

        self.receiver_thread.start()
        self.user_thread.start()
        self.token_monitor_thread.start()

        if self.is_token_creator:
            print(f"[TOKEN] aguardando 10s antes do inicial...")
            time.sleep(10)
            self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
            print("[TOKEN] inicial enviado!")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Encerrando...")
            self.running = False
        finally:
            self.sock.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Uso: python ring_node.py <config.txt> <porta>")
        sys.exit(1)
    node = RingNode(sys.argv[1], int(sys.argv[2]))
    node.start()
