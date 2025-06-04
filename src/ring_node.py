import socket
import threading
import time
import queue
import zlib
import random
import os

# ===============================
# Trabalho Final - Redes de Computadores
# Simulação de Rede em Anel com Token e CRC
# ===============================

TOKEN_MSG = "9000"  # Valor do token
DATA_MSG_PREFIX = "7777:"  # Prefixo dos pacotes de dados
# ---------------------------------------------------
# Tempo (em segundos) que cada nó "segura" um pacote
DEFAULT_DATA_TIME = 2
# ---------------------------------------------------

class RingNode:
    def __init__(self, config_file, listen_port):
        # Inicializa o nó lendo as configurações e preparando as threads e variáveis de controle
        self.read_config(config_file)
        self.data_time      = DEFAULT_DATA_TIME  # Tempo de espera para envio de dados
        self.listen_port    = listen_port  # Porta UDP para escuta
        self.msg_queue      = queue.Queue(maxsize=10)  # Fila de mensagens a serem enviadas
        self.running        = True  # Controle de execução das threads
        self.awaiting_ack   = False  # Indica se está aguardando confirmação de entrega
        self.last_sent_msg  = None  # Última mensagem enviada (não mais usada)
        self.retransmit     = None  # Armazena mensagem a ser retransmitida após NACK
        self.retransmit_done = False  # Flag para garantir retransmissão única após NACK
        self.current_msg    = None  # Mensagem atualmente em processamento

        # Cria e configura o socket UDP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Permite broadcast
        self.sock.bind(("", self.listen_port))

        # Cria a thread responsável por receber mensagens da rede (escuta o socket UDP)
        self.receiver_thread      = threading.Thread(target=self.receiver_loop,      daemon=True)
        # Cria a thread responsável por ler comandos do usuário (input do terminal)
        self.user_thread          = threading.Thread(target=self.user_loop,          daemon=True)
        # Cria a thread responsável por monitorar o token e gerar um novo se necessário
        self.token_monitor_thread = threading.Thread(target=self.token_monitor_loop, daemon=True)

        # Controle do token
        self.ultimo_token  = time.time()  # Última vez que o token passou
        self.token_timeout = 30          # Tempo limite para considerar token perdido
        self.token_gerado  = False        # Flag para evitar múltiplos tokens

        # Lista de peers (apelidos dos outros nós, para broadcast)
        # self.discover_peers()  # Comentado pois não estamos mais utilizando a lista de peers

    def read_config(self, config_file):
        """Lê o arquivo de configuração do nó.
        Formato:
        1. ip:porta do próximo nó
        2. apelido do nó
        3. tempo do token (s)
        4. se gera o token inicial (true|false)
        """
        with open(config_file, 'r') as f:
            lines = [l.strip() for l in f if l.strip()]
        ip, port = lines[0].split(":")
        self.next_ip          = ip
        self.next_port        = int(port)
        self.nickname         = lines[1]
        self.token_time       = int(lines[2])
        self.is_token_creator = lines[3].lower() == 'true'

    # def discover_peers(self):
    #     """Descobre os peers lendo todos os arquivos de configuração na pasta."""
    #     cfg_dir = os.path.dirname(__file__)
    #     for fn in os.listdir(cfg_dir):
    #         if fn.startswith("config_") and fn.endswith(".txt"):
    #             with open(os.path.join(cfg_dir, fn)) as f:
    #                 lns = [l.strip() for l in f if l.strip()]
    #             if len(lns) >= 2 and lns[1] != self.nickname:
    #                 self.peers.append(lns[1])

    def receiver_loop(self):
        """Thread principal de recepção de pacotes UDP."""
        while self.running:
            try:
                data, _ = self.sock.recvfrom(4096)
                msg = data.decode(errors='ignore')
                if msg == TOKEN_MSG:
                    # Verifica token duplicado (chegou cedo demais)
                    tempo_desde_ultimo = time.time() - self.ultimo_token
                    if tempo_desde_ultimo < self.token_time * 0.8:  # Fator de segurança
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
        """Thread para leitura de comandos do usuário (input de mensagens)."""
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

    def inserir_falha(self, mensagem, prob=0.0):
        """Insere falha aleatória na mensagem com probabilidade 'prob'."""
        if random.random() < prob and mensagem:
            i = random.randint(0, len(mensagem)-1)
            c = chr((ord(mensagem[i]) + 1) % 256)
            return mensagem[:i] + c + mensagem[i+1:]
        return mensagem

    def handle_token(self):
        """Processa o token recebido: envia mensagem, retransmite ou repassa token."""
        print(f"[TOKEN] aguardando {self.token_time}s...")
        time.sleep(self.token_time)

        # Retransmissão de NACK pendente (apenas uma vez)
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

        # Nova mensagem da fila?
        if self.current_msg is None and not self.msg_queue.empty() and not self.awaiting_ack:
            self.current_msg = self.msg_queue.queue[0]  # Pega a próxima mensagem sem remover

        if self.current_msg and not self.awaiting_ack:
            dest, msg = self.current_msg
            print(f"[DADOS] preparando para {dest}, aguardando {self.data_time}s...")
            time.sleep(self.data_time)

            if dest == "TODOS":
                # Envia a mensagem para o próximo nó do anel, não via broadcast UDP
                crc = zlib.crc32(msg.encode())
                msg_falha = self.inserir_falha(msg)
                pkt = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};TODOS;{crc};{msg_falha}"
                self.sock.sendto(pkt.encode(), (self.next_ip, self.next_port))
                print(f"[DADOS-ANEL-TODOS] enviado para {self.next_ip}:{self.next_port}: {pkt}")
                self.msg_queue.get()  # Remove a mensagem da fila após envio
                self.current_msg = None
                return

            # Unicast normal
            crc = zlib.crc32(msg.encode())
            msg_falha = self.inserir_falha(msg)
            pkt = f"{DATA_MSG_PREFIX}naoexiste;{self.nickname};{dest};{crc};{msg_falha}"
            self.sock.sendto(pkt.encode(), (self.next_ip, self.next_port))
            print(f"[DADOS] enviado: {pkt}")
            self.awaiting_ack  = True
            return

        # Sem nada p/ fazer: repassa token
        time.sleep(5)
        self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
        print(f"[TOKEN] repassado por {self.nickname}")

    def handle_data(self, msg):
        """Processa pacotes de dados recebidos."""
        # msg = "7777:status;origem;destino;crc;texto"
        payload = msg[len(DATA_MSG_PREFIX):]
        status, origem, destino, crc, texto = payload.split(";", 4)
        crc_calc = str(zlib.crc32(texto.encode()))

        # Log para visualizar mensagens broadcast (TODOS) em nós intermediários
        if destino == "TODOS" and origem != self.nickname and status == "naoexiste":
            print(f"[BROADCAST-VISUALIZADO] {self.nickname} viu broadcast de {origem} para TODOS: {texto}")

        # 1) Pacote não é para mim?
        if destino != self.nickname:
            # Se for broadcast (destino == "TODOS"), processa mas NÃO repassa!
            if destino == "TODOS":
                # Já processou, não repassa para evitar loop
                return
            # Detectar timeout de destinatário que não existe
            if origem == self.nickname and status == "naoexiste":
                print(f"[FALHA] destinatário '{destino}' não existe ou está offline. Pacote: {msg}")
                self.awaiting_ack  = False
                self.last_sent_msg = None
                if self.current_msg:
                    self.msg_queue.get()  # Remove a mensagem da fila após falha
                    self.current_msg = None
                self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))
                return
            # Só repassa adiante
            print(f"[REPASSANDO] {msg}")
            self.sock.sendto(msg.encode(), (self.next_ip, self.next_port))
            return

        # 2) Chegou em mim (destinatário)
        if status == "naoexiste":
            # Entrega inicial
            if crc != crc_calc:
                print(f"[ERRO] CRC inválido! Esperado {crc_calc}, recebido {crc}. Pacote: {msg}")
                status2 = "NACK"
            else:
                print(f"[RECEBIDA] {msg}")
                status2 = "ACK"
            # Envia retorno (origem/destino trocados)
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

        # Após processar retorno, libera token
        self.sock.sendto(TOKEN_MSG.encode(), (self.next_ip, self.next_port))

    def token_monitor_loop(self):
        """Thread que monitora o token: só o nó criador pode gerar novo token se perdido."""
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
        """Inicia as threads e o funcionamento do nó."""
        print("=== INICIANDO NÓ ===")
        print(f"{self.nickname} | porta {self.listen_port} | próximo {self.next_ip}:{self.next_port}")
        # self.discover_peers()  # Comentado pois não estamos mais utilizando a lista de peers
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