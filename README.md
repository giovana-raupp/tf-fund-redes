# Trabalho Final - Redes de Computadores

## Simulação de Rede em Anel com Token e CRC

Este projeto implementa uma rede local em anel, simulando o envio de mensagens entre máquinas usando o protocolo UDP, controle por token, detecção de erros com CRC32 e retransmissão automática.

---

## Objetivo

- Simular o funcionamento de uma rede em anel, onde apenas quem possui o token pode transmitir mensagens.
- Implementar fila de mensagens, controle de token, broadcast, retransmissão, detecção de destinatário inexistente e visualização do caminho dos pacotes.

---

## Estrutura dos Arquivos de Configuração

Cada nó deve ter um arquivo de configuração no formato:

```
<ip_destino_do_token>:<porta>
<apelido_da_maquina>
<tempo_token_em_segundos>
<gera_token_inicial: true|false>
```

**Exemplo:**
```
127.0.0.1:6002
Giovana
1
true
```

---

## Como Rodar

1. **Abra três terminais** (um para cada nó).
2. Execute em cada terminal, trocando o arquivo de configuração e a porta conforme o nó:

```
python src/ring_node.py src/config_Giovana.txt 6001
python src/ring_node.py src/config_Samara.txt 6000
python src/ring_node.py src/config_Mykelly.txt 6002
```

3. **Envie mensagens** digitando no terminal:
   - Para um nó específico:
     ```
     Samara Olá, Samara!
     ```
   - Para todos (broadcast):
     ```
     TODOS Olá, grupo!
     ```

---

## Como Encerrar Todos os Nós Rapidamente

Se quiser encerrar todos os processos Python (todos os nós) de uma vez, use o comando:

```
pkill -f python
```

Isso é útil para finalizar rapidamente todos os terminais abertos durante os testes.

---

## Funcionalidades

- **Token:** Apenas quem possui o token pode enviar mensagens.
- **Fila:** Cada nó possui uma fila de até 10 mensagens.
- **Unicast e Broadcast:** Envio para um destino ou para todos.
- **Controle de erro:** CRC32 e módulo de inserção de falhas.
- **Retransmissão:** Mensagem retransmitida apenas uma vez após NACK.
- **Detecção de destinatário inexistente:** Mensagem removida da fila e aviso ao usuário.
- **Token perdido/duplicado:** O nó criador monitora a ausência do token e gera um novo quando necessário; tokens duplicados são descartados.
- **Visualização:** Prints mostram o caminho dos pacotes, retransmissões, fila cheia, etc.

---

## Exemplos de Prints

- `[DADOS] enviado: 7777:naoexiste;Giovana;Samara;...;Mensagem 1`
- `[REPASSANDO] 7777:naoexiste;Giovana;Samara;...;Mensagem 1`
- `[RECEBIDA] 7777:naoexiste;Giovana;Samara;...;Mensagem 1`
- `[RETORNO] enviado: 7777:ACK;Samara;Giovana;...;Mensagem 1`
- `[CONFIRMADA] 7777:ACK;Samara;Giovana;...;Mensagem 1`
- `[BROADCAST-VISUALIZADO] Mykelly viu broadcast de Giovana para TODOS: Olá, grupo!`
- `[FILA] cheia, descartei mensagem.`
- `[FALHA] destinatário 'Fulano' não existe ou está offline. Pacote: ...`
- `[ALERTA] Token duplicado detectado! Ignorando token recebido cedo demais.`
- `[ALERTA] Token perdido! Gerando novo token...`

---   

## Observações

- O sistema foi testado com 3 nós, mas pode ser expandido para mais.
- Para simular falhas, ajuste o parâmetro `prob` na função `inserir_falha`.
- Falta comentar o código e testar entre 3 máquinas.
  
---

**Desenvolvido para o Trabalho Final de Redes de Computadores.** 
