
### 1. Enquadramento e Delimitação no SubControl (TCP Framing)

* **Contexto:** O TCP oferece um fluxo contínuo de bytes (*byte stream*), não delimitando o início e fim de mensagens.
* **Decisão:** Cada mensagem SubControl é antecedida por um campo de **1 Byte de comprimento** (`comprimento`), que indica exatamente o número de bytes do corpo da mensagem que se seguem (`tipo + campos`).
* **Justificação:**
  - Evita o uso de caracteres sentinela (delimitadores de texto como `\n` ou `\0`), que exigiriam varrimento byte a byte e técnicas de *escaping*.
  - Mensagens de controlo têm tamanho reduzido (o maior quadro, `SUBSCRIBE`, ocupa 50 bytes). Um único byte permite expressar comprimentos até 255 bytes ($0 \text{ a } 255$), minimizando a sobrecarga de largura de banda.

---

### 2. Codificação dos Campos do SubControl (Campos Fixos vs Variáveis)

* **Contexto:** É proibido recorrer a bibliotecas de serialização automática (como JSON, Protobuf, MessagePack ou Pickle). Todos os campos devem ser binários e construídos byte a byte.
* **Decisão:**
  - `comprimento`: 1 byte.
  - `tipo`: 1 byte (`1` = HELLO, `2` = HELLO_ACK, `3` = SUBSCRIBE, `4` = UNSUBSCRIBE, `5` = ACK).
  - `role`: 1 byte (`1` = publisher, `2` = subscriber).
  - `udp_port`: 2 bytes em formato *big-endian* (porta onde o subscritor escuta; 0 no publicador).
  - `id`: **16 bytes fixos**, string codificada em UTF-8 com preenchimento à direita em `0x00`.
  - `data_port`: 2 bytes em *big-endian* no `HELLO_ACK`.
  - `qos`: 1 byte (`1` = G1, `2` = G2).
  - `filtro`: **48 bytes fixos**, string UTF-8 com preenchimento à direita em `0x00`.
* **Justificação:**
  - A adoção de campos de tamanho fixo elimina a complexidade de descodificar múltiplos prefixos de tamanho intermédios e torna a estrutura previsível em inspeções com Wireshark.
  - O preenchimento com `0x00` permite a recuperação imediata da string original com `rstrip(b"\x00")`.

---

### 3. Descoberta Dinâmica de Portos de Dados UDP

* **Contexto:** O enunciado proíbe estritamente a fixação de portas estáticas no código (ex.: fixar 5000 causaria falhas no corretor automático). A configuração fornece aos clientes apenas a porta do plano de controlo TCP do Broker (`broker_control`).
* **Decisão:**
  - O Broker lê a chave `data_port` da sua configuração e vincula o socket UDP a essa porta.
  - Ao receber a mensagem `SC_HELLO` do cliente via TCP, o Broker responde com `SC_HELLO_ACK`, transportando nos seus 2 bytes de dados o número da sua `data_port`.
  - Simultaneamente, o subscritor cria o seu socket UDP numa porta temporária atribuída pelo sistema operativo, obtém a porta atribuída e envia-a no campo `udp_port` do seu `SC_HELLO`.
* **Justificação:**
  - O Broker aprende o IP do cliente através do socket TCP e o porto de dados UDP através do cabeçalho `SC_HELLO`.
  - O cliente aprende o porto de dados do Broker via `SC_HELLO_ACK`.
  - Ambos os lados descobrem dinamicamente onde escutar e enviar dados, respeitando o contrato de execução sem nada fixado no código.

---

### 4. Estrutura do Cabeçalho Binário do EventCast (UDP)

* **Contexto:** O transporte de dados via UDP necessita de metadados binários desenhados pelo grupo para identificar tipos de mensagem, fluxos, sequência, QoS e integridade.
* **Decisão:** O datagrama EventCast possui um cabeçalho binário fixo de **74 Bytes**, seguido do payload opaco:
  $$\text{kind (1B)} \mid \text{qos (1B)} \mid \text{pub\_id (16B)} \mid \text{fluxo (2B)} \mid \text{seq (4B)} \mid \text{topico (48B)} \mid \text{tam\_payload (2B)} \mid \text{payload}$$
  - `kind` (1B): `0x01` para PUBLISH, `0x02` para DELIVER, `0x03` para ACK.
  - `qos` (1B): `1` para G1, `2` para G2.
  - `pub_id` (16B): `--id` do publicador original em UTF-8 (preenchido com `0x00`).
  - `fluxo` (2B): Índice do fluxo de publicação (uint16 big-endian).
  - `seq` (4B): Número de sequência monótono dentro do fluxo (uint32 big-endian).
  - `topico` (48B): 3 níveis de 16 bytes cada (`regiao/estacao/metrica`).
  - `tam_payload` (2B): Comprimento em bytes do payload (uint16 big-endian).
  - `payload`: Bytes de dados do sensor.
* **Justificação:**
  - A separação entre cabeçalho fixo e payload permite ao parser validar de imediato se o pacote cumpre o tamanho mínimo ($74 \text{ bytes}$) antes de extrair dados.
  - O campo `tam_payload` explícito previne que ruído ou truncaturas na rede corrompam a leitura da aplicação.

---

### 5. Estruturação dos Tópicos em Três Níveis Fixos

* **Contexto:** O espaço de nomes exige exatamente três níveis (`regiao/estacao/metrica`) e proíbe caracteres universais no meio (ex.: `north/*/temperature` é inválido).
* **Decisão:** Os 48 bytes do campo `topico` são segmentados em três subcampos contíguos de **16 Bytes cada**:
  - `regiao`: 16 bytes UTF-8 (*null-padded*);
  - `estacao`: 16 bytes UTF-8 (*null-padded*);
  - `metrica`: 16 bytes UTF-8 (*null-padded*).
* **Justificação:**
  - Assegura uma representação tabular e alinhada dos tópicos na rede.
  - Facilita futuras expansões (como a propagação de subscrições e agregação de rotas na Fase 4) e agiliza a verificação de prefixos pelo Broker sem necessidade de parsing complexo de delimitadores `/`.

---

### 6. Identificação Universal da Mensagem (`pub_id`, `fluxo`, `seq`)

* **Contexto:** Cada publicação necessita de um identificador uníco de ponta a ponta (`message_id`) que deve constar de forma idêntica tanto no ficheiro de log do publicador como no do subscritor.
* **Decisão:** O `message_id` é gerado estruturadamente como:
  $$\text{message\_id} = \text{f "}\{\text{pub\_id}\}\text{-}\{\text{fluxo}\}\text{-}\{\text{seq}\}\text{"}$$
  - No publicador: gravado no log antes do envio e decomposto nos campos binários do cabeçalho UDP.
  - No Broker: os campos `pub_id`, `fluxo` e `seq` são mantidos estritamente inalterados no encaminhamento.
  - No subscritor: recomposto na receção e gravado no log de entregas.
* **Justificação:**
  - Garante unicidade global mesmo com múltiplos publicadores, múltiplos fluxos por publicador e milhares de mensagens.

---

### 7. Desduplicação e Filtragem no Broker (Otimização de Rede)

* **Contexto:** Um subscritor pode ter filtros sobrepostos (ex.: `north/*` e `north/station7/temperature`). O enunciado exige que o subscritor receba apenas **uma cópia** da publicação e dá a liberdade de decidir onde descartar os duplicados (no Broker ou no Subscritor).
* **Decisão:** A decisão e desduplicação é efetuada **no Broker**.
* **Justificação:**
  - Se a decisão fosse tomada no subscritor, o broker enviaria múltiplos datagramas através dos routers do CORE para cada filtro correspondente, saturando desnecessariamente a rede.
  - Fazendo a correspondência no broker, é enviado **apenas um único datagrama UDP** por subscritor interessado, poupando largura de banda de rede e processamento nos nós recetores.

---

### 8. Cálculo Dinâmico do Nível de Garantia (Semântica QoS)

* **Contexto:** Cada fluxo de publicação e cada filtro de subscrição declaram um nível de garantia (QoS 1 = at-least-once, QoS 2 = exactly-once).
* **Decisão:** No momento da entrega, o Broker calcula o QoS efetivo do datagrama `EC_DELIVER` com base na fórmula:
  $$\text{QoS}_{\text{entrega}} = \min(\text{QoS}_{\text{fluxo}}, \max(\text{QoS}_{\text{filtros\_correspondentes}}))$$
* **Justificação:**
  - Cumpre rigorosamente o enunciado: quem publica decide o valor máximo da informação publicada, e uma subscrição nunca pode elevar a garantia acima da do fluxo de origem ($\min$).
  - Perante filtros sobrepostos do mesmo subscritor com garantias distintas (ex.: um com QoS 1 e outro com QoS 2), aplica-se a garantia mais forte ($\max$), satisfazendo ambos os requisitos com uma só entrega.

---

### 9. Sincronização de Arranque via `start_after`

* **Contexto:** No arranque simultâneo pelo banco de testes automático, os processos são lançados em paralelo. Se o publicador começasse a transmitir imediatamente, os subscritores poderiam ainda não ter concluído o handshake TCP de subscrição, perdendo-se mensagens iniciais.
* **Decisão:**
  - O Publicador estabelece de imediato a sessão SubControl (TCP) com o broker para obter a porta de dados.
  - O Publicador faz `time.sleep(self.start_after)` antes de iniciar as threads dos fluxos de publicação.
* **Justificação:** Dá tempo aos subscritores para abrirem as suas sessões e registarem os seus filtros de subscrição, garantindo determinismo nos testes de avaliação.

---

### 10. Robustez contra Cabeçalhos Truncados e Erros de Protocolo

* **Contexto:** Redes não fiáveis podem entregar datagramas fragmentados ou corrompidos.
* **Decisão:** A classe utilitária `Reader` verifica estritamente os limites em cada leitura (`pos + n <= len(data)`). Se faltarem bytes, é levantada a exceção `ProtocolError("cabeçalho truncado")`.
  - No Broker e no Subscritor (plano UDP): o datagrama inválido é silenciosamente descartado sem crashar o processo.
  - No SubControl (plano TCP): se um quadro estiver incompleto ou violar o protocolo, a conexão TCP é fechada ordenadamente e os recursos libertados.
* **Justificação:** Impede ataques de negação de serviço, estouro de memória e comportamentos indefinidos provocados por pacotes malformados.

---

### 11. Modelo de Concorrência e Gestão de Sessões

* **Contexto:** O sistema tem de gerir múltiplos publicadores e subscritores em simultâneo.
* **Decisão:**
  - **Broker:**
    - Uma thread dedicada de aceitação TCP (`_accept_loop`).
    - Uma thread por cada cliente TCP ligado (`_serve_client`), permitindo trocas assíncronas de mensagens de controlo sem bloquear outros clientes.
    - Loop principal síncrono dedicado à escuta e encaminhamento de datagramas UDP (`_route`).
    - Estado de sessões (`self.sessions`) protegido por mutex (`threading.Lock`).
  - **Publisher:**
    - Uma thread independente por cada fluxo da chave `publish`, permitindo ritmos e volumes de tráfego concorrentes.
* **Justificação:** Arquitetura leve e sem dependências externas pesadas, mantendo o código claro e de fácil explicação técnica na demonstração oral.
