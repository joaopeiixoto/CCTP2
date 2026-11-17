# Decisões de desenho — SubControl (TCP) e EventCast (UDP)

> Estado: fases 1 e 2 implementadas e testadas, com os cabeçalhos do relatório mais o `hop_seq` (`tests/test_phase2.py`, ver `TESTES.md`).
> A secção 15 lista o que **ainda não existe** e é só intenção.

## 0. Limites assumidos

| Campo | Limite | Onde é validado |
|---|---|---|
| `--id` | 1 a 16 bytes UTF-8, sem espaços | `main.py` (erro → código de saída 2) |
| Nível de tópico (`regiao`, `estacao`, `metrica`) | 1 a 16 bytes UTF-8, sem `*`, espaços nem `0x00` | `topics.py`, ao ler a configuração e ao descodificar |
| Tópico / filtro na rede | 48 bytes (3×16 bytes, sem barras) | consequência dos limites acima |
| `fluxo` | 0 a 65535 (uint16) | no publicador, ao ler a configuração |
| `count` de um fluxo | até 2³² (o `seq` é uint32, de 0 a 2³²−1) | no publicador, ao ler a configuração |
| `payload` | 0 a 1394 bytes | `ec_encode` (envio) e `ec_decode` (receção) |

**Justificação:** campos de tamanho fixo exigem limites explícitos. Sem validação à entrada, um valor excessivo só rebentava mais tarde, a meio de uma thread de publicação. Falhar no arranque, com mensagem clara, é preferível a falhar a meio da execução. O máximo de payload, 1394 = 1500 (MTU) − 20 (IP) − 8 (UDP) − 78 (cabeçalho), garante que nenhum datagrama é fragmentado: com fragmentação, perder um fragmento é perder o datagrama inteiro.

---

## 1. Delimitação das mensagens no SubControl

* **Decisão:** cada mensagem é precedida por 1 byte com o comprimento do corpo (`tipo + campos`). O recetor lê 1 byte e depois exatamente esse número de bytes (`recv_exact`).
* **Porquê 1 byte:** o maior quadro, `SUBSCRIBE`, tem corpo de 50 bytes (1 + 1 + 48), muito abaixo de 255.
* **Validação dupla:** cada tipo tem um tamanho fixo (`SC_SIZE`). Um quadro cujo comprimento não corresponde ao tipo é inválido e fecha a ligação.
* **Porquê prefixo e não sentinela:** o corpo contém bytes arbitrários (portas, `0x00` de preenchimento), por isso um delimitador exigiria *escaping*.

## 2. Mensagens e campos do SubControl

Todos os inteiros são *big-endian*. Os textos são UTF-8 preenchidos à direita com `0x00`.

| Tipo | Valor | Corpo (depois do comprimento) | Tamanho do corpo |
|---|---|---|---|
| `HELLO` | 1 | `tipo(1) role(1) udp_port(2) id(16)` | 20 |
| `HELLO_ACK` | 2 | `tipo(1) data_port(2)` | 3 |
| `SUBSCRIBE` | 3 | `tipo(1) qos(1) filtro(48)` | 50 |
| `UNSUBSCRIBE` | 4 | `tipo(1) filtro(48)` | 49 |
| `ACK` | 5 | `tipo(1)` | 1 |

`role`: 1 = publicador, 2 = subscritor. `udp_port` é a porta UDP onde o cliente recebe datagramas: ACKs, no publicador; DELIVERs, no subscritor.

* **`filtro` com 48 bytes e sem barras:** três campos de 16 bytes, `regiao(16) estacao(16) metrica(16)`. Um nível universal `*` é um campo vazio (só `0x00`): `north/*` viaja como `north` + 32 bytes `0x00`; `north/station7/*` como `north` + `station7` + 16 bytes `0x00`. Na receção, um campo vazio seguido de um campo preenchido (o `*` no meio, `north/*/temperature`) é rejeitado.
* **Um único `ACK` genérico:** cada pedido do cliente recebe `ACK` antes do seguinte, por isso o `ACK` não precisa de identificar o pedido. O cliente espera-o no máximo 5 s (`SC_REPLY_TIMEOUT_S`).
* **O SubControl não tem mensagem de erro nem de terminação.** Perante um pedido inválido o broker fecha a ligação sem resposta. O fim normal de um cliente é fechar o TCP. A **expulsão** é o broker fechar a ligação (secção 11).

## 3. Sessão antes de dados, e descoberta das portas

Nenhuma porta de dados está no código nem na configuração dos clientes.

1. O cliente abre o seu socket UDP numa porta efémera, **antes** do `HELLO`.
2. Liga-se ao `broker_control` e envia `HELLO` com essa porta em `udp_port`.
3. O broker regista a sessão com o endereço UDP `(IP observado na ligação TCP, udp_port)` e responde `HELLO_ACK` com a sua `data_port`. **O cliente só conhece a porta de dados depois deste `HELLO_ACK`**, por isso nunca envia um datagrama sem ter a sessão aberta.
4. O broker **só aceita datagramas de endereços com sessão**: um `PUBLISH` tem de vir do endereço de um publicador, um `ACK` do endereço de um subscritor.

Na fase 1 o publicador enviava `udp_port = 0`; na fase 2 recebe ACKs, por isso anuncia a sua porta.

## 4. Cabeçalho EventCast (UDP)

**PUBLISH (1) e DELIVER (2)** — cabeçalho fixo de **78 bytes** seguido do payload:

| Offset | Campo | Bytes | Significado |
|---|---|---|---|
| 0 | `kind` | 1 | 1 = PUBLISH (publicador→broker), 2 = DELIVER (broker→subscritor) |
| 1 | `qos` | 1 | 1 = G1, 2 = G2 — a garantia **desta** entrega |
| 2 | `hop_seq` | 4 | número da transmissão **nesta ligação**: o que a janela usa e o ACK confirma |
| 6 | `pub_id` | 16 | `--id` do publicador original |
| 22 | `fluxo` | 2 | índice do fluxo `publish` na configuração do publicador |
| 24 | `seq` | 4 | contador do fluxo, começa em 0 |
| 28 | `topico` | 48 | `regiao(16) estacao(16) metrica(16)`, sem barras |
| 76 | `tam_payload` | 2 | comprimento do payload (0 a 1394) |
| 78 | `payload` | `tam_payload` | opaco para o protocolo |

**ACK (3)** — **5 bytes** fixos: `kind(1) | hop_seq(4)`.

* **Dois identificadores, dois âmbitos** (secção 3.5 do enunciado). O `hop_seq` só tem significado numa ligação: o publicador numera o que envia ao broker, e o broker numera, **por subscritor**, o que envia a cada um. Por isso é contíguo em cada ligação (0, 1, 2…), o que permite uma janela deslizante. O trio `pub_id/fluxo/seq` identifica a publicação de ponta a ponta e o broker copia-o sem o alterar.
* **O ACK só leva o `hop_seq`:** o endereço de origem do datagrama identifica a ligação, e o `hop_seq` identifica a transmissão dentro dela.
* **Porquê não confirmar com `pub_id/fluxo/seq`:** (1) o broker não deduplica em G1 (secção 10), por isso a mesma publicação pode estar duas vezes em trânsito para um subscritor, e um ACK com o identificador de ponta a ponta seria ambíguo; (2) um subscritor recebe vários fluxos misturados e só alguns deles, e os `seq` que lhe chegam não são contíguos, o que impede uma janela deslizante; (3) na fase 4 cada salto entre brokers vai precisar do seu próprio número.
* **Leitura pelo recetor:** com o cabeçalho fixo, cada campo está sempre na mesma posição. O recetor verifica que há pelo menos 78 bytes, lê os campos pelos offsets, e exige que o datagrama tenha **exatamente** `78 + tam_payload` bytes. Com menos, está truncado; com mais, é inválido. Em ambos os casos é descartado.
* **Sem campo de integridade próprio:** o checksum UDP cobre a corrupção.

## 5. Tópicos e correspondência

* **Representação na rede:** três subcampos de 16 bytes, sem barras, quer em publicações (EventCast) quer em filtros de subscrição (SubControl). As barras não viajam: são as fronteiras entre os campos. Na receção, `decode_topic` / `decode_filter` retiram o preenchimento `0x00` de cada campo e voltam a juntar os níveis com `/`.
* **Representação no broker:** cada sessão guarda `dict[filtro → qos]`, com o filtro em texto (`north/*`), e `matches(filtro, topico)` compara por igualdade ou por `startswith` do prefixo (`north/*` → `north/`). O prefixo inclui a barra, por isso `north/*` não corresponde a `northeast/...`.
* **Custo:** uma publicação percorre todos os filtros de todos os subscritores, O(S·F). Aceitável para as dimensões do trabalho (até 6 subscritores, poucos filtros).
* **Validação:** um filtro é um tópico completo ou um prefixo de 1 ou 2 níveis terminado em `/*`. Um `*` noutra posição é inválido. O broker rejeita-o fechando a sessão, e o subscritor rejeita-o ao ler a configuração (código de saída 2).

## 6. Identificador de mensagem e volta dos contadores

`message_id = "{pub_id}-{fluxo}-{seq}"`

* O publicador escreve a linha de registo **na primeira transmissão, antes do envio**; as retransmissões não escrevem nada. Se o processo morrer entre as duas operações, o registo de referência fica com uma mensagem que pode não ter chegado, em vez de faltar-lhe uma que chegou.
* O subscritor reconstrói a string a partir do cabeçalho, pelo que o `message_id` é igual nos dois registos. É único porque os `--id` são distintos e porque `fluxo` e `seq` são inteiros (lida da direita, a string não é ambígua mesmo que o `--id` tenha hífenes).
* **`seq` não dá a volta:** `count` está limitado a 2³².
* **`hop_seq` dá a volta** (módulo 2³², `SendWindow.add`). A janela é calculada módulo 2³² (`(next − base) mod 2³² < N`, aritmética de números de série, RFC 1982). Como N = 4096 é muito menor do que 2³¹, dois pendentes nunca têm o mesmo `hop_seq`, e um ACK nunca é ambíguo, mesmo depois da volta. A condição clássica do selective repeat, N ≤ metade do espaço de números, está cumprida com muita margem. A volta está testada em `t_cabecalhos`.

## 7. Cópia única decidida no broker

Com filtros sobrepostos, o broker junta todos os filtros de um subscritor que correspondem ao tópico e envia **um** datagrama por subscritor.

* A alternativa, decidir no subscritor, enviaria um datagrama por filtro correspondido e descartaria os excedentes depois de atravessarem o anel. Ambas cumprem a regra, mas só esta poupa tráfego nas ligações entre routers.

## 8. Garantia de cada entrega

`qos_entrega = min(qos_fluxo, max(qos dos filtros correspondidos))`

* O valor segue no campo `qos` do `DELIVER`, e é o que o subscritor consulta: com `qos = 2` consulta a tabela de duplicados, com `qos = 1` regista sempre. É esta a linha de código que distingue G1 de G2 (`subscriber.py`, `if msg.qos == 2`).
* **A fiabilidade é a mesma para os dois níveis:** o mecanismo de retransmissão (secção 9) não depende do qos. O qos só decide se o recetor deduplica.
* **Tabela de duplicados:** conjunto de `(topico, message_id)` que cresce sem limite durante a execução. Aceitável para a duração dos testes.

## 9. Fiabilidade sobre UDP — selective repeat

Implementado em `reliability.py` (`SendWindow`) e usado nas duas ligações: publicador→broker e broker→cada subscritor.

* **Emissor — janela deslizante:** a janela é o intervalo `[base, base + N)` de `hop_seq`, em que `base` é o `hop_seq` mais antigo ainda por confirmar. Só se envia um `hop_seq` novo se couber na janela. Cada datagrama enviado fica pendente com o **seu próprio temporizador**. Um ACK retira exatamente esse `hop_seq`; se era a `base`, a janela desliza até ao pendente seguinte (ou até `next`, se já não há nenhum). Quando um temporizador expira, retransmite-se **só esse datagrama**, com o **mesmo** `hop_seq` (não os que vinham a seguir, como no go-back-N), e o seu RTO duplica.
* **Recetor:** confirma **cada datagrama bem formado**, incluindo duplicados e entregas de tópicos que já cancelou: é o ACK que pára as retransmissões. Não tem buffer de reordenação: a ordem não é garantida (secção 4 do enunciado), por isso entrega logo. Também **não descarta duplicados ao nível da ligação** — num selective repeat clássico o recetor descarta o que já recebeu, e isso seria eliminar duplicados em G1.
* **Tamanho da janela, N = 4096:** numa janela deslizante, um datagrama perdido várias vezes seguidas trava a `base` até ser confirmado. N tem de cobrir o ritmo × o tempo de recuperação: a 500 mensagens/s, cinco perdas seguidas do mesmo datagrama demoram 0,2 + 0,4 + 0,8 + 1 + 1 = 3,4 s, ou seja 1700 mensagens. Com N = 1024 medimos o burst de 2000 mensagens com 10% de perda a demorar 5,37 s em vez de 4,0 s, porque a janela travava; com N = 4096 demora 4,00 s. Quando a janela está cheia, o publicador espera por ACKs (o ritmo atrasa, sem rajada de compensação) e o broker guarda as entregas na fila desse subscritor.
* **O recetor não tem janela:** não reordena nem descarta duplicados, e confirma cada datagrama, por isso não há buffer do lado dele a proteger.
* **O broker confirma cada PUBLISH logo que o põe nas filas**, mesmo sem nenhum subscritor interessado. A partir daí a publicação é responsabilidade do broker; sem esse ACK o publicador retransmitiria até ao fim do `drain`.
* **Um subscritor parado não atrasa os outros:** cada subscritor tem a sua janela, a sua fila e o seu `hop_seq`. O publicador recebe o ACK do broker independentemente do estado dos subscritores.
* **Medido** (`tests/test_phase2.py`, 300 publicações, perda de 30% em cada sentido): 404 retransmissões do broker para 404 perdas (239 DELIVER e 165 ACK perdidos). Cada retransmissão corresponde a uma perda e não houve retransmissões espúrias.

## 10. Sem deduplicação no broker (G1)

* O broker distribui cada `PUBLISH` que recebe, incluindo retransmissões do publicador cujo ACK se perdeu. O enunciado permite-o em G1 ("uma publicação retransmitida é entregue as vezes que chegar").
* Consequência: em G1, os duplicados no registo do subscritor vêm das duas ligações (ACK perdido entre o publicador e o broker, e ACK perdido entre o broker e o subscritor). Com 30% de perda medimos 193 duplicados em 200 publicações G1.
* Em G2 o subscritor já elimina os duplicados pela tabela da secção 8, por isso a garantia está cumprida. Deduplicar no broker para qos=2, e poupar esses datagramas à rede, é uma decisão da fase 3.

## 11. Expulsão (`evict_after`)

* Cada sessão de subscritor guarda o instante do último ACK recebido (`last_ack`) e o instante em que passou a ter algo por entregar (`pending_since`, `None` quando a fila e a janela estão vazias).
* A cada 20 ms (`TIMER_TICK`) o broker verifica, para cada subscritor com algo por entregar: `agora − max(last_ack, pending_since) ≥ evict_after`. Um subscritor sem nada por entregar nunca é expulso, porque "não confirmar" só faz sentido havendo algo a confirmar.
* **Expulsão:** com o lock, a sessão é retirada do estado e a fila e os pendentes são descartados, por isso nenhuma thread volta a enviar-lhe nada. Depois, `shutdown(SHUT_RDWR)` envia o FIN; a thread SubControl desse cliente acorda do `recv` e fecha o socket.
* **"Nem antes nem muito depois":** a precisão é a do `TIMER_TICK`. Medido: expulsão 2,028 s depois do `SIGSTOP`, com `evict_after = 2`.
* **Um subscritor parado menos do que `evict_after` mantém a sessão e recebe tudo:** o broker continua a retransmitir (com RTO até 1 s); o kernel guarda esses datagramas no buffer do socket do subscritor parado (o subscritor pede 4 MiB de `SO_RCVBUF`; o kernel limita-o a `net.core.rmem_max`, por omissão cerca de 208 KiB, o que chega para milhares de datagramas); com o `SIGCONT`, o subscritor lê-os e confirma-os de imediato. A fila do broker não tem limite, por isso nada se perde nesse intervalo.
* **Do lado do subscritor:** o ciclo principal vigia também o socket TCP. Fora de um pedido o broker não envia nada, por isso o TCP ficar legível significa fecho. O TCP é verificado **antes** do UDP, e por isso, depois de uma expulsão, nada do que estiver no buffer é registado. O processo continua até `duration`, sem confirmar nem registar, e termina com código 0.
* **`UNSUBSCRIBE`:** o broker retira o filtro e descarta da fila **e dos pendentes** tudo o que já não corresponde a nenhum filtro. Assim, depois do cancelamento não sai nenhum datagrama desse tópico, nem sequer retransmissões do que estava em trânsito.

## 12. Temporizadores

| Temporizador | Onde | Valor | Quando expira |
|---|---|---|---|
| RTO de um datagrama | publicador; broker (por subscritor) | 200 ms inicial, ×2 a cada expiração, teto 1 s | retransmite só esse datagrama e duplica o seu RTO |
| Janela | publicador; broker (por subscritor) | N = 4096 `hop_seq` | — (cheia: o emissor espera por ACKs) |
| Verificação de temporizadores | publicador (`IO_TICK`), broker (`TIMER_TICK`) | 20 ms | percorre os pendentes; no broker, verifica também a expulsão |
| `evict_after` | broker, por subscritor | configuração | fecha o TCP, descarta fila e pendentes |
| `drain` | publicador | configuração | pára de retransmitir e termina (antes, se tudo confirmado) |
| `start_after` | publicador | configuração | começa a publicar |
| `duration` | broker, subscritor | configuração | termina |
| Ação `at` | subscritor | configuração; granularidade 50 ms (`LOOP_TICK`) | envia SUBSCRIBE/UNSUBSCRIBE e espera o ACK |
| Resposta SubControl | clientes | 5 s | dá a sessão por perdida |
| Ligação ao broker | clientes | tenta durante 10 s, de 100 em 100 ms | o processo termina com código 1 |

* **Porquê 200 ms fixos:** no CORE o RTT é da ordem de 1 ms, por isso 200 ms é conservador e não gera retransmissões espúrias (nenhuma medida). O custo é a latência de recuperação de uma perda. Um RTO adaptativo (estimador de Jacobson/Karels, como o do TCP) reduziria esta latência, mas não é necessário para a correção.
* **Porquê teto de 1 s:** com backoff sem teto, um subscritor parado durante alguns segundos só voltaria a ser sondado muito depois, e a recuperação depois do `SIGCONT` atrasaria. Com 1 s, um subscritor parado continua a receber pelo menos uma retransmissão por segundo para o buffer.

## 13. Datagramas e quadros inválidos

* `Reader.take` verifica `pos + n <= len(dados)` e `Reader.end` verifica que não sobram bytes. Faltar ou sobrar um byte levanta `ProtocolError`. Um campo de texto com `0x00` no meio (e não só no fim, como preenchimento) também é inválido.
* **Plano UDP (broker, publicador e subscritor):** um datagrama inválido, truncado, com `kind` inesperado, `qos` fora de {1, 2}, tópico inválido ou vindo de um endereço sem sessão é descartado sem resposta e sem efeito no processo. Não é confirmado.
* **Plano TCP (broker):** um quadro incompleto, um comprimento 0 ou que não corresponde ao tamanho fixo do tipo, um tipo desconhecido ou um pedido inválido fecham a ligação e libertam a sessão. Os restantes clientes não são afetados (testado em `t_robustez`).

## 14. Concorrência

* **Broker:** uma thread aceita ligações TCP; uma thread por cliente trata o SubControl; a thread principal recebe os datagramas UDP; uma thread de temporizadores retransmite e expulsa. Todo o estado é protegido por um único `Lock`. Os envios UDP são feitos com o lock, para que nenhuma thread envie, depois de um cancelamento ou de uma expulsão, um datagrama que tenha preparado antes (um `sendto` UDP não bloqueia na prática).
* **Publicador:** uma thread por fluxo `publish`, cada uma com o seu ritmo, e uma thread de I/O que recebe os ACKs e retransmite. A janela é protegida por uma `Condition`, que também acorda os fluxos à espera de lugar. O registo tem o seu `Lock`.
* **Subscritor:** um único ciclo com `select` sobre o UDP e o TCP (timeout de 50 ms) que executa as ações `at` quando chegam ao seu instante. Não há threads; uma ação bloqueia a receção durante o seu `ACK` (no máximo 5 s).