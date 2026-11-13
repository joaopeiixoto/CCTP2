# Decisões de desenho — SubControl (TCP) e EventCast (UDP)

> Estado: fase 1 implementada. A secção 12 lista o que **ainda não existe** e é só intenção.

## 0. Limites assumidos

| Campo | Limite | Onde é validado |
|---|---|---|
| `--id` | 1 a 16 bytes UTF-8, sem espaços | `main.py` (erro → código de saída 2) |
| Nível de tópico (`regiao`, `estacao`, `metrica`) | 1 a 16 bytes UTF-8, sem `*` | `topics.py`, ao ler a configuração |
| Tópico / filtro na rede | 48 bytes (3×16 bytes, sem barras) | consequência dos limites acima |
| `fluxo` | 0 a 65535 (uint16) | no publicador, ao ler a configuração |
| `count` de um fluxo | até 2³² (uint32 de `seq`) | no publicador, ao ler a configuração |
| `payload` | até 65535 bytes (uint16) | `ec_encode` |

**Justificação:** campos de tamanho fixo exigem limites explícitos. Sem validação à entrada, um valor excessivo só rebentava mais tarde, a meio de uma thread de publicação. Falhar no arranque, com mensagem clara, é preferível a falhar a meio da execução.

---

## 1. Delimitação das mensagens no SubControl

* **Decisão:** cada mensagem é precedida por 1 byte com o comprimento do corpo (`tipo + campos`). O recetor lê 1 byte e depois exatamente esse número de bytes (`recv_exact`).
* **Porquê 1 byte:** o maior quadro, `SUBSCRIBE`, tem corpo de 50 bytes (1 + 1 + 48), muito abaixo de 255. Um prefixo de tamanho variável (como no MQTT) só compensaria com mensagens grandes.
* **Porquê prefixo e não sentinela:** o corpo contém bytes arbitrários (portas, `0x00` de preenchimento), por isso um delimitador exigiria *escaping*. O prefixo também dispensa varrer byte a byte.
* **Limite assumido:** `send_frame` rejeita corpos com mais de 255 bytes. Mensagens futuras entre brokers têm de caber neste limite ou o prefixo muda.

## 2. Mensagens e campos do SubControl

Todos os inteiros são *big-endian*. As strings são UTF-8 preenchidas à direita com `0x00` (`rstrip` recupera o original).

| Tipo | Valor | Corpo (depois do comprimento) |
|---|---|---|
| `HELLO` | 1 | `tipo(1) role(1) udp_port(2) id(16)` |
| `HELLO_ACK` | 2 | `tipo(1) data_port(2)` |
| `SUBSCRIBE` | 3 | `tipo(1) qos(1) filtro(48)` |
| `UNSUBSCRIBE` | 4 | `tipo(1) filtro(48)` |
| `ACK` | 5 | `tipo(1)` |

`role`: 1 = publicador, 2 = subscritor.

* **Campos fixos em vez de prefixos internos:** o parse fica uma sequência de leituras de tamanho conhecido e a verificação de limites reduz-se a uma só (secção 10).
* **`filtro` com 48 bytes e sem barras:** usa a mesma representação do tópico no EventCast (três campos contíguos de 16 bytes: `regiao(16) estacao(16) metrica(16)`), em que um nível universal é representado por um campo vazio preenchido com `0x00` (ex.: `north/*` viaja com `regiao="north"` e `estacao`/`metrica` a `0x00`). Elimina a necessidade de enviar ou fazer parse de barras na rede, tornando a descodificação direta e unificando o formato dos dois protocolos em 48 bytes.
* **Um único `ACK` genérico:** cada pedido do cliente (`HELLO` exceto, que tem `HELLO_ACK`) recebe `ACK` antes do seguinte, por isso o `ACK` não precisa de identificar o pedido. O cliente espera-o em modo pedido/resposta (`request`).
* **O SubControl não tem mensagem de erro nem de terminação.** Perante um pedido inválido o broker fecha a ligação sem resposta. O fim normal de um cliente é fechar o TCP, e o broker trata o fim de ligação como fim de sessão, retirando-a do estado. Evita mensagens que o enunciado não exige e deixa o TCP sinalizar o fim.

## 3. Descoberta dos portos de dados

Nenhum porto de dados está no código nem na configuração dos clientes.

1. O cliente liga-se ao `broker_control` e envia `HELLO`.
2. O broker responde `HELLO_ACK` com a sua `data_port`. **Responde antes de aceitar qualquer outra mensagem**, por isso o cliente nunca envia um datagrama sem ter a sessão aberta.
3. O subscritor abre o socket UDP numa porta efémera, antes do `HELLO`, e envia-a em `udp_port`. O broker junta-lhe o IP observado na ligação TCP e fica com o destino das entregas.
4. O publicador envia `udp_port = 0`, porque na fase 1 não recebe nada por UDP.

## 4. Cabeçalho EventCast (UDP)

Cabeçalho fixo de 74 bytes seguido do payload:

| Campo | Bytes | Significado |
|---|---|---|
| `kind` | 1 | 1 = PUBLISH, 2 = DELIVER, 3 = ACK (reservado, fase 2) |
| `qos` | 1 | 1 = G1, 2 = G2 |
| `pub_id` | 16 | `--id` do publicador original |
| `fluxo` | 2 | índice do fluxo `publish` na configuração do publicador |
| `seq` | 4 | contador do fluxo, começa em 0 |
| `topico` | 48 | `regiao(16) estacao(16) metrica(16)` |
| `tam_payload` | 2 | comprimento do payload |
| `payload` | variável | opaco para o protocolo |

* **`tam_payload` explícito:** o UDP já preserva as fronteiras do datagrama, por isso o campo não protege contra ruído (isso é o checksum do UDP). Serve para validar a estrutura: um datagrama com menos bytes do que declara é truncado e é descartado.
* **`qos` no próprio cabeçalho:** o recetor sabe a garantia de cada entrega sem estado auxiliar (secção 8).
* **Sem campo de integridade próprio:** o checksum UDP cobre a corrupção. Acrescentar outro não traria nada nas condições do trabalho.

## 5. Tópicos e correspondência

* **Representação na rede:** três subcampos de 16 bytes, sem barras, quer em publicações (EventCast) quer em filtros de subscrição (SubControl). Os níveis têm posição fixa e não há delimitadores a procurar.
* **Representação no broker:** `decode_topic` reconstrói a string `regiao/estacao/metrica`. Cada sessão guarda `dict[filtro → qos]` e `matches(filtro, topico)` compara por igualdade ou por `startswith` do prefixo (`north/*` → `north/`).
* **Custo:** uma publicação percorre todos os filtros de todos os subscritores, O(S·F). Aceitável para as dimensões do trabalho (poucos subscritores e filtros).
* **Validação:** um filtro é um tópico completo ou um prefixo de 1 ou 2 níveis terminado em `/*`. Um `*` noutra posição é inválido. O broker rejeita-o fechando a sessão, e o subscritor rejeita-o ao ler a configuração.

## 6. Identificador de mensagem

`message_id = "{pub_id}-{fluxo}-{seq}"`

* O publicador escreve a linha de registo **antes** de enviar o datagrama. Se o processo morrer entre as duas operações, o registo de referência fica com uma mensagem que pode não ter chegado, em vez de faltar-lhe uma que chegou.
* O broker copia `pub_id`, `fluxo` e `seq` sem os alterar. O subscritor reconstrói a string a partir do cabeçalho, pelo que o `message_id` é igual nos dois registos.
* **Unicidade:** depende de os `--id` serem distintos entre publicadores, o que o contrato de execução garante. O `--id` não tem espaços, para não partir o formato do registo.
* **Volta do `seq`:** o contador não dá a volta. `count` está limitado a 2³² e `fluxo` a 65536 fluxos, validados no arranque. A eliminação de duplicados em G2 compara `message_id` por igualdade e não depende da ordem. Se a fase 2 introduzir janela deslizante, as comparações de `seq` entre emissor e recetor têm de ser módulo 2³².

## 7. Cópia única decidida no broker

Com filtros sobrepostos, o broker agrega todos os filtros de um subscritor que correspondem ao tópico e envia **um** datagrama por subscritor.

* A alternativa, decidir no subscritor, enviaria um datagrama por filtro correspondido e descartaria os excedentes depois de atravessarem o anel. Ambas cumprem a regra do enunciado, mas só esta poupa tráfego nas ligações entre routers.
* **Consequência:** o broker passa a tomar a decisão de entrega por subscritor. É o estado que a fase 3 já exige.

## 8. Garantia de cada entrega

`qos_entrega = min(qos_fluxo, max(qos dos filtros correspondidos))`

* O valor segue no campo `qos` do `DELIVER`, e é o que o subscritor consulta: com `qos = 2` consulta a tabela de duplicados, com `qos = 1` regista sempre. É esta a linha de código que distingue G1 de G2.
* **Tabela de duplicados:** conjunto de `(topico, message_id)` que cresce sem limite durante a execução. Aceitável para a duração dos testes. Em execuções longas teria de ser limitada.

## 9. Subscrever e cancelar

* Subscrever um filtro já existente **substitui** o `qos`.
* Cancelar um filtro inexistente é aceite (devolve `ACK`) e não altera nada. A operação é idempotente.
* O subscritor ignora também, localmente, datagramas cujo tópico já não corresponde a nenhum filtro ativo. Cobre os que estavam em trânsito no momento do cancelamento. O broker deixa de os enviar assim que processa o `UNSUBSCRIBE`.

## 10. Datagramas e quadros inválidos

* `Reader.take` verifica `pos + n <= len(dados)`. Faltar um byte levanta `ProtocolError`.
* **Plano UDP (broker e subscritor):** o datagrama inválido, truncado, com `kind` inesperado ou com `qos` fora de {1, 2}, é descartado sem resposta e sem efeito no processo.
* **Plano TCP (broker):** um quadro incompleto, um tipo desconhecido ou um pedido inválido fecham a ligação e libertam a sessão. Os restantes clientes não são afetados.
* O objetivo é que uma mensagem malformada nunca derrube o processo. Não há outras garantias de robustez nem de resistência a abuso.

## 11. Concorrência

* **Broker:** uma thread aceita ligações TCP, uma thread por cliente trata o SubControl, e a thread principal recebe e encaminha os datagramas UDP. O mapa de sessões e os filtros são protegidos por um `Lock`. O encaminhamento trabalha sobre uma cópia (*snapshot*) para não segurar o lock durante o envio.
* **Publicador:** uma thread por fluxo `publish`, cada uma com o seu ritmo. O registo tem um `Lock` porque as threads escrevem no mesmo ficheiro.
* **Subscritor:** um único ciclo com *timeout* de 50 ms, que executa as ações `subscribe`/`unsubscribe` quando chegam ao seu instante e entre elas recebe datagramas. Não há threads nem partilha de estado, mas uma ação bloqueia a receção durante o seu `ACK`.

## 12. Ainda não implementado

* **Fase 2:** `ACK` por UDP (`kind = 3`). O publicador passa a receber datagramas, por isso `udp_port = 0` deixa de servir e vai ser preciso um porto de dados para ele. O mecanismo de retransmissão e os temporizadores vão ser documentados quando existirem.
* **Fase 2:** expulsão por `evict_after`. O subscritor atual não trata a ligação ser fechada pelo broker.
* **Fase 4:** `role` de broker no `HELLO`, mensagens de subscrição entre brokers e um segundo identificador de âmbito por salto, distinto do `message_id` de ponta a ponta.
