# AMQP 1.0 skin — plano (pré-código)

Gate do usuário: "próxima fila após convergir: amqp 1.0" / "planejar antes de
codar". Este documento é o plano; nenhum código de wire 1.0 foi escrito antes
dele.

## O que AMQP 1.0 é (e não é)

AMQP 1.0 não é uma versão de 0-9-1 — é outro protocolo: sem exchanges/filas no
wire (isso virou "address"), com **links** unidirecionais (sender/receiver)
dentro de **sessions** dentro de **connections**, controle de fluxo por
**crédito de link** (não prefetch), settlement explícito por **disposition**
(accepted/rejected/released/modified), e um sistema de tipos próprio
(described types). Handshake: header próprio `AMQP\x03\x01\x00\x00` (SASL) /
`AMQP\x00\x01\x00\x00` (bare) — **discriminável no mesmo porto 5672** pelos
bytes 4-7 (0-9-1 usa `\x00\x00\x09\x01`). O dispatch de header já existe no
amqp.d (hoje ecoa e fecha); vira o ponto de entrada da skin.

## Princípios (mesmos das outras skins)

- EXTEND-only sobre o core: keyspace de filas idêntico ao 0-9-1
  (`amq.q.<nome>` listas, mesmos hooks gAmqpPush/Pop/PeekHead/Len), mesma
  `amqp-db`. Mensagem 1.0 mapeia PARA o record v4 → **interop total**: publica
  em 1.0, consome em 0-9-1 e vice-versa (como o Rabbit faz conversão).
- Módulo novo `source/dreads/amqp10.d`; amqp.d só ganha o dispatch de header.
- Roteamento reusa routeTo/bindings existentes (o "algoritmo de match
  imbatível" cobre address→exchange também).

## Mapeamentos-chave

| AMQP 1.0 | dreads |
|---|---|
| target/source address `"nome"` | fila `nome` (declara on-attach como 0-9-1 declare) |
| address `/exchanges/X/RK` | routeTo(X, RK) no publish; no consume: fila efêmera bound |
| transfer (sender→broker) | finishPublish-equivalente → RPUSH + confirms via disposition |
| link-credit (flow) | janela do consumer fiber (crédito = limite de entregas em voo) |
| disposition accepted | ack (dropUnacked) |
| disposition released | requeue (settleNegative requeue=true) |
| disposition rejected/modified | dead-letter (settleNegative requeue=false) / requeue+annotations |
| idle-timeout (open) | heartbeat: frames vazios na metade do intervalo (lição do it31!) |
| message sections | header→delivery-mode/priority; properties→correlation-id/reply-to/ttl/content-type; application-properties→headers table; data→body |

## Milestones (1 por iteração de loop, ship→review-fresh igual)

1. **M1 — handshake + tipos**: dispatch de header no 5672, SASL
   PLAIN/ANONYMOUS (mesma ACL gate), open/close com idle-timeout, decoder e
   encoder do sistema de tipos 1.0 (primitivos + described). dub tests dos
   codecs.
2. **M2 — sessions/links + publish**: begin/end, attach/detach de RECEIVER
   (broker recebe = cliente publica), transfer→record v4, disposition de
   settlement pro publisher, flow com incoming-window.
3. **M3 — consume**: attach de SENDER (broker envia), consumer fiber dirigido
   por link-credit, drain flag, dispositions do cliente (accepted/released/
   rejected → ack/requeue/DLX).
4. **M4 — conformance**: harness = **rabbitmq-amqp-java-client** (o cliente
   1.0 oficial novo do time do Rabbit, suite própria) + smoke qpid-proton-python
   e rhea. Laundering mínimo estilo conformance/ + loop de convergência.
5. **M5 — interop**: testes 0-9-1↔1.0 cruzados (publica numa, consome na
   outra), TTL/DLX/maxlen respeitados via meta existente.

## Riscos conhecidos (das lições do 0-9-1)

- Ordering no wire (consume-ok/cancel-ok/close): 1.0 tem attach/detach com as
  MESMAS corridas — aplicar desde o dia 1 os padrões flushSeq/hold-ok.
- TLS-static clobber em yields cross-shard: mesmos padrões stack-copy.
- Heartbeat: mandar na METADE do idle-timeout (não no intervalo cheio).
- Nunca reestruturar o walk do routeTo.

## Fora de escopo v1 (pedir aprovação depois)

Transações 1.0 (txn-capability), dynamic nodes com lifetime policies além do
básico, resume de link (unsettled map), multi-hop/link-routing, TLS.
