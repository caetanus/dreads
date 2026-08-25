# Production drop-in — plano (aprovado 2026-08-24: "b")

Fechar o que separa as skins de um drop-in de produção. O wire já está
provado pelas suítes dos vendors (RMQ Erlang 344/344, java 325/330,
librdkafka 160/166, Paho 27/27); a distância real é transversal:
TLS → SASL/enforcement Kafka → management API RMQ. Extend-only: cada
listener plaintext continua idêntico; TLS é porta NOVA, `c.tls is null`
= caminho atual byte a byte.

## M1 — TLS engine + RESP (`tls-port`) — **LANDADO `04d3683`** (+0,15% instr plaintext)

**Motor: OpenSSL via BIO de memória, bindings extern(C) à mão**
(source/dreads/tls.d — estilo uringraw: sem dep dub, linka libssl.so.3).
O socket segue 100% do vibe; o TLS é uma tradução de bytes por conexão:

    socket → cipher bytes → BIO rbio → SSL_read → plaintext no inb (parser intacto)
    outb plaintext → SSL_write → BIO wbio → cipher bytes → socket

- `TlsCtx` por listener (cert/key/CA carregados no boot, main thread;
  `SSL_CTX` é thread-safe para `SSL_new`). Min proto TLS 1.2.
- `TlsConn` por conexão (criado no shard dono, vive e morre lá — regra
  do alocador). API: `feed(cipher)`, `readPlain(ref ByteBuffer)`,
  `writePlain(bytes)`, `drainCipher(ref ByteBuffer)`, `shutdown()`.
  Handshake dirigido pelos mesmos read/write (WANT_READ/WANT_WRITE).
- Config (nomes Redis): `tls-port`, `tls-cert-file`, `tls-key-file`,
  `tls-ca-cert-file`, `tls-auth-clients yes|no|optional` (mTLS).
- Integração RESP: listener novo em tls-port; serveClient ganha os dois
  pontos de tradução (read site + flush site), gated em `c.tls`.
- Validação: unit (handshake loopback in-memory, cert efêmero gerado com
  `openssl req` no scratch), blackbox `redis-cli --tls` + `openssl
  s_client`, bench sanity (tls-port não toca o hot path plaintext —
  instr/op idêntico no arbiter).

## M2 — TLS nas skins — **LANDADO `097b0fe`** (pika/paho/kafka-python sobre TLS; advertised-port fix)

- `--mqtt-tls-port` (8883), `--amqp-tls-port` (5671, cobre 0-9-1 e 1.0 —
  mesmo listener/detecção de header), `--kafka-tls-port` (SSL://).
  Mesmo TlsConn nos read/flush sites de cada skin (mqtt.d, amqp.d,
  kafka.d — cada uma tem 1 ponto de read e 1-2 de flush).
- Dashboard HTTPS: adiado (M4 decide).
- Validação: mosquitto_{pub,sub} --cafile, pika ssl.SSLContext,
  librdkafka build com SSL → **destrava o estrutural 0064**; kcat -X
  security.protocol=ssl; suites completas re-rodadas na porta TLS.

## M3 — Kafka SASL + ACL enforcement — **LANDADO** (M3a PLAIN `f1985e7`, M3b SCRAM `8d344ed`, M3c enforcement `1cbb739`)

- SaslHandshake (17) + SaslAuthenticate (36), mecanismos PLAIN e
  SCRAM-SHA-256/512, validando contra os MESMOS usuários ACL do RESP
  (one-ring auth, como MQTT/AMQP já fazem). PLAIN exige tls ou flag
  explícita `kafka-plain-without-tls yes` (não vazar senha em claro por
  default). SCRAM: armazenar verifier (salt, StoredKey, ServerKey) no
  registro ACL — extend (campo novo), não mudar o formato existente.
- **Enforcement** das ACLs Kafka já armazenadas (kafka.acls): authorizer
  no dispatch das APIs (produce → WRITE topic, fetch → READ, group →
  READ group, admin → ALTER/DESCRIBE/CREATE/DELETE), principal = usuário
  SASL (ou ANONYMOUS). Gated: sem ACLs definidas = allow-all (custo zero,
  comportamento atual).
- Validação: librdkafka 0109/0115/0119 (com kafka-acls.sh disponível),
  kcat sasl, kafka-python SASL_PLAIN; suítes completas (nada regride sem
  SASL configurado).

## M4 — RabbitMQ management HTTP API — **LANDADO** (v1 `7799bb7` read plane + queues; v2 `0644282` connection registry list + close)

- Porta 15672, servidor HTTP próprio (fundação: dashboard.d já serve
  HTTP). Superfície v1 (o que UI/rabbitmqadmin/operators mais usam):
  GET /api/overview, /api/queues[/vhost[/name]], /api/exchanges,
  /api/bindings, /api/connections, /api/channels, /api/vhosts,
  /api/users, /api/nodes; PUT/DELETE queues+exchanges+bindings;
  DELETE /api/connections/<name> (close). Auth = basic auth contra ACL.
- Isso destrava os 5 erros restantes do suite java (rabbitmqctl
  close_all_connections → shim script apontado em rabbitmqctl.bin que
  chama a API).
- Validação: rabbitmqadmin list/declare/delete, java suite com o shim,
  UI oficial apontada no endpoint (se render, bônus).

## Fora de escopo (registrado, não esquecido)

Cluster-aparente por protocolo (réplicas Kafka visíveis, quorum queues),
KIP-848, MQTT WebSockets/$SYS (candidatos a M5), retention/compaction
background Kafka, federation/shovel/policies RMQ.

Bateria por milestone: dub test DMD+LDC, pika 82/82, golib 5/5, suíte do
protocolo tocado, arbiter instr/op no plaintext, `pkill -9 -x dreads`.
