# dreads × LavinMQ × Qpid Broker-J — AMQP 0-9-1 (2026-08-24, fastbox 3950X)

Cliente: RabbitMQ PerfTest (docker, host-net), transient, autoack, 30s/run.
RAM = VmRSS do processo do broker (mesma métrica p/ todos). Sequencial (um
broker por vez). LavinMQ cloudamqp/lavinmq:latest; Qpid apache/qpid-broker-j
:latest (heap default E -Xmx10g — mesmo resultado).

## Throughput (msg/s)

| Cenário                     | dreads s1 | dreads s4      | LavinMQ  | Qpid Broker-J |
|-----------------------------|-----------|----------------|----------|---------------|
| 1P/1C 16B                   | 140K      | 130K           | 141K     | 136–141K      |
| 4P/4C 16B, 4 filas          | 364K      | 309K/144K¹     | 460K     | 45K/25K → **OOM** |
| 4P/4C 1KB, 4 filas          | 255K      | 274K/209K¹     | 268K     | 69K/58K²      |
| 32 conexões (4× PerfTest)   | 357K      | **585K/338K**¹ | 482K     | —             |
| backlog 1M×1KB (só produce) | 233K      | 189K           | 362K     | 2.6K → morreu |

¹ send/recv divergem no s4: consumo cross-shard é o gargalo conhecido da
campanha de perf (pendente). O send 585K@s4 reproduz o pico histórico da
saturação (588K@s8) — SEM regressão de hotpath pós-conformidade.
² isolado em broker fresco; morre de OOM em qualquer cenário sustentado.

1P/1C ≈141K nos TRÊS brokers = teto do cliente PerfTest por conexão, não do
broker.

## RAM (VmRSS)

| Estado             | dreads s1 | LavinMQ | Qpid Broker-J |
|--------------------|-----------|---------|---------------|
| idle               | 23 MB     | 33 MB   | 240–490 MB    |
| sob carga (s2/s3)  | 49 MB     | 69 MB   | 500–960 MB    |
| backlog 1M×1KB     | 1.12 GB   | 55 MB   | OOM (morto)   |

Arquiteturas distintas no backlog: dreads guarda mensagens NA RAM (modelo
Redis — 1 GB de payload + ~12% overhead, mais eficiente que RabbitMQ faria);
LavinMQ escreve tudo em disco por design (55 MB de RSS é o selling point
deles). Qpid Broker-J não sobrevive a nenhum cenário de acúmulo com a imagem
estoque.

## Sanidade de hotpath (mesma data)

RESP no binário corrente: SET 1.20M rps, GET 1.09M (redis-benchmark -P16
-c32 --threads 4) — bate o baseline "master ~1.0M single" da memória de
bench. As campanhas de conformidade (RabbitMQ laundry, consumer groups,
transações, streams 1.0) NÃO custaram o hotpath.
