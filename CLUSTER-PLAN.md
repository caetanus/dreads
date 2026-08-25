# Cluster — plano (recomeço 2026-08-25, do shard-como-servidor)

Distribuir shards SEM estender o hop SPSC (o erro anterior). A essência:
**um shard É uma instância Redis; ele já fala RESP.** Rotear pra um shard/nó
remoto = ser cliente RESP dele. Modelo = Redis Cluster com PROXY (transparente
pra qualquer cliente, inclusive as skins que fazem RESP interno).

## Modelo

- **Slot → nó.** Cada key tem um slot (keyToSlot, CRC16, [0,16384)). Um mapa
  slot-range → nó diz quem é o dono. O nó que recebe um comando de slot que NÃO
  é seu **proxeia** o comando RESP cru pro nó dono e repassa o reply.
- **Nada de protocolo wire novo** — RESP já é request/reply em ordem por conexão.
  Sem corrid, sem ponteiro viajando, sem ShardMsg-sobre-TCP.
- **O roteamento LOCAL (SPSC/hop) fica intocado** — é aceleração same-process.
  A camada de cluster senta ACIMA: decide nó primeiro; se local, cai no caminho
  local de sempre.
- Skins (Kafka/AMQP/SQS) ganham de graça: guardam via RESP keyspace ops, que
  passam pelo mesmo proxy.

## Integração

Em executeCommand, logo após resolver o slot do comando (shardOwnerOf já chama
commandRouteSlot): se `gClusterOn` e o slot é de um nó remoto → clusterForward
(manda rawCmd, lê 1 reply RESP, append em `o`), return. Senão, caminho local.
Comandos keyless (slot < 0) e conn-local nunca proxiam.

## clusterForward

Conexão RESP pooled POR THREAD (TLS) pra cada nó peer (sem contenção
cross-thread; reconecta em falha). Manda rawCmd; lê 1 reply completo (framing
RESP: +/-/: linha, $ bulk, * array recursivo); append verbatim em `o`. DB: o
proxy trilha o db da conexão pooled e manda SELECT quando muda (as skins usam
dbs != 0).

## Increments

- **I1**: config (cluster-slots "0-8191@self,8192-16383@host:port"), mapa
  slot→nó, clusterForward (conn pooled + framing de reply), hook no
  executeCommand. Teste: 2 nós, slots meio a meio, SET/GET RESP cross-nó.
- **I2**: DB-aware (SELECT no proxy) → skins distribuídas (SQS de 2 nós).
- **I3**: MOVED opcional (cliente cluster-aware pula o proxy), health/reconnect,
  A/B do custo local (gate gClusterOn).
- **I4** (fase 2): replicação — cada slot-range/nó num grupo Raft.

## Fora de escopo (por ora)

Resharding ao vivo, migração de slots, ASK redirects, gossip de topologia
(mapa é estático via config), TLS no link inter-nó.
