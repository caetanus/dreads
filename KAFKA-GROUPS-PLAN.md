# Kafka consumer groups — plano (aprovado pelo usuário 2026-08-24: "vai")

Substituir o stub de membro único por um coordenador de grupo REAL (protocolo
clássico, KIP-848 fora de escopo), destravando os 37 testes librdkafka gated.
Transações/idempotência continuam gated à parte.

## Arquitetura

**Estado no shard dono.** O grupo vive em TLS no shard
`shardOfSlot(keyToSlot("kafka.cg.<group>"))` — a MESMA slot do hash de offsets,
então estado de membership e offsets do grupo são co-donos. Nenhum broadcast,
nenhum lock: toda transição de FSM executa no drain do dono (que não yielda),
logo é atômica por construção.

**Transporte: novo `ShardMsg.kafkaGroup`.** Payload
`[u64 pend][req binário]`, meta = shard de origem. O drain do dono chama
`kgroupApply(req, reply)` (módulo novo `dreads.kafkagroup`) e devolve via
`ShardMsg.reply` com tag = pending (caminho já existente). Mesmo shard = chamada
direta. O requisitante usa `acquireShardPending` + wait, padrão amqpDataExec.

**Modelo de espera: POLL, não park no dono.** JoinGroup/SyncGroup precisam
"segurar" a resposta até a barreira fechar. A fiber do cliente (no shard da
conexão) fica num loop: manda a op → recebe `WAIT` → dorme ~15ms → repete, com
prazo limitado pelo rebalance timeout (cap 70s). O dono nunca parkeia; cada op
é uma transição atômica que responde o estado corrente. Per-connection
head-of-line blocking durante join é comportamento fiel ao broker real
(processamento em ordem por conexão).

## FSM (protocolo clássico)

Estados: `Empty → PreparingRebalance → CompletingRebalance → Stable`.

- **Join**: registra/atualiza membro (protocolos, timeouts, metadata),
  marca joinedRound. Estado ≠ Preparing → vira Preparing com deadline
  `now + max(rebalanceTimeout dos conhecidos)` (cap 60s; initial delay = 0,
  como brokers de teste). Barreira fecha quando TODOS os membros conhecidos
  re-entraram OU no deadline (não-reentrantes são descartados). Fechou:
  generation++, líder = líder anterior se re-entrou senão primeiro, protocolo =
  primeiro da preferência do líder suportado por todos (senão
  INCONSISTENT_GROUP_PROTOCOL 23), estado = Completing. Resposta de join do
  líder carrega a lista completa de membros+metadata; seguidores lista vazia.
- **member id vazio (v4+)**: responde MEMBER_ID_REQUIRED(79) + id gerado; o
  cliente re-join com o id (dança padrão). v<4: gera e segue direto.
- **Sync**: valida generation(22)/membro(25). Preparing → 27. Líder entrega
  assignments (member→bytes) → estado Stable; todos recebem seu assignment
  (seguidores em WAIT até Stable).
- **Heartbeat**: valida; Preparing/Completing → REBALANCE_IN_PROGRESS(27);
  Stable → NONE. Atualiza lastHb (MonoTime).
- **Leave**: remove membro(s); restam membros → Preparing; senão Empty.
- **Sweep** (carona no amqpTtlTick 50ms, por shard): evicção por
  sessionTimeout vencido → rebalance; fecha barreiras no deadline.
- **OffsetCommit fencing**: SÓ valida quando a request traz generation >= 0 e
  o grupo existe na FSM (gen errada → 22, membro desconhecido → 25).
  Consumidores simples (assign(), generation -1) seguem no caminho atual
  intocado — golib/franz-go não regressam.

## Correções de wire junto (bugs do stub achados na leitura)

- JoinGroup clássico v5: falta `group_instance_id` no parse da request E no
  member da response (v5+). Provável causa-raiz do loop JoinGroup→erro do
  librdkafka.
- DescribeGroups: refletir estado/membros/assignments reais.
- LeaveGroup clássico v3: members em batch (auditar parser).

## Milestones

- **M1**: módulo kafkagroup.d + ShardMsg.kafkaGroup + FSM
  join/sync/heartbeat/leave + fixes de wire. Smoke: 2 consumidores librdkafka
  concorrentes dividem 4 partições e rebalanceiam.
- **M2**: sweep de evicção + fencing no commit + DescribeGroups real.
- **M3**: static membership (group.instance.id) + fluxos cooperative-sticky
  (mesma mecânica de broker; rounds extras de rebalance).
- **M4**: loop de convergência dos ~37 testes gated (refute-first; os que
  exigirem transações ficam para o gate de txn).

Fora de escopo: KIP-848, transações/idempotência, ACLs de grupo,
group.initial.rebalance.delay configurável.

Bateria por iteração: dub test DMD+LDC, golib 5/5, librdkafka spot
(0029/0030/0033 + core 0001/0002), pika 82/82 (amqp intocado ⇒ 1× no fim),
`pkill -9 -x dreads`.
