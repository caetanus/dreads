# Kafka idempotência + transações — plano (gate aprovado 2026-08-24: "a")

Destravar 0090/0094 (idempotência) e 0103/0129 (transações + read_committed).
Extend-only: produtores sem producer id (pid = -1 no batch v2) não pagam NADA
— todo caminho novo é gated no pid do header do RecordBatch e num epoch
atômico global para o lado do Fetch.

## T1 — produtor idempotente (0090, 0094)

O header fixo do RecordBatch v2 carrega producerId(i64)@43, producerEpoch
(i16)@51, baseSequence(i32)@53, attributes(i16)@21. Lidos direto do slice no
handleProduce (decodeV2Batch intocado).

Estado por partição no keyspace: hash `kafka.pid.<topic>.<p>`, field =
`<pid>`, value = `<epoch>:<lastBaseSeq>:<lastCount>:<lastBaseOffset>`.
Check→push→update roda na fiber da conexão (um pid nunca produz concorrente
na mesma partição — o cliente serializa; fencing entre pids usa epoch).

Regras (Kafka):
- epoch do batch < epoch gravado → INVALID_PRODUCER_EPOCH(47).
- epoch maior → aceita e reseta a sequência.
- mesmo epoch: esperado = lastBaseSeq + lastCount (wrap i32);
  baseSeq == esperado → aceita; batch INTEIRO já gravado (retry do último
  batch) → responde NONE com o baseOffset CACHEADO (dedup!); senão →
  OUT_OF_ORDER_SEQUENCE_NUMBER(45).
- primeiro batch do pid: aceita e registra.

## T2 — coordenador de transações (0103)

Reusa o transporte ShardMsg.kafkaGroup: ops novas no kgroupApply (TXN_INIT/
TXN_ADD/TXN_END), estado TLS `tTxns[tid]` no shard dono da key
`kafka.txn.<transactional_id>` = {pid estável, epoch++ por re-init (fencing),
partições tocadas}.
- InitProducerId com tid: mesmo pid, epoch+1 (re-init fenceia o zumbi).
- AddPartitionsToTxn: valida epoch (47), registra partições.
- Produce transacional (attrs bit 0x10): no primeiro batch do (pid,epoch)
  desde o último marker, grava firstOffset em field `txn:<pid>` do hash
  kafka.pid da partição.
- EndTxn: para cada partição registrada, apende um CONTROL MARKER no log
  (record interno tag 0xFE: [pid i64][type i16] — 0 abort, 1 commit); abort
  também apende o range em `kafka.txa.<t>.<p>` ("pid firstOffset"); limpa o
  field txn:<pid>. Marcadores ocupam offsets, como no Kafka.

## T3 — read_committed no Fetch (0129, 0103)

Gate global `gKafkaTxnSeen` (atômico; 0 = nunca houve produce transacional →
custo zero no fetch). Quando ativo, Fetch v4+:
- last_stable_offset = min(firstOffset das txns abertas na partição) ou hw;
- aborted_transactions = ranges de `kafka.txa.<t>.<p>`;
- o empacotador de batches SPLITA nos control records: batch normal
  acumulado, depois um batch de controle próprio (attrs 0x30, count 1, key =
  [version=0 i16][type i16]) — o cliente pula sozinho.
O caminho v0-v3 (sem read_committed) emite o marker como registro comum
(fora do escopo dos testes).

TxnOffsetCommit continua persistindo direto (visível imediatamente); se 0103
exigir visibilidade só-no-commit, bufferizar no coordenador (revisar).

## T4 — convergência

0090 → 0094 → 0129 → 0103 no loop refute-first; bateria completa por
iteração (dub×2, golib 5/5 — o TransactionProcessor do golib exercita o
caminho txn!, spot librdkafka, pika 1× no fim).

Fora de escopo: KIP-848, produtor idempotente sem v2-batches, retenção de
markers, compactação de kafka.txa.
