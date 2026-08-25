# SQS parity — plano (retomado 2026-08-24, era o item pós-drop-in)

Um skin HTTP/REST (diferente das faces wire) que fala o protocolo do Amazon
SQS, sobre o mesmo core sharded. A aposta estrutural do [[one-ring-vision]]:
**fila SQS = lista no keyspace** (`sqs.q.<name>`), SendMessage = RPUSH,
ReceiveMessage = pop + visibility timeout. Extend-only: nova porta/módulo,
zero mudança no core.

## Protocolo

boto3/aws-cli modernos falam o **JSON protocol** do SQS: `POST /`,
header `X-Amz-Target: AmazonSQS.<Action>`, corpo JSON, resposta
`application/x-amz-json-1.0`. Auth SigV4 é ACEITA mas NÃO validada no v1
(accept-any, como as outras skins na config default — ElasticMQ faz igual).

## Modelo de dados

- **Fila**: lista `sqs.q.<name>` — RPUSH do record `msgid\x1fmd5\x1fbody`.
- **Registro de filas**: set `sqs.queues` (SADD no CreateQueue) — ListQueues,
  sweep, GetQueueUrl.
- **In-flight (visibility)**: hash `sqs.if.<name>`, field = receipt handle,
  value = `deadlineMs\x1frecord`. ReceiveMessage move da lista pro hash com
  deadline = now + VisibilityTimeout; DeleteMessage = HDEL; o sweep re-empurra
  os vencidos de volta pra fila.
- Roteamento: tudo via `amqpDataExec` (hop pro shard dono da key) — funciona
  sob sharding, ao contrário do bridge do dashboard.

## Operações (v1)

CreateQueue, GetQueueUrl, ListQueues, DeleteQueue, GetQueueAttributes,
SendMessage, SendMessageBatch, ReceiveMessage (visibility + MaxNumberOfMessages
+ WaitTimeSeconds long-poll básico), DeleteMessage, DeleteMessageBatch,
PurgeQueue, SetQueueAttributes (no-op tolerante).

## Visibility sweep

Timer por shard (carona na manutenção per-shard existente): para cada fila em
`sqs.queues` cujo dono é ESTE shard, HSCAN `sqs.if.<name>`, re-empurra
(deadline < now) pra `sqs.q.<name>` e HDEL. O(inflight) por tick — ok pro v1.

## Fora de escopo (v2/documentado)

SigV4 real, FIFO queues (dedup/group), DLX/redrive policy, message attributes
tipadas, delay queues, long-poll de verdade (park em vez de retornar vazio),
tags, KMS. Batch limita a 10 (limite AWS).

## Bateria

dub test DMD+LDC, boto3 smoke (create/send/receive/delete/visibility-redelivery),
pika 82/82 + golib 5/5 (nada tocado nas outras faces), `pkill -9 -x dreads`.
