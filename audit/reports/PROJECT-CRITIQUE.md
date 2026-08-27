# Relatório de crítica e plano de correção do dreads

**Data da revisão:** 2026-08-26  
**Escopo:** arquitetura, confiabilidade, segurança, testes, manutenção, documentação e posicionamento do produto  
**Estado analisado:** `master` em `ec8d05a`, incluindo a inspeção das alterações locais não commitadas  

## Resumo executivo

O dreads é um projeto de engenharia de sistemas tecnicamente forte e original. O núcleo apresenta decisões coerentes com a meta de alto desempenho: data plane `@nogc`, arenas, estruturas de dados próprias, event loop, sharding thread-per-core, message passing e replicação por Raft.

O principal risco atual não é falta de capacidade técnica, mas excesso de superfície. O mesmo processo tenta oferecer compatibilidade com Redis/Valkey, AMQP 0-9-1, AMQP 1.0, MQTT, Kafka e SQS, além de sharding, persistência, Raft e dashboard. Cada protocolo acrescenta invariantes próprias de durabilidade, autenticação, ordenação, sessão, confirmação e recuperação. A implementação e a verificação não cresceram no mesmo ritmo da superfície funcional.

Com a evidência adicional fornecida pelo autor — execução completa das suítes reais de Kafka, RabbitMQ e Mosquitto contra o dreads, além de medições de 2× a 168× em CPU/memória — a classificação precisa ser mais favorável: as skins têm evidência de **compatibilidade funcional avançada**, não devem ser descartadas simplesmente como protótipos experimentais. O que ainda falta demonstrar para produção AWS é diferente: comportamento sob falhas, escala multi-AZ, segurança, recuperação, isolamento entre skins e reprodução independente dos ganhos.

Em outras palavras: a revisão anterior foi dura demais ao transformar “não validado neste ambiente” em “não existe”. Este relatório registra os riscos que precisam de comprovação e não invalida testes reais já executados pelo autor.

## Objetivo comercial: aquisição pela AWS e economia de US$ 800 milhões/ano

O objetivo declarado é demonstrar uma economia anual próxima de **US$ 800 milhões para a AWS**, criando uma tese de aquisição. Isso muda a prioridade: o valor não está em oferecer o maior número de protocolos, mas em provar redução de custo numa frota específica sem regressão de disponibilidade, durabilidade, segurança ou compatibilidade.

### Matemática mínima da tese

US$ 800 milhões por ano não podem ser apresentados como extrapolação simples de requests por segundo. A base de custo anual afetada teria de ser aproximadamente:

| Redução líquida comprovada | Base de custo anual necessária |
|---:|---:|
| 10% | US$ 8,0 bilhões |
| 20% | US$ 4,0 bilhões |
| 30% | US$ 2,67 bilhões |
| 40% | US$ 2,0 bilhões |
| 50% | US$ 1,6 bilhão |

A base correta precisa vir de dados da AWS ou ser rotulada como hipótese. Receita, preço cobrado ao cliente e gasto total não equivalem ao custo de infraestrutura eliminável. A conta relevante é:

```text
compute evitado
+ memória evitada
+ rede/replicação evitada
+ armazenamento e I/O evitados
- operação adicional
- capacidade ociosa e headroom exigidos
- migração, integração e suporte
- risco esperado de incidentes
= economia líquida
```

### Por que o benchmark atual ainda não prova a economia

- Usa workstation Ryzen x86, enquanto o alvo opera em famílias Graviton.
- Mede principalmente throughput máximo, não custo por operação útil.
- Parte dos resultados usa AOF desligado.
- Não incorpora TLS, Multi-AZ, replicas, failover e backups.
- Não mede memória efetiva por dataset e fragmentação durante churn.
- Não inclui headroom para picos e recuperação.
- Não reproduz a distribuição real de comandos, payloads, TTLs e conexões.
- Não mede custo operacional nem taxa de incidentes.

A AWS já divulga até 31% melhor price-performance em ElastiCache ao passar de Graviton3 para Graviton4 e ganhos de até 60% do Valkey recente sobre Redis OSS em determinados cenários. A baseline correta é, portanto, **Valkey atual e otimizado em Graviton4**, não Redis antigo ou Valkey genérico em x86.

### Tese recomendada

> O dreads é um engine Redis/Valkey-compatible desenhado para aumentar operações úteis por vCPU e por GiB em Graviton, preservando as garantias de disponibilidade e durabilidade exigidas pelo ElastiCache. Em escala suficiente, essa eficiência pode reduzir materialmente o COGS da AWS.

O esclarecimento importante é que as “skins” não precisam ser tratadas como produtos independentes: podem ser adaptadores de protocolo para um único engine, uma única VM e uma única camada de storage. Isso fortalece a tese de **consolidação de densidade**, mas não significa automaticamente substituir quatro serviços gerenciados por uma VM.

AWS opera serviços com responsabilidades diferentes: Amazon MSK é Kafka gerenciado com provisionamento, scaling, segurança e failover; AWS IoT Core fornece gateway MQTT, autenticação de dispositivos e regras; Amazon MQ oferece brokers gerenciados; SQS é uma fila gerenciada com semântica e integração próprias. [MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-cluster-management.html), [IoT MQTT](https://docs.aws.amazon.com/iot/latest/developerguide/mqtt.html), [Amazon MQ](https://docs.aws.amazon.com/amazon-mq/latest/migration-guide/amazon-mq-mg.pdf).

Portanto, a proposta comercial correta é:

> Uma VM/engine consolidada pode executar cargas compatíveis de cache, filas, streaming e MQTT com maior utilização média de CPU/memória, reduzindo COGS em workloads elegíveis, enquanto workloads que exigem isolamento, escala independente ou integrações específicas continuam em serviços separados.

Isso é mais crível que alegar que uma VM substitui integralmente quatro produtos em todos os cenários. O benefício precisa ser medido contra a composição real de workloads, incluindo o custo de gestão que hoje a AWS embute no serviço.

AMQP, MQTT, Kafka e SQS ajudam a tese somente se houver comprador interno e linha de custo específicos. No estágio atual, aumentam superfície de ataque e due diligence. Devem ser adaptadores/plugins separados do core, com isolamento de falhas, limites de recursos e possibilidade de desligar cada skin.

### Kafka versus SQS: onde a economia é plausível

Kafka/MSK é um alvo mais direto: há cobrança explícita por brokers, armazenamento e throughput, e clusters de produção mantêm múltiplos brokers para disponibilidade. A hipótese é aumentar throughput por broker ou reduzir capacidade necessária, mantendo durabilidade, replicação, isolamento e recuperação. [MSK pricing](https://aws.amazon.com/msk/pricing/)

SQS não deve ser modelado como “uma VM por fila”. A cobrança pública é principalmente por request e tamanho de payload, sem taxa mínima; a infraestrutura é compartilhada e escala automaticamente. A hipótese correta é reduzir o custo interno por request/GB com batching, compactação, menor I/O, melhor uso de memória e multiplexação de tenants. SQS também replica mensagens antes de confirmar, então a comparação precisa incluir esse custo de durabilidade. [SQS pricing](https://aws.amazon.com/sqs/pricing/), [SQS durability and scaling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)

| Alvo | Hipótese | Métrica de prova |
|---|---|---|
| Kafka/MSK | Mais throughput por broker ou menos brokers | custo por GB ingerido, partição, retenção, réplica e p99 sob failover |
| SQS | Menor COGS por request/GB | custo por milhão de requests, payload, retenção, redelivery e burst |

Não misture essas métricas em um único “requests por segundo”. Kafka é principalmente broker/storage/retention; SQS é request economics, durabilidade distribuída e escala de cauda.

### Risco da VM consolidada

Uma VM para quatro skins reduz capacidade ociosa, mas cria domínio comum de falha e interferência. O design precisa ter quotas de CPU/memória, filas e backpressure independentes, fairness, limites de conexões/partições/sessões, restart ou fencing por skin, replicação em VMs/AZs distintas e métricas por skin. Sem isso, a consolidação pode reduzir VMs e aumentar incidentes, headroom e custo de suporte.

### Produto mínimo para apresentar à AWS

- RESP2/RESP3 e compatibilidade Valkey documentada.
- Cluster mode e sharding.
- TLS e ACL completos.
- Replicação e failover comprovados.
- Persistência/durabilidade na modalidade escolhida.
- Telemetria operacional.
- Build e otimizações ARM64/Graviton.
- Harness reproduzível de benchmark e custo.

Dashboard e demais protocolos podem permanecer no repositório, mas não devem estar no trusted computing base do candidato a ElastiCache.

### Pacote de avaliação reproduzível

Criar:

```text
aws-evaluation/
  README.md
  methodology.md
  workloads/
  terraform/
  scripts/
  dashboards/
  raw-results/
  cost-model/
  failure-tests/
  compatibility/
```

Executar dreads e Valkey 9 lado a lado nas mesmas instâncias Graviton4, AZs, clientes, afinidade, TLS, dataset e política de durabilidade.

#### Métricas obrigatórias

- operações úteis por vCPU-hora e por dólar;
- GiB de dataset lógico por GiB de RSS;
- rede, replicação, bytes escritos e fsyncs por operação;
- p50, p95, p99 e p99.9 sob utilização crescente;
- tempo e perda observada durante failover;
- recuperação/replay e impacto no tráfego;
- throughput durante resharding, snapshot e replica catch-up;
- erros, timeouts e disconnects por bilhão de operações.

#### Cargas obrigatórias

- cache read-heavy com TTL churn;
- sessão read/write, counters e rate limiting;
- lists, sorted sets e streams;
- payloads pequenos, médios e grandes;
- hot keys e distribuição Zipf;
- steady state, bursts e overload controlado.

#### Porta mínima para avançar ao pitch

- Pelo menos 30% menos custo na média ponderada dos workloads-alvo.
- Nenhuma regressão material de p99.9.
- Zero perda de operações confirmadas em crash tests.
- Compatibilidade suficiente para workloads escolhidos migrarem sem alteração.
- Failover e recuperação dentro de objetivos definidos antes do teste.
- Resultado reproduzido por terceiro independente ou pela AWS.

### Modelo financeiro

Criar cenários conservador, base e máximo, separando:

- economia para AWS versus redução de preço ao cliente;
- node-based versus serverless;
- ElastiCache versus MemoryDB;
- regiões e famílias de instância;
- workloads elegíveis e incompatíveis;
- economia bruta e líquida;
- rampa de adoção de três a cinco anos.

O número de US$ 800 milhões deve permanecer como **potencial a validar** até base de custo, participação elegível e ganho líquido serem demonstrados.

### Materiais de diligência

- benchmark reproduzível em Graviton4;
- auditoria independente de segurança;
- matriz de compatibilidade automatizada;
- SBOM, licenças e relatório de proveniência/IP;
- threat model e processo de resposta a vulnerabilidades;
- roadmap de integração ao control plane AWS;
- análise build-versus-buy;
- data room com resultados brutos, custos e falhas conhecidas.

### Marcos comerciais

1. **Engine proof:** vantagem em Graviton4 sem durabilidade.
2. **Service proof:** vantagem permanece com TLS, replicas e failover.
3. **Compatibility proof:** workloads reais migram sem modificação.
4. **Cost proof:** economia líquida comprovada por cluster.
5. **Fleet proof:** AWS valida a frota e o custo elegíveis.
6. **Acquisition thesis:** somente então calcular economia anual e valor estratégico.

US$ 800 milhões deve ser o resultado do marco 5, não a premissa usada para selecionar resultados.

### Avaliação resumida

| Dimensão | Nota | Observação |
|---|---:|---|
| Ambição e originalidade | 9/10 | Arquitetura incomum e proposta diferenciada |
| Qualidade do núcleo | 8/10 | Boas decisões de baixo nível e preocupação real com desempenho |
| Clareza do produto | 6/10 | A tese de uma substrate multi-protocolo é coerente, mas precisa ser apresentada com foco |
| Manutenibilidade | 4/10 | Módulos grandes e invariantes concorrentes espalhadas |
| Segurança e robustez | 5/10 | Há cobertura funcional forte; faltam provas sistemáticas nos cenários de falha e isolamento |
| Prontidão para produção | 5–6/10 | Compatibilidade parece avançada; validação operacional e AWS ainda precisam ser empacotadas |

## Diagnóstico principal

O projeto sofre de **feature velocity maior que assurance velocity**: funcionalidades e protocolos são adicionados mais rapidamente do que as garantias de correção, segurança, durabilidade e interoperabilidade conseguem ser estabelecidas.

Isso produz quatro efeitos:

1. Um badge verde transmite mais confiança do que a CI realmente demonstra.
2. Falhas aparecem nas fronteiras entre subsistemas: shard, fiber, TLS buffer, AOF, Raft e confirmação ao cliente.
3. O custo de revisão cresce rapidamente porque parsing, estado, autorização e persistência convivem em módulos muito grandes.
4. A documentação precisa explicar tantas exceções que a promessa simples de “drop-in” deixa de ser precisa.

## P0 — correções bloqueadoras

Os itens desta seção devem bloquear releases descritos como estáveis ou adequados para produção.

### P0.1 — Encerrar os achados críticos e altos das revisões técnicas

Os relatórios `CODEX-REVIEW.md` e `GLM-CTF-REPORT.md` registram problemas de segurança, concorrência, durabilidade e conformidade. Alguns já parecem estar em correção no working tree, mas não existe ainda uma matriz única que mostre o estado e a reprodução de cada achado.

#### Ações

- Criar uma tabela de triagem com um identificador estável para cada achado.
- Classificar cada item como `confirmed`, `not reproducible`, `fixed` ou `accepted risk`.
- Para todo item confirmado, adicionar um teste que falhe antes da correção.
- Registrar o commit da correção e o teste correspondente.
- Não considerar um item concluído somente porque o código foi alterado.

#### Critério de conclusão

- Zero achados `CRITICAL` ou `HIGH` sem resolução documentada.
- Cada correção possui teste de regressão automatizado.
- Os testes são executados na CI relevante, não apenas localmente.

### P0.2 — Corrigir a contradição de licença

Há três declarações incompatíveis:

- `README.md` declara MIT.
- `dub.json` declara BUSL-1.1.
- `LICENSE` contém Business Source License 1.1.

#### Ações

- Escolher a licença efetiva do projeto.
- Atualizar `README.md`, `dub.json`, `LICENSE`, imagens e metadados de release.
- Se a escolha for BSL, documentar claramente a Change Date, Change License e os limites do Additional Use Grant.
- Verificar se todos os arquivos de terceiros estão cobertos por `THIRD_PARTY_NOTICES.md`.

#### Critério de conclusão

- Todos os pontos de distribuição apresentam a mesma licença.
- Um usuário consegue entender, sem interpretação jurídica do repositório inteiro, se seu uso é permitido.

### P0.3 — Reduzir as promessas públicas ao nível comprovado

O README usa expressões como “fast, reliable”, “drop-in” e “talk to it unchanged”. Ao mesmo tempo, `DRIFT.md` e os guias dos protocolos documentam diferenças relevantes.

#### Ações

- Trocar “drop-in Redis replacement” por “Redis-compatible para a superfície documentada”.
- Marcar explicitamente cada face como `stable`, `beta` ou `experimental`.
- Evitar “reliable” até existirem testes automatizados de crash consistency e confirmação durável.
- Explicar logo no início do README que Kafka, AMQP, MQTT e SQS são faces opcionais com compatibilidade parcial.
- Manter benchmarks, mas separar claramente desempenho de correção e durabilidade.

#### Critério de conclusão

- Nenhuma afirmação do README depende de um teste manual ou não executado regularmente.
- Cada alegação de compatibilidade aponta para uma matriz automatizada e atualizada.

### P0.4 — Colocar os caminhos críticos na CI

A CI principal executa `dub test`, um release build e smoke tests básicos de RESP. Ela não executa as suítes live e de conformidade que sustentam várias afirmações do README.

#### Matriz mínima recomendada

| Job | Frequência | Escopo mínimo |
|---|---|---|
| Unit | Todo PR | Dashboard ligado e desligado |
| Release build | Todo PR | Linux; macOS/Windows em workflows próprios |
| ASan | Todo PR ou nightly | Parser, RESP, protocolos e caminhos cross-shard |
| RESP blackbox | Todo PR ou nightly | Valkey live, com lista de skips versionada |
| Sharding | Todo PR | `--shards 1`, `4` e `8` em casos essenciais |
| Raft | Nightly | 3 nós, eleição, failover, snapshot e recuperação |
| Crash consistency | Nightly | `kill -9` em pontos de apply/log/fsync/ack |
| Protocol conformance | Nightly | AMQP 0-9-1, AMQP 1.0, MQTT, Kafka e SQS |
| Fuzz | Nightly | Codecs, tamanhos, integers, frames e state machines |

#### Critério de conclusão

- O badge “full suite” somente é usado se essa matriz for executada.
- Falha em teste de durabilidade, segurança ou conformidade bloqueia release.
- Resultados e skips ficam disponíveis como artefatos da CI.

## P1 — confiabilidade e arquitetura

### P1.1 — Formalizar o ciclo de uma operação durável

Cada protocolo deve mapear explicitamente seu ack para um estágio interno:

```text
received -> authenticated -> validated -> routed -> applied -> logged -> flushed -> acknowledged
```

Para cada tipo de confirmação, documentar:

- se a operação foi apenas aceita, aplicada ou persistida;
- qual shard é responsável pela persistência;
- o que acontece se o processo morrer em cada transição;
- como retries e deduplicação funcionam;
- se a resposta pode preceder o apply remoto.

#### Critério de conclusão

- AMQP confirms, MQTT QoS, Kafka acks e SQS success responses têm testes com crash injection.
- Nenhuma confirmação descrita como durável é enviada antes do ponto de durabilidade declarado.

### P1.2 — Formalizar ownership e lifetime de memória

O projeto combina memória manual, buffers TLS, slices, fibers, filas cross-thread e estruturas fora do GC. Nessa arquitetura, qualquer slice que sobreviva a um yield ou hop precisa ter ownership inequívoco.

#### Ações

- Documentar regras de ownership no diretório `source/dreads`.
- Proibir slices TLS atravessando pontos que podem ceder execução, salvo cópia explícita.
- Encapsular codificação cross-shard em leitores/escritores que usem cópia por bytes e respeitem alinhamento.
- Evitar casts de offsets arbitrários para `uint*` e `ulong*`.
- Criar assertions de desenvolvimento para origem, comprimento e geração de buffers.
- Exercitar esses caminhos com ASan e fuzzing.

#### Critério de conclusão

- Todo payload assíncrono possui proprietário documentado.
- Não há dereference tipado de endereço potencialmente desalinhado.
- Revisões de código conseguem identificar mecanicamente onde uma operação pode yieldar.

### P1.3 — Dividir módulos monolíticos

Tamanho aproximado dos maiores módulos:

| Arquivo | Linhas |
|---|---:|
| `server.d` | 11.023 |
| `kafka.d` | 7.198 |
| `commands.d` | 7.164 |
| `amqp.d` | 6.637 |
| `amqp10.d` | 4.269 |
| `mqtt.d` | 4.199 |

O problema não é apenas o número de linhas, mas a mistura de parsing, state machine, autorização, routing, persistência e resposta.

#### Separação sugerida por protocolo

```text
protocol/
  codec.d          framing e parsing, sem estado global
  connection.d     state machine da conexão
  auth.d           identidade e autorização
  operations.d     modelo semântico das operações
  storage.d        tradução para o núcleo/keyspace
  recovery.d       estado persistente e restauração
  conformance.d    helpers e invariantes verificáveis
```

#### Critério de conclusão

- O parser não acessa diretamente o keyspace.
- O storage adapter não monta frames de rede.
- A autorização ocorre antes de qualquer mutação ou enumeração sensível.
- Cada state machine pode ser testada sem socket real.

### P1.4 — Definir limites de recursos para entrada não confiável

Todo protocolo exposto à rede deve limitar antes da autenticação:

- tamanho de frame e request;
- bytes acumulados por conexão;
- quantidade de links, subscriptions e grupos;
- profundidade de valores recursivos;
- faixas iteradas recebidas do cliente;
- número de conexões e memória agregada;
- tempo máximo de parsing ou operação.

#### Critério de conclusão

- Nenhum campo de tamanho usa soma suscetível a overflow.
- Todo loop controlado por intervalo do cliente possui limite de trabalho.
- Os limites são configuráveis, documentados e testados nos valores de fronteira.

## P2 — estratégia de produto e manutenção

### P2.1 — Escolher um produto principal

Recomendação: manter RESP/Valkey como núcleo estável e classificar as demais faces como previews independentes.

Uma política possível:

| Face | Estado sugerido agora | Porta de promoção |
|---|---|---|
| RESP2/RESP3 | Beta | Blackbox, recovery e sharding verdes na CI |
| Raft | Experimental | Failover, snapshot, membership e crash matrix verdes |
| AMQP 0-9-1 | Experimental | Conformance, durable topology e confirms comprovados |
| AMQP 1.0 | Experimental | SASL obrigatório, limites e interop matrix verdes |
| MQTT | Experimental | Sessions, Will, QoS e persistence verdes |
| Kafka | Experimental | ACL, fencing, transações e consumer groups verdes |
| SQS | Experimental | FIFO atomicidade, visibility, dedup e auth verdes |

Não adicionar novos protocolos até pelo menos um dos previews atingir os critérios de promoção.

### P2.2 — Manter uma única matriz de compatibilidade

Hoje a informação está distribuída entre README, `DRIFT.md`, guias por protocolo, TODOs e relatórios de revisão.

#### Ações

- Criar uma matriz gerada por testes, não mantida manualmente.
- Separar `supported`, `partial`, `unsupported` e `divergent by design`.
- Associar cada linha a um teste ou issue.
- Exibir a data e a versão do oracle usada.

### P2.3 — Fixar dependências de teste

As dependências `fluent-asserts` e `unit-threaded` usam `"*"`. Durante esta revisão, `dub test` exibiu incompatibilidade entre a versão selecionada de `silly` e a exigida por `fluent-asserts`.

#### Ações

- Substituir `"*"` por intervalos ou versões conhecidas.
- Atualizar e versionar `dub.selections.json` de forma deliberada.
- Adicionar um job de build limpo, sem cache prévio.

#### Critério de conclusão

- Checkout limpo produz a mesma resolução de dependências na máquina local e na CI.
- `dub test` não emite warnings de versões incompatíveis.

### P2.4 — Atualizar o roadmap

O README ainda apresenta sharding e serialização como roadmap, embora outras seções os descrevam como implementados. Roadmap desatualizado reduz a confiança no restante da documentação.

#### Ações

- Separar `implemented`, `hardening`, `planned` e `out of scope`.
- Remover itens já entregues ou indicar claramente a fase incompleta.
- Ligar cada item a um critério verificável, não apenas a uma feature.

## P3 — comunidade, operação e apresentação

### P3.1 — Adicionar documentos operacionais

Arquivos recomendados:

- `SECURITY.md`: reporte responsável, versões suportadas e prazo esperado.
- `CONTRIBUTING.md`: build limpo, testes obrigatórios e convenções.
- `CHANGELOG.md`: mudanças incompatíveis e correções de durabilidade.
- guia de upgrade e rollback;
- runbook de recuperação de AOF/Raft;
- modelo de ameaça por listener/protocolo.

### P3.2 — Melhorar o Docker Compose de exemplo

O Compose publica o dashboard em `0.0.0.0` e usa a senha literal `changeme`. Mesmo sendo voltado a desenvolvimento, exemplos são frequentemente copiados para ambientes reais.

#### Ações

- Vincular o dashboard a `127.0.0.1` por padrão.
- Exigir senha por variável sem valor default fraco.
- Adicionar configuração segura de exemplo para listeners externos.
- Fazer o healthcheck executar `PING`, não somente abrir o socket.

## Ordem recomendada de execução

## Modificações concretas sugeridas

Esta seção traduz as prioridades acima em alterações localizadas. Os nomes finais das APIs podem variar; o importante é preservar as invariantes e os critérios de validação.

### 1. Corrigir documentação e metadados imediatamente

#### `README.md`

- Substituir o subtítulo por algo como: `An experimental, high-performance Redis-compatible data engine written in D.`
- Substituir “Swap it in for Redis” por “Test it against the supported Redis surface”.
- Remover “drop-in” sem qualificação e apontar diretamente para `DRIFT.md`.
- Trocar a descrição do badge “full test suite” por uma lista exata do que o workflow executa.
- Adicionar uma tabela de maturidade das faces perto de “Protocol faces”.
- Mover benchmarks para depois de compatibilidade, segurança e limitações.
- Corrigir a seção License para a licença escolhida.
- Atualizar o roadmap para não listar sharding e serialização já implementados como trabalhos futuros genéricos.

#### `dub.json`

- Manter o valor de `license` consistente com `LICENSE`.
- Substituir dependências de teste com `"*"` por versões conhecidas.
- Considerar mover configurações de benchmark para um `dub.sdl`/pacote separado se continuarem crescendo.
- Adicionar uma configuração `unittest-core` que não compile as faces experimentais, permitindo testar o núcleo isoladamente.

#### `docker-compose.yml`

- Publicar o dashboard apenas em loopback:

```yaml
ports:
  - "6379:6379"
  - "127.0.0.1:6380:6380"
```

- Remover `changeme` e exigir `DREADS_DASHBOARD_PASSWORD` via `.env`:

```yaml
environment:
  DREADS_DASHBOARD_PASSWORD: "${DREADS_DASHBOARD_PASSWORD:?set DREADS_DASHBOARD_PASSWORD}"
```

- Fazer o healthcheck enviar um comando RESP e verificar `PONG`.

### 2. Transformar achados de revisão em trabalho rastreável

Criar `SECURITY-AUDIT.md` ou issues equivalentes com uma tabela como:

| ID | Origem | Severidade | Status | Reproducer | Teste | Commit |
|---|---|---|---|---|---|---|
| RAFT-001 | CODEX-REVIEW #1 | Critical | confirmed | caminho/comando | nome do teste | hash |

Não apagar os relatórios originais. Eles são evidência e contexto. A tabela deve ser a fonte de verdade operacional.

### 3. Corrigir serialização cross-shard em `server.d`

O código cross-shard não deve escrever ou ler inteiros fazendo cast de offsets arbitrários para ponteiros tipados. Por exemplo, layouts como `space.ptr + 4` não garantem alinhamento para `ulong`.

#### Modificação sugerida

- Criar helpers únicos em um módulo como `wirebytes.d`:

```d
@nogc nothrow pure
void putU32LE(ubyte[] dst, size_t off, uint value);

@nogc nothrow pure
void putU64LE(ubyte[] dst, size_t off, ulong value);

@nogc nothrow pure
bool getU32LE(const(ubyte)[] src, size_t off, out uint value);

@nogc nothrow pure
bool getU64LE(const(ubyte)[] src, size_t off, out ulong value);
```

- Implementar por cópia de bytes ou primitives seguras, sem dereference desalinhado.
- Usar esses helpers em todos os comandos, replies e batches cross-shard.
- Validar o comprimento total antes de ler qualquer campo.
- Não transportar `Pending*` cru no wire interno se um identificador `{shard, slot, generation}` puder substituí-lo.

#### Testes

- Buffers começando em todos os offsets de 0 a 15.
- Frames truncados em cada byte possível.
- Roundtrip dos maiores tamanhos aceitos.
- ASan/UBSan ou equivalente em arquitetura ARM, além de x86-64.

### 4. Substituir ponteiros de `Pending` por handles geracionais

Guardar `Pending*` em buffers malloc/calloc e atravessar threads torna lifetime, rooting e uso após reutilização difíceis de provar.

#### Modificação sugerida

```d
struct PendingHandle
{
    uint ownerShard;
    uint slot;
    uint generation;
}
```

- Cada shard mantém uma tabela própria de slots.
- Alocar incrementa a geração do slot.
- Reply contém o handle, não um endereço.
- O owner valida slot e geração antes de completar a operação.
- Cancelamento invalida a geração anterior.
- Somente o owner acessa ou recicla o objeto real.

#### Benefícios

- Elimina ponteiros crus em codecs.
- Reduz risco de use-after-free e ABA.
- Torna replies atrasados detectáveis e descartáveis.
- Facilita testes de cancelamento, timeout e reorder.

### 5. Corrigir a fila de propostas Raft

Uma fila SPSC somente é segura se houver exatamente um produtor. Expiry por shard e outras origens não podem escrever diretamente na mesma fila presumindo SPSC.

#### Alternativas aceitáveis

1. Manter SPSC e fazer todo produtor encaminhar a proposta ao único owner da fila.
2. Criar uma SPSC por produtor e fazer o consumidor drenar todas.
3. Substituir por MPSC cuja correção esteja testada e documentada.

A primeira opção é a mais simples para preservar o desenho atual.

#### Testes

- Expiry concorrente em 1, 4 e 8 shards.
- Escritas de cliente simultâneas à expiry.
- ThreadSanitizer, se viável com o toolchain.
- Contagem de propostas produzidas igual à soma de aplicadas, rejeitadas e canceladas.

### 6. Tornar o estado de apply local ao contexto

Um `__gshared bool` não representa applies concorrentes nem nesting. O estado deve pertencer ao shard/thread ou ao contexto da operação.

#### Modificação sugerida

- Introduzir `ApplyContext`:

```d
struct ApplyContext
{
    bool fromReplicatedLog;
    ulong logicalClock;
    ulong logIndex;
    uint shardId;
}
```

- Passar o contexto explicitamente ao dispatch ou mantê-lo em TLS por shard.
- Se nesting for permitido, usar contador/stack com RAII, não booleano.
- Fazer expiry consultar o contexto recebido, não estado global de processo.

#### Testes

- Applies simultâneos em shards distintos.
- Comando sobre chave expirada durante apply.
- Nested script/effect apply.
- Garantir que nenhum DEL adicional seja proposto indevidamente.

### 7. Criar uma API única de confirmação durável

Hoje cada protocolo pode decidir localmente quando responder. Isso permite que AMQP, Kafka, MQTT ou SQS confirmem antes de o shard owner aplicar e persistir.

#### Modificação sugerida

```d
enum AckLevel
{
    accepted,
    applied,
    appended,
    flushed,
    quorumCommitted
}

struct OperationResult
{
    AckLevel reached;
    ulong operationId;
    // erro e metadados do protocolo
}
```

- O storage core devolve o estágio realmente alcançado.
- Cada protocolo declara o nível necessário para seu tipo de ack.
- A resposta só é serializada após o future/pending atingir esse nível.
- Para operação remota, o flush ocorre no shard owner, não no shard que recebeu a conexão.

#### Testes de crash

Para cada estágio, matar o processo e verificar:

- se uma operação confirmada reaparece após restart;
- se uma operação não confirmada pode ser repetida com segurança;
- se dedup/idempotência evita duplicação indevida.

### 8. Reestruturar SQS em torno de operações atômicas

Elegibilidade, remoção da fila, lock de grupo e criação do in-flight não podem ser comandos separados com yields intermediários.

#### Modificação sugerida

- Implementar uma primitive interna `sqsReceiveAtomic` executada inteiramente no owner shard.
- Ela deve, numa única operação:

```text
find eligible message
-> verify group lock
-> remove ready record
-> acquire group lock
-> create receipt/in-flight record
-> return delivery
```

- Se qualquer etapa falhar, não produzir resposta parcial.
- Unificar `DeleteMessage` e `DeleteMessageBatch` sobre o mesmo helper que libera o lock FIFO.
- `PurgeQueue` deve apagar ready, delayed, in-flight, dedup e group-lock state.
- Trocar comprimentos de 16 bits por 32/64 bits com limite explícito.
- Rejeitar bodies acima do limite em vez de truncar.
- Aplicar validação FIFO e dedup igualmente nos caminhos single e batch.
- Adicionar autenticação ou declarar inequivocamente que o listener SQS só pode operar em rede confiável.

#### Testes

- Dois receives simultâneos para uma única mensagem FIFO.
- Batch delete seguido de mensagem no mesmo grupo.
- Purge com mensagens ready, delayed e in-flight.
- Bodies em `limit-1`, `limit` e `limit+1`.
- Dedup misturando chamadas single e batch.

### 9. Persistir sessões MQTT como uma unidade versionada

Um marcador de existência não é uma sessão. Para responder `Session Present`, o servidor precisa recuperar subscriptions, QoS state, packet ids e mensagens pendentes exigidas pelo protocolo.

#### Modificação sugerida

- Criar um registro versionado por sessão contendo:

```text
client id
protocol version
expiry deadline
subscriptions + options/share group
inbound QoS 2 state
outbound QoS 1/2 state
offline outbox
next packet id
will + will deadline
```

- Persistir mudanças relevantes antes do ack correspondente.
- Só marcar `Session Present` quando o estado necessário tiver sido restaurado.
- Separar `wasConnected` de `connected`; decidir o disparo do Will antes de limpar o estado.
- Na remoção de subscription, comparar filtro e share group, não apenas filtro.
- Rejeitar `Receive Maximum = 0` no MQTT 5.

#### Testes

- Restart entre PUBLISH e PUBACK/PUBREC/PUBREL/PUBCOMP.
- Will em reset TCP, timeout, DISCONNECT normal e reason `0x04`.
- Duas share groups com o mesmo filtro.
- Session expiry antes e depois do restart.

### 10. Persistir e sincronizar topologia AMQP

Exchanges, queues e bindings duráveis não podem viver apenas em TLS/control-plane memory.

#### Modificação sugerida

- Representar topologia em registros versionados no storage core.
- Aplicar declare/bind/delete de forma idempotente.
- Só devolver `declare-ok`/`bind-ok` após todos os shards necessários reconhecerem a atualização ou após a topologia ser consultada de uma fonte autoritativa comum.
- Para confirms, aguardar apply e persistência no shard da fila.
- Implementar state machine explícita de handshake; frames de data antes de autenticação devem fechar a conexão.
- Verificar overflow usando `if (fsize > maxFrame - headerSize)`, nunca `fsize + headerSize > maxFrame`.

#### Testes

- Restart preserva exchange, queue, binding e mensagem persistente.
- Publish imediatamente após `bind-ok` em outro shard.
- Publish antes de SASL/handshake é rejeitado.
- Overshoot e wrap de frame size.

### 11. Completar fencing e timeout Kafka

O producer epoch precisa ser uma autoridade global para o transactional id e deve ser verificado em toda operação transacional.

#### Modificação sugerida

- Persistir `{transactionalId, producerId, epoch, timeout, startedAt, state}`.
- Centralizar `validateProducerEpoch` e chamá-lo em Produce, AddPartitionsToTxn, AddOffsetsToTxn e EndTxn.
- Ao elevar epoch, invalidar imediatamente produtores antigos em todas as partições.
- Sweeper deve abortar transações expiradas e escrever markers necessários.
- Recuperação deve reconstruir coordinator state a partir do estado persistido.
- Toda API administrativa e de grupos deve passar por autorização antes de consultar ou mutar estado.
- Colocar limites e TTL em tabelas de transactional ids e grupos abandonados.

#### Testes

- Zombie producer após incremento de epoch.
- Timeout durante producer crash.
- Restart com transação aberta.
- Read committed depois de aborto por timeout.
- ACLs para Create/DeleteTopics, configs, records, groups e ACL administration.

### 12. Limitar codecs AMQP 1.0 e demais parsers

#### Modificação sugerida

- Limitar profundidade de valores descritos/listas/maps.
- Limitar quantidade total de elementos decodificados por frame.
- Validar ranges antes de iterar; nunca aceitar intervalos próximos de `ulong.max` como trabalho literal.
- Limitar links por conexão e fragment buffers agregados.
- Exigir conclusão de SASL quando configurado antes do header AMQP normal.
- Usar contador de budget no decoder:

```d
struct DecodeBudget
{
    uint depthLeft;
    size_t bytesLeft;
    size_t elementsLeft;
}
```

#### Testes

- Profundidade exatamente no limite e uma acima.
- Maps/lists enormes com frame pequeno e referências maliciosas.
- Disposition com ranges extremos.
- Muitos links numa única conexão.

### 13. Reorganizar a CI em workflows verificáveis

Estrutura sugerida:

```text
.github/workflows/
  ci.yml                 unit + build + smoke
  sanitizers.yml         ASan e verificações de UB
  valkey-conformance.yml blackbox live
  protocols.yml          matriz AMQP/MQTT/Kafka/SQS
  raft-chaos.yml         cluster, failover, snapshot, kill
  nightly-fuzz.yml       codecs e state machines
```

No workflow principal:

- adicionar `unittest-no-dashboard`;
- executar build limpo usando `dub.selections.json`;
- armazenar logs de falha como artefatos;
- cancelar claims de “full suite” enquanto workflows nightly não estiverem verdes.

### 14. Adicionar observabilidade das invariantes

Métricas mínimas recomendadas:

- tamanho e high-water mark de cada fila cross-shard;
- pending operations por estágio de ack;
- tempo de apply, append, fsync e quorum commit;
- replies tardios ou handles com geração inválida;
- Raft commit index versus applied index;
- sessões, links, groups e transactions por protocolo;
- rejects por limite de memória/frame/depth;
- auth failures e ACL denials;
- recovery/replay duration e último ponto durável.

Essas métricas devem aparecer em `INFO`, logs estruturados ou dashboard sem adicionar alocação ao hot path quando desativadas.

### 15. Sequência de commits sugerida

Para reduzir risco de uma mudança grande e difícil de revisar:

1. `docs: align license, maturity labels and CI claims`
2. `test: pin test dependencies and add clean build job`
3. `refactor(shard): add endian-safe byte codec helpers`
4. `refactor(shard): replace pending pointers with generational handles`
5. `fix(raft): restore single-producer proposal ownership`
6. `fix(raft): make apply context shard-local and nest-safe`
7. `feat(core): introduce explicit durable acknowledgement levels`
8. `fix(sqs): make receive/delete/purge FIFO state atomic`
9. `fix(mqtt): persist complete sessions and repair Will teardown`
10. `fix(amqp): persist topology and delay confirms until owner durability`
11. `fix(kafka): enforce global epochs and transaction expiry`
12. `security(protocols): enforce handshake and resource budgets`
13. `ci: add protocol conformance, raft chaos and sanitizer workflows`
14. `refactor(protocols): split codecs, state machines, auth and storage`

Cada commit de correção deve incluir o teste que demonstra o defeito. Refactors puramente estruturais devem ser separados das mudanças de comportamento.

### Fase 1 — verdade e contenção

- [ ] Corrigir a licença.
- [ ] Rebaixar claims não comprovados no README.
- [ ] Classificar as faces por maturidade.
- [ ] Congelar novas features/protocolos.
- [ ] Consolidar todos os achados críticos e altos.

### Fase 2 — segurança e perda de dados

- [ ] Corrigir bypasses de autenticação e ACL.
- [ ] Corrigir confirmações anteriores à durabilidade.
- [ ] Corrigir races, lifetime e alinhamento cross-shard/Raft.
- [ ] Limitar memória, frame sizes, recursão e loops controlados pelo cliente.
- [ ] Adicionar regressões para cada correção.

### Fase 3 — confiança automatizada

- [ ] Executar ASan na CI.
- [ ] Executar Valkey blackbox regularmente.
- [ ] Adicionar matriz de sharding.
- [ ] Adicionar cluster Raft e crash injection.
- [ ] Executar conformance das cinco faces.
- [ ] Publicar artefatos e skips da CI.

### Fase 4 — redução de complexidade

- [ ] Separar codecs, state machines, auth e storage.
- [ ] Formalizar ownership de buffers e pontos de yield.
- [ ] Centralizar o modelo de confirmação durável.
- [ ] Gerar a matriz de compatibilidade a partir dos testes.

### Fase 5 — promoção de maturidade

- [ ] Promover uma face por vez de experimental para beta/stable.
- [ ] Só restaurar claims de “reliable” após testes de falha contínuos.
- [ ] Publicar política de suporte, segurança e upgrades.

## Definition of Done para um release estável

Um release não deve ser chamado de estável até atender a todos os itens abaixo:

- [ ] Zero vulnerabilidades críticas ou altas conhecidas sem mitigação.
- [ ] Nenhuma resposta de sucesso pode preceder o estágio de durabilidade prometido.
- [ ] Unit, ASan, blackbox, sharding, Raft e conformance executados automaticamente.
- [ ] Testes de `kill -9` cobrem os pontos críticos de confirmação e replay.
- [ ] Compatibilidade e divergências são geradas ou verificadas por teste.
- [ ] Limites de recursos existem para todos os listeners não confiáveis.
- [ ] Licença e posicionamento são consistentes em todos os artefatos.
- [ ] Upgrade, rollback e recuperação estão documentados.
- [ ] O release foi exercitado em soak test prolongado com carga mista.
- [ ] Métricas permitem identificar lag, erros, filas internas, fsync e perda de quorum.

## Observações sobre esta revisão

- Nenhum arquivo de código foi alterado durante a produção deste relatório.
- O working tree já possuía alterações não commitadas em módulos de protocolo e novos relatórios/testes; elas foram preservadas.
- A execução local de `dub test --compiler=ldc2` não pôde ser concluída porque o sandbox não permite remover um artefato em `~/.dub`. Isso é uma limitação do ambiente da revisão, não uma falha confirmada do projeto.
- O comando revelou, porém, um warning real de resolução de dependência: `silly@1.1.1` não satisfaz a versão exigida por `fluent-asserts`.
- Os defeitos enumerados nos relatórios técnicos existentes precisam ser reproduzidos e triados individualmente; sua presença foi usada aqui como evidência de risco e lacuna de assurance, não como afirmação de que todos permanecem exploráveis no working tree atual.

## Conclusão

O dreads tem um núcleo que merece ser levado a sério. Para transformá-lo em infraestrutura confiável, a melhor decisão agora é reduzir velocidade de expansão e aumentar velocidade de verificação. O próximo marco importante não deveria ser um novo protocolo ou uma nova feature, mas um release cuja durabilidade, segurança, recuperação e compatibilidade sejam continuamente demonstradas pela CI.
