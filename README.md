# ProtocoloKafka

Simulação de microsserviços de um e-commerce conversando via Kafka: 1
producer publica reservas de pedido e vários consumers independentes
(grupos separados) reagem a cada reserva para finalidades diferentes.

Para detalhes da infraestrutura Kafka (docker-compose, KRaft, comandos CLI),
veja [README-kafka.md](README-kafka.md); para os conceitos de tópico,
partição, offset e instância, veja
[CONCEITOS-KAFKA.md](CONCEITOS-KAFKA.md). Este arquivo cobre só a camada
de aplicação.

## Arquitetura

```
producer/  ──publica reserva de pedido──▶  topico "reservas-pedido"
                                                │
                    consumido de forma independente por:
                                                │
                        ┌───────────────────────┼───────────────────────┐
                        ▼                       ▼                       ▼
          consumer_notificacao_cliente   consumer_notificacao_interna   consumer_estoque
          (e-mail/SMS ao cliente)        (alerta para vendas/logistica) (reserva a quantidade
                                                                          no inventario)
                        │                                               │
                        ▼                                               ▼
              consumer_analytics                              consumer_auditoria
        (agrega faturamento e ranking)                  (grava historico em .jsonl)
```

Cada consumer usa um `group.id` próprio, então todos recebem **todas** as
mensagens do tópico de forma independente — não competem entre si. Os 3
primeiros simulam o fluxo obrigatório pedido pelo negócio (notificar cliente,
notificar equipe interna, reservar estoque); `analytics` e `auditoria` são
consumers extras que mostram que dá para plugar quantos consumidores forem
necessários no mesmo evento.

| Serviço                        | Responsabilidade                                          | group.id                  |
|---------------------------------|-------------------------------------------------------------|---------------------------|
| `producer`                     | Gera e publica reservas de pedido simuladas                 | —                         |
| `consumer_notificacao_cliente` | Simula envio de e-mail/SMS confirmando a reserva ao cliente  | `grupo-notificacao-cliente`|
| `consumer_notificacao_interna` | Simula alerta para a equipe de vendas/logística              | `grupo-notificacao-interna`|
| `consumer_estoque`             | Reserva a quantidade pedida no inventário (com controle de estoque insuficiente, commit manual e DLQ) | `grupo-estoque` |
| `consumer_analytics`           | Agrega faturamento acumulado e ranking de produtos           | `grupo-analytics`         |
| `consumer_auditoria`           | Grava cada evento em `data/auditoria.jsonl`                  | `grupo-auditoria`         |

## Como rodar

1. Subir o Kafka (ver [README-kafka.md](README-kafka.md)):
   ```powershell
   docker compose up -d
   ```

2. Instalar as dependências no venv do projeto:
   ```powershell
   .venv\Scripts\pip install -r requirements.txt
   ```

3. Rodar cada serviço em um terminal separado (todos usam
   `localhost:9092` por padrão):
   ```powershell
   .venv\Scripts\python producer\producer.py
   .venv\Scripts\python consumer_notificacao_cliente\consumer.py
   .venv\Scripts\python consumer_notificacao_interna\consumer.py
   .venv\Scripts\python consumer_estoque\consumer.py
   .venv\Scripts\python consumer_analytics\consumer.py
   .venv\Scripts\python consumer_auditoria\consumer.py
   ```

### Alternativa: tudo em containers

Os 6 serviços também têm `Dockerfile` e estão no compose sob o profile
`apps`. Assim o `docker compose up -d` continua subindo **só o broker**
(fluxo acima, rodando os `.py` pelo PyCharm), e o profile sobe o sistema
inteiro:

```powershell
docker compose --profile apps up -d --build      # broker + 6 serviços
docker compose --profile apps --profile ui up -d --build   # + Kafka UI
docker compose logs -f consumer_estoque          # logs de um serviço
docker compose --profile apps down               # derruba tudo
```

Dentro da rede do compose os serviços usam `kafka:29092`, não
`localhost:9092` — `localhost` ali dentro seria o próprio container. Isso já
vem configurado via `KAFKA_BOOTSTRAP_SERVERS` em cada serviço.

## Instâncias e partições

> Explicação completa dos conceitos em
> [CONCEITOS-KAFKA.md](CONCEITOS-KAFKA.md). O resumo abaixo é só o que
> vale para este compose.

O tópico nasce com 3 partições (`KAFKA_NUM_PARTITIONS: 3`). Cada consumer roda
com um número **diferente** de instâncias (`deploy.replicas` no compose), de
propósito: assim o mesmo tópico mostra os três cenários possíveis de divisão
lado a lado, e fica visível que o número certo de instâncias é uma decisão de
cada serviço, não do Kafka.

| Serviço                        | Instâncias | Como as 3 partições ficam            | Por quê                                         |
|--------------------------------|:----------:|--------------------------------------|-------------------------------------------------|
| `consumer_notificacao_cliente` |   **3**    | 1 partição para cada instância       | sem estado — paralelismo máximo do grupo         |
| `consumer_notificacao_interna` |   **2**    | uma pega 2 partições, a outra pega 1 | sem estado — divisão não precisa ser redonda     |
| `consumer_estoque`             |   **1**    | a única instância recebe as 3        | `ESTOQUE` é um dict na memória do processo       |
| `consumer_analytics`           |   **1**    | a única instância recebe as 3        | acumuladores na memória do processo              |
| `consumer_auditoria`           |   **1**    | a única instância recebe as 3        | todas escreveriam no mesmo `auditoria.jsonl`     |

Os 5 grupos continuam recebendo **100% dos pedidos** — isso não muda. O que
varia é como cada grupo reparte esse trabalho internamente: um pedido
processado pela instância 2 do `grupo-notificacao-cliente` não será processado
pelas instâncias 1 e 3 daquele mesmo grupo, porque a partição onde ele está
pertence a uma instância só.

Os dois consumers com estado ficam em 1 instância **porque o estado mora na
memória do processo**: com 3 instâncias, cada uma teria seu próprio catálogo de
80 teclados (ou seu próprio faturamento parcial) e os números impressos
deixariam de significar algo. Em produção esse estado moraria num banco
compartilhado e eles escalariam como os outros — aqui ele é didático de
propósito, e a limitação que isso cria é parte da lição.

### Vendo a divisão acontecer

```powershell
docker compose --profile apps up -d --build
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

A saída mostra, por grupo, qual `CONSUMER-ID` está com qual partição. Nos
grupos de notificação você verá as partições repartidas; nos demais, as 3 na
mesma linha de consumidor.

Para ver o **teto do paralelismo**, suba uma 4ª instância onde só há 3
partições:

```powershell
docker compose --profile apps up -d --scale consumer_notificacao_cliente=4
```

A quarta sobe, conecta, entra no grupo — e fica parada, sem log de pedido
nenhum. Não sobrou partição para ela. `--scale` sobrepõe o `replicas` do
compose, então para voltar ao normal é só rodar o `up -d` sem a flag.

O producer cria reservas de pedido simuladas (produto, cliente, quantidade,
valor) e publica no tópico `reservas-pedido` a cada 1-3 segundos. Como
`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, o tópico é criado automaticamente no
primeiro uso.

`consumer_estoque` mantém seu próprio catálogo de estoque em memória (como um
serviço de inventário real teria sua própria base) e vai imprimir um aviso de
**estoque insuficiente** quando um produto acabar — deixe o producer rodando
por um tempo para ver esse cenário acontecer.

## Variáveis de ambiente

Todos os serviços aceitam (com valores padrão já prontos para uso local):

| Variável                  | Padrão              | Descrição                        |
|----------------------------|---------------------|-----------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092`     | Endereço do broker                |
| `TOPIC_NAME`               | `reservas-pedido`    | Tópico usado por todos os serviços|
| `GROUP_ID`                 | varia por consumer   | Consumer group (isola o consumo)  |

`consumer_auditoria` também aceita `AUDITORIA_ARQUIVO` para trocar o caminho
do arquivo de log (padrão: `data/auditoria.jsonl` na raiz do projeto).

### Tratamento de erro e DLQ

`consumer_estoque` é o único consumer com **commit manual de offset e dead
letter queue** — os outros 4 usam auto-commit, para o contraste ficar visível
lado a lado. Os conceitos estão em
[CONCEITOS-KAFKA.md](CONCEITOS-KAFKA.md#reprocessamento-erros-e-dead-letter-queue).

Para as duas trilhas de erro acontecerem de verdade existem falhas simuladas,
todas desligáveis:

| Variável                 | Serviço    | Padrão                 | Efeito                                              |
|--------------------------|------------|------------------------|-----------------------------------------------------|
| `PCT_FORA_DE_CATALOGO`   | producer   | `10`                   | % de pedidos com produto inexistente → erro permanente |
| `FALHA_TRANSITORIA_PCT`  | estoque    | `15`                   | % de consultas ao inventário que falham → erro transitório |
| `MAX_TENTATIVAS`         | estoque    | `3`                    | tentativas antes de mandar para a DLQ               |
| `DLQ_TOPIC`              | estoque    | `reservas-pedido.dlq`  | tópico onde as mensagens são estacionadas           |

Zerando `PCT_FORA_DE_CATALOGO` e `FALHA_TRANSITORIA_PCT` no
[docker-compose.yml](docker-compose.yml) o sistema roda sem falha nenhuma.

Os produtos fora de catálogo **só** são erro para o estoque — os outros 4
consumers processam essas mensagens normalmente. Um produto desconhecido é
problema de quem mantém inventário, não de quem notifica ou audita.

O fluxo de erro tem testes que rodam **sem precisar do Kafka** — usam dublês no
lugar da mensagem e do producer da DLQ:

```powershell
.venv\Scripts\python consumer_estoque\teste_dlq.py
```

São 7 cenários (sucesso, erro permanente, JSON inválido, transitório que
esgota, transitório que se recupera, DLQ que não confirma a escrita, e
idempotência). Servem tanto de verificação quanto de documentação executável
do contrato de `tratar()`.

Para ver as mensagens estacionadas, com os headers de diagnóstico:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reservas-pedido.dlq --from-beginning --property print.headers=true
```

## Verificando via Kafka UI

```powershell
docker compose --profile ui up -d
```

Acesse http://localhost:8080 para ver o tópico `reservas-pedido`, as
mensagens e o lag de cada consumer group em tempo real.
