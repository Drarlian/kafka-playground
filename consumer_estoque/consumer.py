"""Servico de estoque com tratamento de erro e dead letter queue.

Este e o unico consumer do projeto que faz commit manual de offset. Os outros
usam auto-commit, que e mais simples mas tem uma consequencia importante: o
offset avanca mesmo quando o processamento falha, entao uma mensagem
problematica e perdida em silencio.

Aqui o fluxo e o oposto e esta detalhado em CONCEITOS-KAFKA.md:

    poll -> processa -> deu certo?  -> commit
                     -> erro transitorio?  -> tenta de novo (backoff)
                     -> esgotou tentativas / erro permanente?  -> DLQ -> commit

O commit so acontece depois do sucesso OU depois de a mensagem estar
estacionada na DLQ. Nunca antes: e isso que impede a perda silenciosa.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "reservas-pedido")
GROUP_ID = os.environ.get("GROUP_ID", "grupo-estoque")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", f"{TOPIC_NAME}.dlq")

MAX_TENTATIVAS = int(os.environ.get("MAX_TENTATIVAS", "3"))

# Chance de o "banco de inventario" simulado estar fora do ar. Existe para
# exercitar o caminho de erro transitorio -> retry. Coloque 0 para desligar e
# ver o fluxo feliz isolado.
FALHA_TRANSITORIA_PCT = float(os.environ.get("FALHA_TRANSITORIA_PCT", "15"))

# Catalogo proprio do servico de estoque (simula sua propria base de dados
# de inventario, independente do que o producer conhece).
ESTOQUE = {
    "teclado-mecanico": 80,
    "mouse-gamer": 120,
    "monitor-27-4k": 25,
    "cadeira-escritorio": 40,
    "headset-bluetooth": 90,
    "webcam-1080p": 60,
    "ssd-nvme-1tb": 70,
    "notebook-15-polegadas": 15,
}

# Commit manual da semantica at-least-once: a mesma mensagem pode ser
# entregue duas vezes, e sem protecao o estoque seria debitado em dobro.
#
# ATENCAO - este set e um MARCADOR DIDATICO, nao uma garantia completa.
# Ele cobre a duplicacao dentro do mesmo processo (tipicamente apos um
# rebalanceamento, quando a instancia recebe de volta uma particao e
# reprocessa mensagens cujo offset ainda nao tinha sido commitado).
#
# Ele NAO cobre o caso que mais motiva a idempotencia: o processo morrer
# entre o processar() e o commit(). Nesse cenario o set morre junto, e na
# volta o pedido e aplicado de novo. Idempotencia de verdade exige estado
# DURAVEL - uma constraint de unicidade no banco de inventario, por exemplo.
PEDIDOS_APLICADOS = set()


class ErroTransitorio(Exception):
    """Falha passageira (rede, timeout, dependencia fora do ar).

    Tentar de novo tem chance real de funcionar.
    """


class ErroPermanente(Exception):
    """Falha que vai se repetir identica em toda tentativa.

    Payload invalido, regra de negocio violada, produto inexistente. Retentar
    e so desperdicio - e pior, segura a particao enquanto isso.
    """


def criar_consumer(max_tentativas: int = 15) -> Consumer:
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        # A diferenca central em relacao aos outros consumers: quem decide
        # quando o offset avanca e este codigo, nao um timer em segundo plano.
        "enable.auto.commit": False,
    }
    for tentativa in range(1, max_tentativas + 1):
        try:
            consumer = Consumer(config)
            consumer.list_topics(timeout=5)
            print(f"[estoque] conectado ao Kafka em {BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as exc:
            print(
                f"[estoque] tentativa {tentativa}/{max_tentativas} "
                f"falhou ao conectar no Kafka: {exc}"
            )
            time.sleep(3)
    raise RuntimeError("nao foi possivel conectar ao Kafka")


def consultar_inventario(produto: str):
    """Simula a consulta ao banco de inventario, que as vezes esta indisponivel."""
    if random.uniform(0, 100) < FALHA_TRANSITORIA_PCT:
        raise ErroTransitorio("banco de inventario indisponivel (timeout)")
    return ESTOQUE.get(produto)


def processar(pedido: dict) -> None:
    pedido_id = pedido["pedido_id"]

    if pedido_id in PEDIDOS_APLICADOS:
        print(f"[estoque] pedido {pedido_id} ja aplicado, ignorando (idempotencia)")
        return

    produto = pedido["produto"]
    quantidade = pedido["quantidade"]

    if quantidade <= 0:
        raise ErroPermanente(f"quantidade invalida: {quantidade}")

    disponivel = consultar_inventario(produto)

    if disponivel is None:
        raise ErroPermanente(f"produto fora do catalogo de estoque: {produto}")

    if disponivel >= quantidade:
        ESTOQUE[produto] = disponivel - quantidade
        print(
            f"[estoque] reservado {quantidade}x {produto} para o pedido "
            f"{pedido_id} | estoque restante: {ESTOQUE[produto]}"
        )
    else:
        # Isto NAO e um erro: e um desfecho de negocio previsto. A mensagem
        # foi processada com sucesso, o pedido so nao pode ser atendido agora.
        # Mandar isso para a DLQ seria confundir "falhou" com "deu nao".
        print(
            f"[estoque] ESTOQUE INSUFICIENTE para {produto} no pedido "
            f"{pedido_id} | solicitado={quantidade}, "
            f"disponivel={disponivel} | pedido marcado para backorder"
        )

    PEDIDOS_APLICADOS.add(pedido_id)


def enviar_para_dlq(dlq: Producer, msg, erro: Exception, tentativas: int) -> bool:
    """Estaciona a mensagem no topico de dead letter, com contexto do erro.

    Devolve True se a DLQ confirmou a escrita. Em False o offset NAO pode
    avancar - a mensagem precisa ser reprocessada, senao some dos dois lugares.
    """
    dlq.produce(
        DLQ_TOPIC,
        key=msg.key(),
        # Payload ORIGINAL, intacto. A DLQ precisa ser reprocessavel: depois
        # de corrigir a causa, um replay republica isto no topico de origem.
        # Transformar a mensagem aqui inviabilizaria esse replay.
        value=msg.value(),
        headers={
            "erro": str(erro),
            "erro_tipo": type(erro).__name__,
            "topico_origem": msg.topic(),
            "particao_origem": str(msg.partition()),
            "offset_origem": str(msg.offset()),
            "consumer_group": GROUP_ID,
            "tentativas": str(tentativas),
            "falhou_em": datetime.now(timezone.utc).isoformat(),
        },
    )
    # produce() e assincrono: ele so enfileira a mensagem num buffer local.
    # O flush bloqueia ate o broker confirmar, e devolve quantas ficaram
    # pendentes. Qualquer coisa diferente de zero significa que a escrita NAO
    # foi confirmada - e ai o offset nao pode avancar.
    pendentes = dlq.flush(10)
    if pendentes > 0:
        print(
            f"[estoque] !!! FALHA ao gravar na DLQ ({pendentes} pendente(s)): "
            f"offset {msg.offset()} sera reprocessado"
        )
        return False

    print(
        f"[estoque] >>> DLQ: mensagem do offset {msg.offset()} "
        f"(particao {msg.partition()}) estacionada em '{DLQ_TOPIC}' "
        f"apos {tentativas} tentativa(s) | {type(erro).__name__}: {erro}"
    )
    return True


def tratar(msg, dlq: Producer) -> bool:
    """Processa uma mensagem e diz se o offset dela pode avancar.

    True no sucesso e tambem quando a mensagem foi estacionada na DLQ - nos
    dois casos o trabalho com ela terminou. False so quando a gravacao na DLQ
    nao foi confirmada, caso em que ela precisa voltar.
    """
    try:
        pedido = json.loads(msg.value())
    except json.JSONDecodeError as exc:
        # Permanente por natureza: o payload nao vai se consertar sozinho.
        # Zero tentativas - repetir seria so segurar a particao.
        return enviar_para_dlq(dlq, msg, exc, tentativas=0)

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            processar(pedido)
            return True

        except ErroPermanente as exc:
            return enviar_para_dlq(dlq, msg, exc, tentativas=tentativa)

        except ErroTransitorio as exc:
            if tentativa == MAX_TENTATIVAS:
                return enviar_para_dlq(dlq, msg, exc, tentativas=tentativa)

            # Backoff exponencial: 2s, 4s. O total fica MUITO abaixo do
            # max.poll.interval.ms (5 min por padrao) de proposito - passar
            # desse limite faz o broker expulsar esta instancia do grupo e
            # disparar rebalanceamento.
            espera = 2 ** tentativa
            print(
                f"[estoque] tentativa {tentativa}/{MAX_TENTATIVAS} falhou "
                f"({exc}), repetindo em {espera}s"
            )
            time.sleep(espera)

        except Exception as exc:
            # Rede de seguranca: qualquer erro nao previsto vai para a DLQ em
            # vez de derrubar o processo. Sem isto, uma unica mensagem
            # inesperada mataria o container.
            return enviar_para_dlq(dlq, msg, exc, tentativas=tentativa)

    # Inalcancavel: o laco sempre retorna. Guarda contra edicoes futuras que
    # mexam no fluxo acima - na duvida, nao avanca o offset.
    return False


def main():
    consumer = criar_consumer()
    dlq = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    consumer.subscribe([TOPIC_NAME])
    print(f"[estoque] consumindo topico '{TOPIC_NAME}' (grupo={GROUP_ID})")
    print(
        f"[estoque] commit manual | ate {MAX_TENTATIVAS} tentativas | "
        f"DLQ='{DLQ_TOPIC}' | falha transitoria simulada em "
        f"{FALHA_TRANSITORIA_PCT}% das consultas"
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[estoque] erro no consumo: {msg.error()}")
                continue

            # So commita depois do sucesso ou de a mensagem estar CONFIRMADA
            # na DLQ. Se nem uma coisa nem outra, o offset fica onde esta e a
            # mensagem volta no proximo poll - reprocessar e sempre melhor que
            # perder.
            if tratar(msg, dlq):
                consumer.commit(msg)
    except KeyboardInterrupt:
        print("[estoque] encerrando...")
    finally:
        dlq.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
