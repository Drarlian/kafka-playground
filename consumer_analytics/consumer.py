import json
import os
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "reservas-pedido")
GROUP_ID = os.environ.get("GROUP_ID", "grupo-analytics")

total_pedidos = 0
faturamento_total = 0.0
quantidade_por_produto = defaultdict(int)


def criar_consumer(max_tentativas: int = 15) -> Consumer:
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    for tentativa in range(1, max_tentativas + 1):
        try:
            consumer = Consumer(config)
            consumer.list_topics(timeout=5)
            print(f"[analytics] conectado ao Kafka em {BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as exc:
            print(
                f"[analytics] tentativa {tentativa}/{max_tentativas} "
                f"falhou ao conectar no Kafka: {exc}"
            )
            time.sleep(3)
    raise RuntimeError("nao foi possivel conectar ao Kafka")


def processar(pedido: dict) -> None:
    global total_pedidos, faturamento_total

    total_pedidos += 1
    faturamento_total += pedido["valor_total"]
    quantidade_por_produto[pedido["produto"]] += pedido["quantidade"]

    print(
        f"[analytics] pedido {pedido['pedido_id']} processado | "
        f"total de pedidos={total_pedidos} | "
        f"faturamento acumulado=R$ {faturamento_total:.2f}"
    )

    if total_pedidos % 5 == 0:
        top_produto = max(quantidade_por_produto.items(), key=lambda item: item[1])
        print(
            f"[analytics] === resumo a cada 5 pedidos === "
            f"produto mais vendido ate agora: {top_produto[0]} "
            f"({top_produto[1]} unidades)"
        )


def main():
    consumer = criar_consumer()
    consumer.subscribe([TOPIC_NAME])
    print(f"[analytics] consumindo topico '{TOPIC_NAME}' (grupo={GROUP_ID})")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[analytics] erro no consumo: {msg.error()}")
                continue

            pedido = json.loads(msg.value())
            processar(pedido)
    except KeyboardInterrupt:
        print("[analytics] encerrando...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
