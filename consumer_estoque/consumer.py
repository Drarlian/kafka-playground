import json
import os
import time

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "reservas-pedido")
GROUP_ID = os.environ.get("GROUP_ID", "grupo-estoque")

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
            print(f"[estoque] conectado ao Kafka em {BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as exc:
            print(
                f"[estoque] tentativa {tentativa}/{max_tentativas} "
                f"falhou ao conectar no Kafka: {exc}"
            )
            time.sleep(3)
    raise RuntimeError("nao foi possivel conectar ao Kafka")


def processar(pedido: dict) -> None:
    produto = pedido["produto"]
    quantidade = pedido["quantidade"]
    disponivel = ESTOQUE.get(produto, 0)

    if disponivel >= quantidade:
        ESTOQUE[produto] = disponivel - quantidade
        print(
            f"[estoque] reservado {quantidade}x {produto} para o pedido "
            f"{pedido['pedido_id']} | estoque restante: {ESTOQUE[produto]}"
        )
    else:
        print(
            f"[estoque] ESTOQUE INSUFICIENTE para {produto} no pedido "
            f"{pedido['pedido_id']} | solicitado={quantidade}, "
            f"disponivel={disponivel} | pedido marcado para backorder"
        )


def main():
    consumer = criar_consumer()
    consumer.subscribe([TOPIC_NAME])
    print(f"[estoque] consumindo topico '{TOPIC_NAME}' (grupo={GROUP_ID})")

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

            pedido = json.loads(msg.value())
            processar(pedido)
    except KeyboardInterrupt:
        print("[estoque] encerrando...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
