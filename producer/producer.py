import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "reservas-pedido")

PRODUTOS = [
    ("teclado-mecanico", 350.00),
    ("mouse-gamer", 180.00),
    ("monitor-27-4k", 2100.00),
    ("cadeira-escritorio", 890.00),
    ("headset-bluetooth", 260.00),
    ("webcam-1080p", 210.00),
    ("ssd-nvme-1tb", 430.00),
    ("notebook-15-polegadas", 4200.00),
]

CLIENTES = [f"cliente-{i:03d}" for i in range(1, 31)]


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] falha ao entregar mensagem: {err}")
    else:
        print(
            f"[producer] reserva de pedido enviada -> topico={msg.topic()} "
            f"particao={msg.partition()} offset={msg.offset()}"
        )


def gerar_reserva_pedido() -> dict:
    produto, preco_unitario = random.choice(PRODUTOS)
    quantidade = random.randint(1, 5)
    return {
        "evento": "reserva_pedido_criada",
        "pedido_id": str(uuid.uuid4()),
        "cliente_id": random.choice(CLIENTES),
        "produto": produto,
        "quantidade": quantidade,
        "preco_unitario": preco_unitario,
        "valor_total": round(preco_unitario * quantidade, 2),
        "criado_em": datetime.now(timezone.utc).isoformat(),
    }


def criar_producer(max_tentativas: int = 15) -> Producer:
    config = {"bootstrap.servers": BOOTSTRAP_SERVERS}
    for tentativa in range(1, max_tentativas + 1):
        try:
            producer = Producer(config)
            producer.list_topics(timeout=5)
            print(f"[producer] conectado ao Kafka em {BOOTSTRAP_SERVERS}")
            return producer
        except Exception as exc:
            print(
                f"[producer] tentativa {tentativa}/{max_tentativas} "
                f"falhou ao conectar no Kafka: {exc}"
            )
            time.sleep(3)
    raise RuntimeError("nao foi possivel conectar ao Kafka")


def main():
    producer = criar_producer()
    print(f"[producer] publicando reservas de pedido simuladas no topico '{TOPIC_NAME}'")

    try:
        while True:
            pedido = gerar_reserva_pedido()
            producer.produce(
                TOPIC_NAME,
                key=pedido["pedido_id"],
                value=json.dumps(pedido),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        print("[producer] encerrando...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
