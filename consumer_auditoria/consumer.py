import json
import os
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "reservas-pedido")
GROUP_ID = os.environ.get("GROUP_ID", "grupo-auditoria")

ARQUIVO_AUDITORIA = Path(
    os.environ.get("AUDITORIA_ARQUIVO", Path(__file__).parent.parent / "data" / "auditoria.jsonl")
)


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
            print(f"[auditoria] conectado ao Kafka em {BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as exc:
            print(
                f"[auditoria] tentativa {tentativa}/{max_tentativas} "
                f"falhou ao conectar no Kafka: {exc}"
            )
            time.sleep(3)
    raise RuntimeError("nao foi possivel conectar ao Kafka")


def processar(pedido: dict, msg) -> None:
    registro = {
        "topico": msg.topic(),
        "particao": msg.partition(),
        "offset": msg.offset(),
        "pedido": pedido,
    }
    with ARQUIVO_AUDITORIA.open("a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")

    print(
        f"[auditoria] pedido {pedido['pedido_id']} gravado em "
        f"{ARQUIVO_AUDITORIA} (offset={msg.offset()})"
    )


def main():
    ARQUIVO_AUDITORIA.parent.mkdir(parents=True, exist_ok=True)

    consumer = criar_consumer()
    consumer.subscribe([TOPIC_NAME])
    print(f"[auditoria] consumindo topico '{TOPIC_NAME}' (grupo={GROUP_ID})")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[auditoria] erro no consumo: {msg.error()}")
                continue

            pedido = json.loads(msg.value())
            processar(pedido, msg)
    except KeyboardInterrupt:
        print("[auditoria] encerrando...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
