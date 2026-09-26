import json
import os
import time

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "reservas-pedido")
GROUP_ID = os.environ.get("GROUP_ID", "grupo-notificacao-cliente")


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
            print(f"[notificacao-cliente] conectado ao Kafka em {BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as exc:
            print(
                f"[notificacao-cliente] tentativa {tentativa}/{max_tentativas} "
                f"falhou ao conectar no Kafka: {exc}"
            )
            time.sleep(3)
    raise RuntimeError("nao foi possivel conectar ao Kafka")


def processar(pedido: dict) -> None:
    print(
        f"[notificacao-cliente] enviando e-mail/SMS para {pedido['cliente_id']}: "
        f"sua reserva do pedido {pedido['pedido_id']} ({pedido['quantidade']}x "
        f"{pedido['produto']}, R$ {pedido['valor_total']:.2f}) foi recebida "
        f"e esta sendo processada."
    )


def main():
    consumer = criar_consumer()
    consumer.subscribe([TOPIC_NAME])
    print(f"[notificacao-cliente] consumindo topico '{TOPIC_NAME}' (grupo={GROUP_ID})")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[notificacao-cliente] erro no consumo: {msg.error()}")
                continue

            pedido = json.loads(msg.value())
            processar(pedido)
    except KeyboardInterrupt:
        print("[notificacao-cliente] encerrando...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
