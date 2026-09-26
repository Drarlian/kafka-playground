# Servico: producer — publica reservas de pedido no topico Kafka.
#
# O contexto de build e a RAIZ do projeto (para enxergar o requirements.txt):
#   docker build -f producer/Dockerfile -t protocolo/producer .
FROM python:3.12-slim

# PYTHONUNBUFFERED: sem isso os print() ficam presos no buffer e so aparecem
# quando o container morre — o "docker compose logs -f" fica mudo.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencias primeiro: essa camada so e reconstruida quando o
# requirements.txt muda, nao a cada alteracao no codigo.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/producer.py ./

RUN useradd --create-home --uid 1000 app && chown -R app:app /app
USER app

CMD ["python", "producer.py"]
