# Servico: consumer_notificacao_interna — alerta a equipe de vendas/logistica.
#
# O contexto de build e a RAIZ do projeto (para enxergar o requirements.txt):
#   docker build -f consumer_notificacao_interna/Dockerfile -t protocolo/notificacao-interna .
FROM python:3.12-slim

# PYTHONUNBUFFERED: sem isso os print() ficam presos no buffer e so aparecem
# quando o container morre — o "docker compose logs -f" fica mudo.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer_notificacao_interna/consumer.py ./

RUN useradd --create-home --uid 1000 app && chown -R app:app /app
USER app

CMD ["python", "consumer.py"]
