# Servico: consumer_estoque — reserva a quantidade pedida no inventario.
#
# Build MULTI-STAGE: os testes rodam dentro do proprio build, no mesmo
# ambiente do runtime, e a imagem final nao leva nada de teste.
#
# O contexto de build e a RAIZ do projeto (para enxergar o requirements.txt):
#   docker build -f consumer_estoque/Dockerfile -t protocolo/estoque .
#   docker build -f consumer_estoque/Dockerfile --target test .   # so os testes

# --------------------------------------------------------------------- base
# Estagio comum: o ambiente Python com as dependencias. Os dois estagios
# seguintes partem daqui, entao o pip install acontece UMA vez.
FROM python:3.12-slim AS base

# PYTHONUNBUFFERED: sem isso os print() ficam presos no buffer e so aparecem
# quando o container morre — o "docker compose logs -f" fica mudo.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------------------------- test
# Aqui entra o codigo E os testes. O RUN executa a suite durante o build:
# se um cenario falhar, o exit code != 0 aborta o build inteiro.
#
# Os testes usam dubles no lugar do Consumer e do Producer, entao nao
# precisam de um broker no ar — o que os torna viaveis dentro de um build.
FROM base AS test

COPY consumer_estoque/consumer.py consumer_estoque/teste_dlq.py ./

RUN python teste_dlq.py

# ------------------------------------------------------------------ runtime
# A imagem que realmente sobe. Note que ela NAO copia do contexto de build:
# copia do estagio de teste.
FROM base AS runtime

# COPY --from=test faz duas coisas de uma vez:
#
#   1. Cria a dependencia que obriga o estagio "test" a ser construido antes
#      deste. Sem ela o BuildKit pula o estagio de teste (ele so constroi o
#      que o alvo final precisa), e a suite nunca rodaria.
#
#   2. Garante que o arquivo que sobe e exatamente o que passou nos testes,
#      e nao uma copia possivelmente diferente vinda do contexto.
#
# O teste_dlq.py fica para tras: ele existiu so no estagio anterior.
COPY --from=test /app/consumer.py ./

RUN useradd --create-home --uid 1000 app && chown -R app:app /app
USER app

# ENTRYPOINT + CMD combinados. Os outros 5 servicos usam so CMD; aqui a
# combinacao esta de proposito, para o repositorio ter um exemplo dos dois
# padroes lado a lado.
#
# ENTRYPOINT fixa o executavel, CMD da o argumento padrao. Argumentos passados
# ao "docker run" substituem o CMD, nunca o ENTRYPOINT:
#
#   docker run img                          -> python consumer.py
#   docker run img -c "print(1+1)"          -> python -c "print(1+1)"
#
# O ganho pratico e poder rodar qualquer coisa em Python contra a imagem, com
# o mesmo ambiente do servico - util para depurar:
#
#   docker run --rm protocolo/estoque:dev -c "import consumer; print(consumer.ESTOQUE)"
#
# O custo: comandos que nao sao Python precisam trocar o entrypoint, porque
# senao viram argumento do interpretador. Para listar arquivos, por exemplo:
#
#   docker run --rm --entrypoint sh protocolo/estoque:dev -c "ls -la /app"
ENTRYPOINT ["python"]
CMD ["consumer.py"]
