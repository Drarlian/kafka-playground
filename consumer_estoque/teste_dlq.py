"""Testes do fluxo de erro e DLQ do consumer_estoque, sem precisar do Kafka.

Exercita a funcao tratar() usando dubles no lugar da mensagem do Kafka e do
producer da DLQ, cobrindo os caminhos descritos em CONCEITOS-KAFKA.md:

    - sucesso                       -> commit, nada na DLQ
    - erro permanente               -> DLQ na hora, sem repetir
    - JSON invalido                 -> DLQ com zero tentativas
    - erro transitorio persistente  -> esgota as tentativas -> DLQ
    - erro transitorio que passa    -> sucesso no retry, nada na DLQ
    - DLQ nao confirma a escrita    -> NAO commita (mensagem volta)
    - idempotencia                  -> mesmo pedido_id debita uma vez so

Script sem dependencia de framework, pelo mesmo motivo do resto do projeto:
roda com o que ja esta no requirements.txt.

    .venv\\Scripts\\python consumer_estoque\\teste_dlq.py
"""

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import consumer as est  # noqa: E402  (precisa do sys.path acima)

CONSULTAR_ORIGINAL = est.consultar_inventario
ESTOQUE_ORIGINAL = dict(est.ESTOQUE)


class FakeMsg:
    """Substitui a Message do confluent_kafka."""

    def __init__(self, payload, offset=42, particao=1):
        self._valor = (
            payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        )
        self._offset = offset
        self._particao = particao

    def value(self):
        return self._valor

    def key(self):
        return b"chave-de-teste"

    def topic(self):
        return "reservas-pedido"

    def partition(self):
        return self._particao

    def offset(self):
        return self._offset


class FakeDLQ:
    """Substitui o Producer da DLQ.

    `pendentes` simula um flush que nao confirmou a escrita: e assim que o
    confluent_kafka sinaliza que a mensagem continua no buffer local.
    """

    def __init__(self, pendentes=0):
        self.enviadas = []
        self._pendentes = pendentes

    def produce(self, topic, key, value, headers):
        self.enviadas.append(headers)

    def flush(self, timeout):
        return self._pendentes


def pedido(produto="mouse-gamer", quantidade=2):
    return {
        "pedido_id": str(uuid.uuid4()),
        "produto": produto,
        "quantidade": quantidade,
    }


def resetar():
    """Devolve o modulo ao estado inicial entre um cenario e outro."""
    est.consultar_inventario = CONSULTAR_ORIGINAL
    est.ESTOQUE.clear()
    est.ESTOQUE.update(ESTOQUE_ORIGINAL)
    est.PEDIDOS_APLICADOS.clear()
    est.FALHA_TRANSITORIA_PCT = 0
    est.MAX_TENTATIVAS = 3
    est.time.sleep = lambda s: None  # nao esperar o backoff de verdade


CENARIOS = []


def cenario(descricao):
    def registrar(fn):
        CENARIOS.append((descricao, fn))
        return fn

    return registrar


@cenario("sucesso: commita e nao usa a DLQ")
def teste_sucesso():
    dlq = FakeDLQ()
    assert est.tratar(FakeMsg(pedido()), dlq) is True
    assert dlq.enviadas == []


@cenario("erro permanente: vai direto para a DLQ, sem repetir")
def teste_erro_permanente():
    dlq = FakeDLQ()
    assert est.tratar(FakeMsg(pedido(produto="produto-fantasma")), dlq) is True
    assert len(dlq.enviadas) == 1
    assert dlq.enviadas[0]["erro_tipo"] == "ErroPermanente"
    assert dlq.enviadas[0]["tentativas"] == "1"


@cenario("JSON invalido: DLQ com zero tentativas")
def teste_json_invalido():
    dlq = FakeDLQ()
    assert est.tratar(FakeMsg(b"{isso nao e json"), dlq) is True
    assert len(dlq.enviadas) == 1
    assert dlq.enviadas[0]["erro_tipo"] == "JSONDecodeError"
    assert dlq.enviadas[0]["tentativas"] == "0"


@cenario("erro transitorio persistente: esgota as tentativas e vai para a DLQ")
def teste_transitorio_esgota():
    est.FALHA_TRANSITORIA_PCT = 100
    dlq = FakeDLQ()
    assert est.tratar(FakeMsg(pedido()), dlq) is True
    assert len(dlq.enviadas) == 1
    assert dlq.enviadas[0]["erro_tipo"] == "ErroTransitorio"
    assert dlq.enviadas[0]["tentativas"] == "3"


@cenario("erro transitorio que passa no retry: nao usa a DLQ")
def teste_transitorio_recupera():
    chamadas = {"n": 0}

    def inventario_instavel(produto):
        chamadas["n"] += 1
        if chamadas["n"] == 1:
            raise est.ErroTransitorio("timeout simulado")
        return est.ESTOQUE.get(produto)

    est.consultar_inventario = inventario_instavel

    dlq = FakeDLQ()
    assert est.tratar(FakeMsg(pedido()), dlq) is True
    assert dlq.enviadas == []
    assert chamadas["n"] == 2, "deveria ter tentado duas vezes"


@cenario("DLQ nao confirma a escrita: offset NAO pode avancar")
def teste_dlq_nao_confirma():
    est.FALHA_TRANSITORIA_PCT = 100
    dlq = FakeDLQ(pendentes=1)
    assert est.tratar(FakeMsg(pedido()), dlq) is False, (
        "sem confirmacao da DLQ o commit tem de ser bloqueado"
    )


@cenario("idempotencia: o mesmo pedido_id debita o estoque uma vez so")
def teste_idempotencia():
    p = pedido(produto="monitor-27-4k", quantidade=5)
    antes = est.ESTOQUE["monitor-27-4k"]

    dlq = FakeDLQ()
    est.tratar(FakeMsg(p), dlq)
    est.tratar(FakeMsg(p), dlq)

    debitado = antes - est.ESTOQUE["monitor-27-4k"]
    assert debitado == 5, f"debitou {debitado}, esperado 5"


def main():
    falhas = []

    for descricao, fn in CENARIOS:
        print(f"\n--- {descricao}")
        resetar()
        try:
            fn()
            print(f"    OK")
        except AssertionError as exc:
            falhas.append((descricao, exc))
            print(f"    FALHOU: {exc}")

    total = len(CENARIOS)
    print(f"\n{total - len(falhas)}/{total} cenarios passaram")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
