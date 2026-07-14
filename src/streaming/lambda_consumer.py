"""Consumer de streaming (AWS Lambda): grava eventos do SQS na Bronze."""

import json
import time

import boto3

BUCKET = "fiap-tc2-286958704145"
PREFIX = "bronze/streaming"

s3 = boto3.client("s3")


def _rejeitar_constante_invalida(valor):
    raise ValueError(f"Constante JSON invalida: {valor}")


def handler(event, context):
    registros = event.get("Records", [])
    gravados = 0

    for registro in registros:
        corpo = registro["body"]
        evento = json.loads(corpo, parse_constant=_rejeitar_constante_invalida)
        if not isinstance(evento, dict):
            raise ValueError("O corpo do evento deve ser um objeto JSON")

        ausentes = [
            campo
            for campo in ("ano", "id_municipio", "id_aluno")
            if evento.get(campo) is None
        ]
        if ausentes:
            raise ValueError(f"Campos obrigatorios ausentes no evento: {ausentes}")

        corpo_valido = json.dumps(evento, ensure_ascii=False, allow_nan=False)
        message_id = registro.get("messageId", str(time.time()))
        key = f"{PREFIX}/dt_ingestao={time.strftime('%Y-%m-%d')}/{message_id}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=corpo_valido.encode("utf-8"),
            ContentType="application/json",
        )
        gravados += 1

    print(f"{gravados} eventos gravados em s3://{BUCKET}/{PREFIX}/")
    return {"gravados": gravados}
