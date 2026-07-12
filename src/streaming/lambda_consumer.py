"""Consumer de streaming (AWS Lambda): grava eventos do SQS na Bronze."""

import time

import boto3

BUCKET = "fiap-tc2-286958704145"
PREFIX = "bronze/streaming"

s3 = boto3.client("s3")


def handler(event, context):
    registros = event.get("Records", [])
    gravados = 0

    for registro in registros:
        corpo = registro["body"]
        message_id = registro.get("messageId", str(time.time()))
        key = f"{PREFIX}/dt_ingestao={time.strftime('%Y-%m-%d')}/{message_id}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=corpo.encode("utf-8"))
        gravados += 1

    print(f"{gravados} eventos gravados em s3://{BUCKET}/{PREFIX}/")
    return {"gravados": gravados}
