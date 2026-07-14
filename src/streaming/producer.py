"""Producer de streaming: reamostra alunos da Bronze e envia como eventos para o SQS."""

import argparse
import io
import json
import math
import time

PROFILE = "fiap-tech-challenge"
REGION = "us-east-1"
BUCKET = "fiap-tc2-286958704145"
QUEUE_NAME = "alfabetizacao-eventos"
SOURCE_KEY = "bronze/alunos/ano=2024/alunos.parquet"


def carregar_amostra(s3, n):
    import pandas as pd

    obj = s3.get_object(Bucket=BUCKET, Key=SOURCE_KEY)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    amostra = df.sample(n=min(n, len(df))).astype(object)
    amostra = amostra.where(pd.notna(amostra), None)
    return amostra.to_dict(orient="records")


def serializar_evento(evento):
    """Converte escalares do pandas/numpy e impede JSON com NaN ou infinito."""
    corpo = {}
    for chave, valor in evento.items():
        if hasattr(valor, "item"):
            valor = valor.item()
        if isinstance(valor, float) and not math.isfinite(valor):
            valor = None
        corpo[chave] = valor
    return corpo, json.dumps(corpo, ensure_ascii=False, allow_nan=False)


def main():
    import boto3

    parser = argparse.ArgumentParser()
    parser.add_argument("--eventos", type=int, default=20, help="quantos eventos enviar")
    parser.add_argument("--intervalo", type=float, default=1.0, help="segundos entre eventos")
    args = parser.parse_args()

    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    s3 = session.client("s3")
    sqs = session.client("sqs")

    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    eventos = carregar_amostra(s3, args.eventos)

    print(f"Enviando {len(eventos)} eventos para {QUEUE_NAME}")
    for i, evento in enumerate(eventos, 1):
        corpo, mensagem = serializar_evento(evento)
        sqs.send_message(QueueUrl=queue_url, MessageBody=mensagem)
        print(f"  [{i}/{len(eventos)}] aluno {corpo.get('id_aluno')} enviado")
        time.sleep(args.intervalo)

    print("Producer concluido.")


if __name__ == "__main__":
    main()
