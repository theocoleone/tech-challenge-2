"""Log estruturado de metricas por etapa (volume, latencia, status) em JSON."""

import json
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
_log = logging.getLogger("pipeline")


class Etapa:
    """Cronometra uma etapa e loga o resultado em JSON ao sair do bloco."""

    def __init__(self, nome, **contexto):
        self.nome = nome
        self.contexto = contexto
        self.linhas = None

    def __enter__(self):
        self.inicio = time.time()
        return self

    def __exit__(self, exc_type, exc, tb):
        registro = {
            "etapa": self.nome,
            "status": "erro" if exc_type else "sucesso",
            "duracao_s": round(time.time() - self.inicio, 2),
            "linhas": self.linhas,
            **self.contexto,
        }
        if exc_type:
            registro["erro"] = str(exc)
        _log.info(json.dumps(registro, ensure_ascii=False))
        return False  # nao suprime a excecao
