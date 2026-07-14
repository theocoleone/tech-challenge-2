import json
import math
import unittest

from src.streaming.producer import serializar_evento


class SerializarEventoTest(unittest.TestCase):
    def test_substitui_valores_nao_finitos_por_null(self):
        corpo, mensagem = serializar_evento({
            "id_aluno": "123",
            "proficiencia": math.nan,
            "peso_aluno": math.inf,
        })

        self.assertIsNone(corpo["proficiencia"])
        self.assertIsNone(corpo["peso_aluno"])
        self.assertEqual(json.loads(mensagem), corpo)
        self.assertNotIn("NaN", mensagem)
        self.assertNotIn("Infinity", mensagem)

    def test_preserva_valores_validos(self):
        corpo, mensagem = serializar_evento({
            "ano": 2024,
            "id_aluno": "123",
            "alfabetizado": "1",
            "proficiencia": 812.5,
        })

        self.assertEqual(json.loads(mensagem), corpo)


if __name__ == "__main__":
    unittest.main()
