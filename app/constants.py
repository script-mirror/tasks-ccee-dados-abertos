from middle.utils import Constants

constants = Constants()


MAPEAMENTO_CVU = {
    "conjuntural": {
        "nome_ccee": "custo_variavel_unitario_conjuntural",
        "resource": "9d321849-e9ee-453d-a20c-ba3a419c55de",
        "url": "https://pda-download.ccee.org.br/"
               "slDZ6F4LST25wuXDGcEKJw/content",
        "columns": {"codigo_modelo_preco": int,
                    "mes_referencia": str,
                    "cvu_conjuntural": float,
                    "cnpj_agente_vendedor": str,
                    },
        "endpoint": f"{constants.BASE_URL}/api/v2/decks/cvu",
    },
    "estrutural": {
        "nome_ccee": "custo_variavel_unitario_estrutural",
        "resource": "8c388d72-f53f-4361-a216-ab342a2ab496",
        "url": "https://pda-download.ccee.org.br/"
               "7yZKFu5JRzys_02q33pCow/content",
        "columns": {"codigo_modelo_preco": int,
                    "mes_referencia": str,
                    "cvu_estrutural": float,
                    "ano_horizonte": int,
                    "codigo_parcela_usina": str,
                    },
        "endpoint": f"{constants.BASE_URL}/api/v2/decks/cvu",
    },

    "conjuntural_revisado": {
        "nome_ccee": "custo_variavel_unitario_conjuntural_revisado",
        "resource": "909b5f98-78f5-49db-8716-8f340503383a",
        "url": "https://pda-download.ccee.org.br/"
               "Rjmj5m8KQDWdZyZZp3EnPg/content",
        "columns": {"codigo_modelo_preco": int,
                    "mes_referencia": str,
                    "cvu_conjuntural": float,
                    "cnpj_agente_vendedor": str,
                    },
        "endpoint": f"{constants.BASE_URL}/api/v2/decks/cvu",
    },

    "merchant": {
        "nome_ccee": "custo_variavel_unitario_merchant",
        "resource": "74e19119-ccc4-45c5-8e07-d34d62572b03",
        "url": "https://pda-download.ccee.org.br/"
               "5UzVaEaqQXqDie5Ev7gD5g/content",
        "columns": {"codigo_modelo_preco": int,
                    "mes_referencia": str,
                    "cvu_cf": float,
                    "cvu_scf": float,
                    "mes_referencia_cotacao": str,
                    },
        "endpoint": f"{constants.BASE_URL}/api/v2/decks/cvu/merchant",
    },

}
