# =============================================================================
# config.py — Constantes do pipeline RFB
# =============================================================================

# Todas as colunas do CSV original — usadas na leitura para mapear posições
COLS_ESTABELECIMENTO_RAW = [
    "CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV", "MATRIZ_FILIAL",
    "NOME_FANTASIA", "SITUACAO_CADASTRAL", "DATA_SITUACAO_CADASTRAL",
    "MOTIVO_SITUACAO_CADASTRAL", "NOME_CIDADE_EXTERIOR", "PAIS",
    "DATA_INICIO_ATIVIDADES", "CNAE_PRINCIPAL", "CNAE_SECUNDARIA",
    "TIPO_LOGRADOURO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "BAIRRO",
    "CEP", "UF", "MUNICIPIO", "DDD1", "TELEFONE_1", "DDD2", "TELEFONE_2",
    "DDD_FAX", "FAX", "EMAIL", "SITUACAO_ESPECIAL", "DATA_SITUACAO_ESPECIAL",
]

# Colunas mantidas após limpeza — descarta dados irrelevantes para os outputs
COLS_ESTABELECIMENTO = [
    "CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV", "MATRIZ_FILIAL",
    "NOME_FANTASIA", "SITUACAO_CADASTRAL", "DATA_SITUACAO_CADASTRAL",
    "MOTIVO_SITUACAO_CADASTRAL", "DATA_INICIO_ATIVIDADES",
    "CNAE_PRINCIPAL", "CNAE_SECUNDARIA",
    "TIPO_LOGRADOURO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "BAIRRO",
    "CEP", "UF", "MUNICIPIO", "DDD1", "TELEFONE_1",
]

# Colunas descartadas:
#   NOME_CIDADE_EXTERIOR, PAIS          — empresas estrangeiras (irrelevante)
#   DDD2, TELEFONE_2, DDD_FAX, FAX      — contatos secundários
#   EMAIL                               — não utilizado nos outputs
#   SITUACAO_ESPECIAL, DATA_SITUACAO_ESPECIAL — irrelevante para ativos

COLS_EMPRESA = [
    "CNPJ_BASICO", "RAZAO_SOCIAL", "NATUREZA_JURIDICA",
    "QUALIFICACAO_RESPONSAVEL", "CAPITAL_SOCIAL", "PORTE", "ENTE_FEDERATIVO",
]

# Padrão do nome dos arquivos ZIP na Receita Federal
FILE_TYPES = {
    "ESTABELE": COLS_ESTABELECIMENTO,
    "EMPRE": COLS_EMPRESA,
}

# Tabela de mapeamento SIAFI ↔ IBGE (relativa à raiz do projeto)
SIAFI_MAP_PATH = "data/Municipios_Parquet_Siafi.csv"
