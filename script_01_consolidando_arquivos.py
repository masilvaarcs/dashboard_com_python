"""
Roteiro que foi criado no DIA 1: INGESTÃO DOS DADOS → Aprendendo como consolidar e empilhar
dados com Python.
"""
import warnings
import os
from pathlib import Path
import sqlite3
import pandas as pd

warnings.filterwarnings("ignore")


def limpar_arquivos_anteriores(diretorio, extensoes):
    """
    Verifica e exclui os arquivos com as extensões fornecidas no diretório especificado.

    Args:
        diretorio (str): O caminho do diretório onde os arquivos serão verificados.
        extensoes (list): Uma lista de strings contendo as extensões dos arquivos a serem
        verificados.

    Returns:
        list: A lista de arquivos atualizada após a exclusão dos arquivos com as extensões
        especificadas.
    """
    arquivos_atualizados = []
    for arquivo in os.listdir(diretorio):
        if arquivo.endswith(tuple(f".{extensao}" for extensao in extensoes)):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            os.remove(caminho_arquivo)
        else:
            arquivos_atualizados.append(arquivo)
    return arquivos_atualizados


# Definindo as extensões dos arquivos que desejo verificar e excluir
extensoes_a_serem_excluidas = ["db", "csv", "parquet"]
# Diretório que deseja utilizar como base para excluir os arquivos
LOCAL_DIRETORIO = ""

# Obtém o caminho absoluto do diretório
LOCAL_DIRETORIO_ABSOLUTO = os.path.abspath(LOCAL_DIRETORIO)

# Chama a função para limpar os arquivos anteriores antes de criar os novos
arquivos = limpar_arquivos_anteriores(
    LOCAL_DIRETORIO_ABSOLUTO, extensoes_a_serem_excluidas
)
print(
    '\n** OBS.: Todos os arquivos "db", "csv", "parquet" foram excluídos\n'
    "   para uma Nova Importação! **"
)


def bytes_megabytes(vr_bytes: int):
    """
    Transforma o valor recebido, em bytes, para megabytes.

    Args:
        vr_bytes (int): O valor em bytes a ser convertido para megabytes.

    Returns:
        float: O valor convertido em megabytes (MB).
    """
    megabytes = vr_bytes / (1024 * 1024)
    return round(megabytes, 2)


# Local diretório
LOCAL_DIRETORIO = "dados"
Path(LOCAL_DIRETORIO)
# Obtém o caminho do diretório que contém os dados de origem
diretorio_path = Path(LOCAL_DIRETORIO)
# Lista os arquivos
fonte_dados = diretorio_path.iterdir()

TOTAL_MB = 0
GB_LOOP = ""
BASE_CONSOLIDADA = ""

# Loop
for loop in fonte_dados:
    # print(loop)  # apenas para testes

    #  buscando o tamanho dos arquivos
    tamanho = os.path.getsize(loop)

    # Convertendo em Megas
    conversao = bytes_megabytes(tamanho)

    # Extraindo o nome do arquivo
    nome_arquivo = str(loop).rsplit("\\", maxsplit=1)[-1]

    # Apenas para verificação.
    # print(f"{nome_arquivo}: {conversao} MB")
    # print("-" * 30)

    TOTAL_MB += tamanho
    GB_LOOP = loop

print(f"\nTemos ao todo {bytes_megabytes(TOTAL_MB)} MBs de dados")

pd.read_csv(GB_LOOP, on_bad_lines="skip", sep=";", encoding="latin-1")
print("")

# Loop para consolidar os arquivos
# Lista os arquivos
arquivos = diretorio_path.iterdir()
print("\nAGUARDE! VOU FAZER A LEITURA AGORA...")
for contador, arq in enumerate(arquivos):
    print(" -> " + str(contador), arq)

    # Se for a 1º interação, cria a base consolidada
    if contador == 0:
        # Vai ler o arquivo do Loop (arq)
        base_dados = pd.read_csv(arq, on_bad_lines="skip", sep=";", encoding="latin-1")
        # Vai salvar na variável
        BASE_CONSOLIDADA = base_dados
    else:
        # Vai ler o arquivo do Loop (arq)
        base_dados = pd.read_csv(arq, on_bad_lines="skip", sep=";", encoding="latin-1")
        # Vai salvar na variável
        BASE_CONSOLIDADA = pd.concat([BASE_CONSOLIDADA, base_dados])

print("\nUFA! A LEITURA ACABOU AGORA...")

# Dimensão
# base_consolidada.shape
print("\n")
print("=" * 65)
print("EXPORTAÇÃO DOS DADOS EM 3 FORMATOS: CSV | DB | PARQUET")
print("=" * 65)
# Exportação, CSV
BASE_CONSOLIDADA.to_csv("base_consolidada.csv", index=False)
print(" -> Exportação para Base Consolidada:  [base_consolidada.csv]")

# Criar o banco de dados
CONEXAO = sqlite3.connect("banco_dados.db")

# Nome da tabela
NOME_TABELA = "base_consolidada"
# Exportando para sqlite3
BASE_CONSOLIDADA.to_sql(NOME_TABELA, CONEXAO, if_exists="append", index=False)
print(" -> Exportado para sqlite3:            [banco_dados.db]")

# Convertendo tudo para string para evitar erros na exportação
for col in BASE_CONSOLIDADA.columns:
    BASE_CONSOLIDADA[col] = BASE_CONSOLIDADA[col].astype(str)

# Exportando para Parquet (para melhorar o processamento)
BASE_CONSOLIDADA.to_parquet("base_consolidada.parquet", index=False, compression="gzip")
print(" -> Exportado para Parquet:            [base_consolidada.parquet]")
print("\n ** FIM desta etapa de Consolidação dos dados! **")
