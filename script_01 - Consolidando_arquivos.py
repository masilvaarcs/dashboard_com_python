# Importar os pacotes/bibliotecas/libs

import pandas as pd
import numpy as np
import warnings
import sqlite3
# Acessar componentes do SISTEMA OPERACIONAL
import os
from pathlib import Path

warnings.filterwarnings('ignore')


# Local diretório
local_diretorio = 'C:/PythonProjetos/Dashboard_com_Python/dados'
Path(local_diretorio)
diretorio_path = Path(local_diretorio)
# print(type(diretorio_path))

# Lista os arquivos
arquivos = diretorio_path.iterdir()


def bytes_megabytes(vr_bytes):
    megabytes = vr_bytes / (1024 * 1024)
    return round(megabytes, 2)


total_mb = 0
gb_loop = ''
base_consolidada = ''
# Loop
for loop in arquivos:
    print(loop)

    #  buscando o tamanho dos arquivos
    tamanho = os.path.getsize(loop)

    # Convertendo em Megas
    conversao = bytes_megabytes(tamanho)

    # Extraindo o nome do arquivo
    nome_arquivo = str(loop).split('\\')[-1]

    print(f'{nome_arquivo}: {conversao} MB')
    print('-' * 30)

    total_mb += tamanho
    gb_loop = loop

print(f'Temos ao todo {bytes_megabytes(total_mb)} MBs de dados')
pd.read_csv(gb_loop, on_bad_lines='skip', sep=';', encoding='latin-1')
print('')

# Loop para consolidar os arquivos
# Lista os arquivos
arquivos = diretorio_path.iterdir()

for contador, arq in enumerate(arquivos):
    print(contador, arq)

    # Se for a 1º interação, cria a base consolidada
    if contador == 0:
        # Vai ler o arquivo do Loop (arq)
        base_dados = pd.read_csv(arq, on_bad_lines='skip', sep=';', encoding='latin-1')
        # Vai salvar na variável
        base_consolidada = base_dados
    else:
        # Vai ler o arquivo do Loop (arq)
        base_dados = pd.read_csv(arq, on_bad_lines='skip', sep=';', encoding='latin-1')
        # Vai salvar na variável
        base_consolidada = pd.concat([base_consolidada, base_dados])
        # Para testes de somente alguns meses
        # if contador == 2:
        #     # Dimensão
        #     # base_consolidada.shape
        #     # Exportação, CSV
        #     base_consolidada.to_csv('base_consolidada.csv', index=False)
        #     print('Exportação realizada para a base Consolidada!')
        #     break


# Dimensão
# base_consolidada.shape
# Exportação, CSV
base_consolidada.to_csv('base_consolidada.csv', index=False)
print('Exportação realizada para a base Consolidada!')

# Criar o banco de dados
conexao = sqlite3.connect('banco_dados.db')

# Nome da tabela
nome_tabela = 'base_consolidada'
# Exportando para sqllite3
base_consolidada.to_sql(nome_tabela, conexao, if_exists='append', index=False)
print('Exportado para sqlite3')

# Convertendo tudo para string para evitar erros na exportação
for col in base_consolidada.columns:
    base_consolidada[col] = base_consolidada[col].astype(str)

# Exportando para Parquet (para melhorar o processamento)
base_consolidada.to_parquet('base_consolidada.parquet', index=False, compression='gzip')
print('Exportado para Parquet')
