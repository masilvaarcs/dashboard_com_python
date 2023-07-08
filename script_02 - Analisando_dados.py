import os
import sys
import time

import numpy as np
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go
import plotly.offline
from plotly.subplots import make_subplots

import datetime
import warnings

warnings.filterwarnings('ignore')

# Importar a base de dados
# Hora de agora
print(datetime.datetime.now())

# Carregar o Parquet
base_dados = pd.read_parquet('base_consolidada.parquet')

# Hora de agora
print(datetime.datetime.now())
print('')

# Tipo
tipo = type(base_dados)
print(tipo)
# Colunas
colunas = base_dados.columns
print(colunas)
print('')

# Informações
print(base_dados.info())
print('')

# Verificando se há dados nulos
nulos = base_dados.isnull().sum()
print('Campos Nulos: ' + str(nulos) + '\n')

# Verificando campos únicos
unicos = base_dados.nunique()
print('Campos únicos: ' + str(unicos) + '\n')

# Total de multas
total_multas = int(base_dados.shape[0])  # shape = Total de linhas e colunas
print(f'Total de multas nos 12 meses {total_multas} \n')

# -- Converter data --
# Tipo antes de converter
print('Tipo antes de converter => ' + str(base_dados['Data da Infração (DD/MM/AAAA)'].dtype))
base_dados['Data da Infração (DD/MM/AAAA)'] = pd.to_datetime(base_dados['Data da Infração (DD/MM/AAAA)'])

# Tipo Após converter
print('Tipo Após converter => ' + str(base_dados['Data da Infração (DD/MM/AAAA)'].dtype))
print('')

# Multas por dia
# multas_por_dia = base_dados['Data da Infração (DD/MM/AAAA)'].value_counts().sort_index()
# print(multas_por_dia)
multas_dia = base_dados['Data da Infração (DD/MM/AAAA)'].value_counts().sort_index().reset_index()
print(multas_dia)
print('')

# Extraindo mês e dia das datas
# multas_dia['Mes'] = pd.to_datetime(multas_dia['index']).dt.month
# multas_dia['Dia'] = pd.to_datetime(multas_dia['index']).dt.day
multas_dia['Mes'] = pd.to_datetime(multas_dia['Data da Infração (DD/MM/AAAA)']).dt.month
multas_dia['Dia'] = pd.to_datetime(multas_dia['Data da Infração (DD/MM/AAAA)']).dt.day
print('')

# Renomeando as colunas
multas_dia.columns = ['Data', 'Quantidade', 'Mês', 'Dia']

# Média móvel (agrupando a cada 7 dias e pegando a média)
multas_dia['Média_Móvel'] = multas_dia['Quantidade'].rolling(7).mean()
print('')

# Informações (vendo o quanto reduziu os dados)
print(multas_dia.info())
print('')

# Gráfico de linhas
# Usando o plotly.express ( "px" )
fig_px = px.line(
    multas_dia,
    x='Data',
    y='Quantidade',
    # Definindo o Título
    title='Gráfico de linhas - Plotly Express'
)
fig_px.show()
print('')

time.sleep(5)  # Dando um tempo para criar o segundo gráfico

# Usando o plotly.graph_objects ( "go" )
# Go
fig_go = go.Figure(
    go.Scatter(
        x=multas_dia['Data'],
        y=multas_dia['Quantidade']
    )
)
# Definindo o Título
fig_go.update_layout(title='Gráfico de linhas - Plotly Graph Objects')
fig_go.show()

time.sleep(3)  # Dando um tempo para criar o segundo gráfico

# Sistema Grid (para criar um relatório - duas colunas: 1 - Gráfico de quantidade e 2 - Gráfico de média móvel)
# Grid
grid = make_subplots(rows=1, cols=2)

# Gráfico 1
grid.add_trace(go.Scatter(
    x=multas_dia['Data'],
    y=multas_dia['Quantidade'],
    mode='lines',
    name='Quantidade'
), row=1, col=1
)
# Gráfico 2
grid.add_trace(go.Scatter(
    x=multas_dia['Data'],
    y=multas_dia['Média_Móvel'],
    mode='lines',
    name='Média Móvel'
), row=1, col=2
)
grid.update_layout(
    title='1 - G R Á F I C O    D E    Q U A N T I D A D E    X    2 - G R Á F I C O   DE   M É D I A  '
          'M Ó V E L',
    # Legendas
    showlegend=True,

    # Ajuste o Plotly
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='right',
        x=1)
)
grid.show()

time.sleep(3)  # Dando um tempo para criar o próximo gráfico

# Sistema Grid (para criar um relatório - duas colunas: 1 - Gráfico de quantidade e 2 - Gráfico de média móvel)
# Grid
grid = make_subplots(rows=1, cols=1)

# Gráfico 1
grid.add_trace(go.Scatter(
    x=multas_dia['Data'],
    y=multas_dia['Quantidade'],
    mode='lines',
    name='Quantidade'
), row=1, col=1
)
# Gráfico 2
grid.add_trace(go.Scatter(
    x=multas_dia['Data'],
    y=multas_dia['Média_Móvel'],
    mode='lines',
    name='Média Móvel'
), row=1, col=1
)
grid.update_layout(
    title='A N Á L I S E   D E   M U L T A S   D I Á R I A',
    # Legendas
    showlegend=True,

    # Ajuste o Plotly
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='right',
        x=1)
)
grid.show()

time.sleep(3)  # Dando um tempo para criar o próximo gráfico

# Boxplot
box = px.box(
    multas_dia,
    x='Mês',
    y='Quantidade',
    color='Mês',
    title='D I S T R I B U I Ç Ã O   D E   M U L T A S   P O R   M Ê S'
)
box.show()
