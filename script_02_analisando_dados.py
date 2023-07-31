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
multas_dia.columns = ['Data', 'Quantidade', 'Mes', 'Dia']

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
# -------------- fig_px.show()
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
# -------------- fig_go.show()

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
# -------------- grid.show()

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
# -------------- grid.show()

time.sleep(3)  # Dando um tempo para criar o próximo gráfico

# Boxplot
box = px.box(
    multas_dia,
    x='Mes',
    y='Quantidade',
    color='Mes',
    title='D I S T R I B U I Ç Ã O   D E   M U L T A S   P O R   M Ê S'
)
# -------------- box.show()

print('\n')

# Estados - Entendendo quais as faixas/quantidades por estado
acumulado_estados = base_dados['UF Infração'].value_counts()
titulo = '-- A C U M U L A D O  E S T A D O S --'
print('=' * titulo.__len__())
print(titulo)
print('=' * titulo.__len__())
print(acumulado_estados)
print('\n')

# Pegando todos os valores e dividindo pela quantidade relativa - pra saber por estado
acumulado_estados_percentual = base_dados['UF Infração'].value_counts(normalize=True)
titulo = '-- A C U M U L A D O  E S T A D O S - P E R C E N T U A L--'
print('=' * titulo.__len__())
print(titulo)
print('=' * titulo.__len__())
print(acumulado_estados_percentual)
print('\n')

# Pega todos os valores e vai somando para ter uma ideia quanto no geral representam
# MG    0.108492
# RJ    0.209805
# BA    0.310661 (representa o somatório de MG + RJ))
# Pode ser usado para fazer um TOP 5 por exemplo
acumulado_estados_acumulado = base_dados['UF Infração'].value_counts(normalize=True).cumsum()
titulo = '-- A C U M U L A D O  E S T A D O S - A C U M U L A N D O--'
print('=' * titulo.__len__())
print(titulo)
print('=' * titulo.__len__())
print(acumulado_estados_acumulado)

print('\n')

# Dicionário com os dados
dicionario = {
    'Estados': acumulado_estados.index,
    'Qtd Multas': acumulado_estados.values,
    'Representação': acumulado_estados_percentual.values,
    'Acumulado': acumulado_estados_acumulado.values
}
# Crio um DataFrame para apresentar os dados
tabela_estados = pd.DataFrame(dicionario)
titulo = '-- A N Á L I S E  C R I A D A  --'
print('=' * titulo.__len__())
print(titulo)
print('=' * titulo.__len__())
print(tabela_estados)

print('\n')

# Montando um gráfico de funil pra representar
# os estados com total de 50% das infrações
# Gráfico de funil
graf_funil = px.funnel(
    tabela_estados[tabela_estados['Acumulado'] < 0.5],
    y='Estados',
    x='Qtd Multas',
    title='Concentração dos 50%'
)
# -------------- graf_funil.show()

print('\n')

# Gráfico de Barras
graf_barras = px.bar(
    tabela_estados,
    x='Estados',
    y='Acumulado',
    title='Multas acumuladas por estado'
)

# -------------- graf_barras.show()

print('\n')

# Gráfico de Calor - Parte 1
# Vou criar uma coluna Mês para montar o gráfico
base_dados['Mes'] = base_dados['Data da Infração (DD/MM/AAAA)'].dt.month
print(base_dados.head())

print('\n')

# Análise
analise_estado_mes = base_dados.groupby(by=['Mes', 'UF Infração']).agg(
    Quantidade=('Município', 'count')
).reset_index()

print(analise_estado_mes.info)

print('\n')

# Montando uma Tabela Dinâmica
tab_pivot = analise_estado_mes.pivot_table(
    index='Mes',
    columns='UF Infração',
    values='Quantidade'
)
titulo = '-- A N Á L I S E  P I V O T  T A B L E  --'
print('=' * titulo.__len__())
print(titulo)
print('=' * titulo.__len__())
print(tab_pivot)

print('\n')

# Gráfico de Calor - Parte 2 (Final)

graf_calor = px.imshow(
    tab_pivot,
    title='Mapa de calor | Multas mensais por estado em 2022'
)

# -------------- graf_calor.show()

print('\n')

# Tipo da Multa
tipo_multa = base_dados['Enquadramento da Infração'].value_counts(normalize=True).cumsum() * 100
titulo = '-- A N Á L I S E  T I P O  D A  M U L T A  --'
print('=' * titulo.__len__())
print(titulo)
print('=' * titulo.__len__())
print(tipo_multa)

print('\n')

# Fazendo a busca dos valores pelo Tipo da multa
# a partir do site do detran de Minas Gerais
path_url = 'https://www.detran.mg.gov.br/infracoes/consultar-tipos-infracoes/index/index/lista-de-infracoes?page=2'
tabela_valores = pd.read_html(path_url)[0]
# Loop no site do Detran e buscar os dados
# url = 'https://www.detran.mg.gov.br/infracoes/consultar-tipos-infracoes/index/index/lista-de-infracoes'
path_url = 'https://www.detran.mg.gov.br/infracoes/consultar-tipos-infracoes/index/index/lista-de-infracoes?page='

base_consolidada_valores = pd.DataFrame()

for Loop in range(1, 24):
    link = f'{path_url}{Loop}'
    # print(link, '\n')

    # Lendo os dados da web
    dados_web = pd.read_html(link)[0]

    # Consolidando os dados
    base_consolidada_valores = pd.concat([base_consolidada_valores, dados_web])

print('\n')

dados_shape = base_consolidada_valores.shape
print(dados_shape)

print('\n')

# Ajustando a coluna [Valor]
# pd.to_numeric(base_consolidada_valores['Valor'], errors='ignore')


# A LINHA DE CÓDIGO ABAIXO NÃO RODA OK NO PYCHARM (USANDO PYTHON 3.11) MAS NO JUPYTER RODA OK
base_consolidada_valores['Valor'] = base_consolidada_valores['Valor'] / 100
# -------------------------------------------------------------------------
print(base_consolidada_valores['Valor'])
print('\n')


# Resolvendo alguns problemas e diferenças
# Renomeando o nome da coluna [Código] para [Código da Infração]
# para ficar com o mesmo nome da coluna de origem
base_consolidada_valores.rename(columns={'Código': 'Código da Infração'}, inplace=True)
# Um código pode aparecer mais de uma vez, mas representar multas diferentes,
# mas possuem o mesmo valor de multa (mas a condição em que a multa foi aplicada foi diferente)
qtde_por_codigo_infracao = base_consolidada_valores['Código da Infração'].value_counts()
print('Quantidade por Tipo de Infração - Mesmo Código')
print(qtde_por_codigo_infracao)

# Ajustando os dados de multas
tab_preco = base_consolidada_valores.groupby(
    by=['Código da Infração']
).mean(numeric_only=True).reset_index()

print('Tabela Preço')
print(tab_preco)

# Reencresver a coluna de Infração
base_consolidada_valores['Código da Infração'] = base_consolidada_valores['Código da Infração'].apply(
    lambda loop: loop[0:4])

# Converto para numero o codigo
base_consolidada_valores['Código da Infração'] = pd.to_numeric(base_consolidada_valores['Código da Infração'])

# Cruzando os dados
Cruzamento = pd.merge(base_consolidada_valores, tab_preco, on='Código da Infração', how='left')
Cruzamento.head()

# Analise por UF e Preço
Tab_Soma = Cruzamento.groupby(by='UF Infração').agg(
    {'Valor': ['count', 'sum']},
)

Tab_Soma.columns = Tab_Soma.columns.droplevel()

Tab_Soma = Tab_Soma.reset_index()

Tab_Soma.head()

px.scatter(
    Tab_Soma,
    x='count',
    y='sum',
    color='UF Infração',
    size='count',
    log_x=True,
    size_max=60,
    title='Bubble PLOT')

fig = px.scatter(Tab_Soma, x='count', y='sum', color='UF Infração', title='Scatter PLOT')
fig.update_traces(marker=dict(size=12))

Analise_Valor_Mes = Cruzamento.groupby(by=['Mes'])['Valor'].sum().reset_index()
print(Analise_Valor_Mes)

print('\n')
