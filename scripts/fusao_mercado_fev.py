
from processamento_dados import Dados

import json
import csv
   

path_json = './data_raw/dados_empresaA.json'
path_csv = './data_raw/dados_empresaB.csv'

#Extract

print("--------------------------------------------------")
print("Iniciando leitura...")

dados_empresaA = Dados.leitura_dados(path_json, 'json')
print(f"### EMPRESA A ###")
print(f"Nome colunas dados: {dados_empresaA.nome_colunas}")
print(f"Quantidade de linhas: {dados_empresaA.qtd_linhas}")

dados_empresaB = Dados.leitura_dados(path_csv, 'csv')
print(f"### EMPRESA B ###")
print(f"Nome colunas dados: {dados_empresaB.nome_colunas}")
print(f"Quantidade de linhas: {dados_empresaB.qtd_linhas}")

print("--------------------------------------------------")

#Transform

print("Transformação dos dados...")

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}


dados_empresaB.rename_columns(key_mapping)
print(f"Nome colunas modificados conforme o key_mapping: {dados_empresaB.nome_colunas}")

print("### FUSÃO DADOS ###")
dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f"Nome colunas dados: {dados_fusao.nome_colunas}")
print(f"Quantidade de linhas: {dados_fusao.qtd_linhas}")


print("--------------------------------------------------")

#Load
print("Salvando os dados...")
path_dados_combinados = './data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(f"Os dados foram exportados para o diretorio: {path_dados_combinados}")


