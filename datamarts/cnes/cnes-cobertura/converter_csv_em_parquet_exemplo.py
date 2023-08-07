import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

STARTS_WITH = 'rl'

# Diretório atual
diretorio_atual = os.getcwd()

# Lista de arquivos CSV que começam com 'tb' ou 'rl'
arquivos_csv = [arquivo for arquivo in os.listdir(diretorio_atual) if arquivo.startswith(STARTS_WITH)]

# Loop através dos arquivos CSV
for arquivo_csv in arquivos_csv:
    # Lê o arquivo CSV usando o pandas
    print("arquivo: " + arquivo_csv)
    df = pd.read_csv(arquivo_csv, sep=";")
    
    # Define o nome do arquivo Parquet
    arquivo_parquet = os.path.splitext(arquivo_csv)[0] + '.parquet'
    
    # Converte o DataFrame para um formato Parquet
    table = pa.Table.from_pandas(df)
    
    # Escreve o arquivo Parquet
    pq.write_table(table, arquivo_parquet)
    
    print(f'Arquivo {arquivo_csv} convertido para {arquivo_parquet}')
    
    