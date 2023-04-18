import glob
import os
from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
class Coluna:
    nome: str
    tipo: str
    descricao: str

    def get_dtype(self):
        return {self.nome: self.tipo}


@dataclass
class EstabelecimentoColuna:
    CODMUNOCOR = Coluna(
        nome="CODMUNOCOR",
        tipo="str",
        descricao="Código do Município de ocorrência"
    )
    DTOBITO = Coluna(
        nome="DTOBITO",
        tipo="str",
        descricao="Data do Óbito. Data no padrão ddmmaaaa"
    )
    CAUSABAS = Coluna(
        nome="CAUSABAS",
        tipo="str",
        descricao="Códigos CID 10. Causa básica da DO"
    )


def parse_date(date_string):
    if len(str(date_string)) == 7:
        date_string = "0" + str(date_string)
    return datetime.strptime(str(date_string), '%d%m%Y')


def sumarizar_arquivos():
    all_files = glob.glob("bronze/*.csv")
    dfs = []
    for filename in all_files:
        df = pd.read_csv(filename)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    df.dropna(subset=['CAUSABAS'], inplace=True)
    filtro = df['CAUSABAS'].str.startswith(('F10', 'F11', 'F12', 'F13', 'F14', 'F15', 'F16', 'F18', 'F19'))
    df = df.loc[filtro]

    df['CAUSABAS'] = df['CAUSABAS'].str[:3]
    df['DTOBITO'] = df['DTOBITO'].apply(parse_date)

    df['MES'] = df['DTOBITO'].dt.month
    df['ANO'] = df['DTOBITO'].dt.year

    df = df.rename(columns={'CODMUNOCOR': 'MUNICIPIO_CODIGO'})

    df['MES_ANO'] = df.apply(lambda x: '{:02d}-{}'.format(x['MES'], x['ANO']), axis=1)

    result = df.groupby(['MUNICIPIO_CODIGO', 'MES_ANO', 'CAUSABAS'])['DTOBITO'].count().reset_index(name='TOTAL_OBITOS')

    result.to_csv('gold/sim-2020.csv', sep=';', index=False)

    print('Arquivo salvo com sucesso!')


def agrupar_arquivos():
    # Set the path to the directory containing the CSV files
    path = 'silver/'

    # Get a list of all the CSV files in the directory that start with 'sim'
    csv_files = [f for f in os.listdir(path) if f.startswith('sim') and f.endswith('.csv')]

    # Create an empty list to hold the dataframes
    dfs = []

    # Loop through each CSV file and read it into a dataframe, then append it to the list of dataframes
    for file in csv_files:
        df = pd.read_csv(os.path.join(path, file), sep=";")
        dfs.append(df)

    # Concatenate all the dataframes in the list into a single dataframe
    combined_df = pd.concat(dfs, ignore_index=True)

    df2 = pd.read_csv('gold/municipios.csv', delimiter=';')

    # Junta os dataframes com base na coluna "MUNICIPIO_CODIGO"
    df_merged = pd.merge(combined_df, df2, on='MUNICIPIO_CODIGO')

    # Converte a coluna "MUNICIPIO_CODIGO" para o tipo de dados inteiro
    df_merged['MUNICIPIO_CODIGO'] = df_merged['MUNICIPIO_CODIGO'].astype(int)

    # Write the combined dataframe to a new CSV file
    output_file = os.path.join('gold/', 'sim.csv')
    df_merged.to_csv(output_file, sep=';', index=False)


if __name__ == "__main__":
    agrupar_arquivos()
