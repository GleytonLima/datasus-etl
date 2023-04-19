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

    df.dropna(subset=['TIPO_EQP'], inplace=True)
    df.dropna(subset=['COMPETEN'], inplace=True)
    df = df[(df["TIPO_EQP"] == 70) | (df["TIPO_EQP"] == 72)]

    # Converter a coluna COMPETEN para string
    df["COMPETEN"] = df["COMPETEN"].astype(str)

    df["ANO"] = df["COMPETEN"].str.slice(stop=4).astype(int)
    # Filtrar as linhas com anos entre 2013 e 2022
    df = df[df["ANO"].between(2013, 2022)]

    # Limpar a coluna "MES" para remover valores inválidos
    df["MES"] = df["COMPETEN"].str.slice(start=4)

    # Converter a coluna "MES" para inteiro
    df["MES"] = pd.to_numeric(df["MES"], errors="coerce").fillna(0).astype(int)

    # Excluir a coluna COMPETEN original
    df.drop("COMPETEN", axis=1, inplace=True)

    df = df.rename(columns={'CODUFMUN': 'MUNICIPIO_CODIGO'})

    result = df.groupby(['MUNICIPIO_CODIGO', 'ANO', "MES", 'TIPO_EQP']).size().reset_index(name="TOTAL_EQUIPES")

    result.to_csv('silver/cnes-ep.csv', sep=';', index=False)

    print('Arquivo salvo com sucesso!')


def agrupar_arquivos():
    # Set the path to the directory containing the CSV files
    path = 'silver/'

    # Get a list of all the CSV files in the directory that start with 'sim'
    csv_files = [f for f in os.listdir(path) if f.startswith('cnes') and f.endswith('.csv')]

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
    output_file = os.path.join('gold/', 'cnes-ep.csv')
    df_merged.to_csv(output_file, sep=';', index=False)


if __name__ == "__main__":
    # sumarizar_arquivos()
    agrupar_arquivos()
