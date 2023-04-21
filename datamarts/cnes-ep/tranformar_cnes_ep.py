import glob
import json
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


def sumarizar_arquivos_nasf_esf():
    all_files = glob.glob("bronze/*.csv")
    dfs = []
    for filename in all_files:
        df = pd.read_csv(filename, sep=';')
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    df.dropna(subset=['TIPO_EQP'], inplace=True)
    df.dropna(subset=['COMPETEN'], inplace=True)
    df = df[(df["TIPO_EQP"] == 70) | (df["TIPO_EQP"] == 72)]

    # Converter a coluna COMPETEN para string
    df["COMPETEN"] = df["COMPETEN"].astype(str)

    df["ANO"] = df["COMPETEN"].str.slice(stop=4).astype(int)
    # Filtrar as linhas com anos entre 2013 e 2022
    df = df[df["ANO"].between(2013, 2023)]

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


def sumarizar_arquivos_consultorios_rua():
    all_files = glob.glob("bronze/*.csv")
    dfs = []
    for filename in all_files:
        df = pd.read_csv(filename, sep=';')
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    df.dropna(subset=['TIPO_EQP'], inplace=True)
    df.dropna(subset=['COMPETEN'], inplace=True)
    df = df[(df["TIPO_EQP"] == 73)]

    # Converter a coluna COMPETEN para string
    df["COMPETEN"] = df["COMPETEN"].astype(str)

    df["ANO"] = df["COMPETEN"].str.slice(stop=4).astype(int)
    # Filtrar as linhas com anos entre 2013 e 2022
    df = df[df["ANO"].between(2013, 2023)]

    # Limpar a coluna "MES" para remover valores inválidos
    df["MES"] = df["COMPETEN"].str.slice(start=4)

    # Converter a coluna "MES" para inteiro
    df["MES"] = pd.to_numeric(df["MES"], errors="coerce").fillna(0).astype(int)

    # Excluir a coluna COMPETEN original
    df.drop("COMPETEN", axis=1, inplace=True)

    df = df.rename(columns={'CODUFMUN': 'MUNICIPIO_CODIGO'})

    result = df.groupby(['MUNICIPIO_CODIGO', 'ANO', "MES", 'TIPO_EQP']).size().reset_index(name="TOTAL_EQUIPES")

    result.to_csv('silver/cnes-ep-consultorio-rua.csv', sep=';', index=False)

    print('Arquivo salvo com sucesso!')


def agrupar_arquivos_nasf_esf():
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

    # inicio preenchendo vazios
    # criando um dataframe com todos os pares de estado/ano possíveis
    municipios = df2['MUNICIPIO_CODIGO'].unique()
    ano_min = combined_df['ANO'].min()
    ano_max = combined_df['ANO'].max()
    anos = pd.date_range(start=str(int(ano_min)), end=str(int(ano_max) + 1), freq='Y').year
    tipos_equipe = [70, 72]

    df3 = pd.DataFrame([(e, t, a) for e in municipios for a in anos for t in tipos_equipe],
                       columns=['MUNICIPIO_CODIGO', 'TIPO_EQP', 'ANO'])

    # fazendo um left join entre o dataframe criado acima e o dataframe original "df2"
    df4 = pd.merge(df3, combined_df, how='left', on=['MUNICIPIO_CODIGO', 'TIPO_EQP', 'ANO'])
    # preenchendo com 0 os valores nulos da coluna "total"
    df4['TOTAL_EQUIPES'] = df4['TOTAL_EQUIPES'].fillna(0)
    df4['MES'] = df4['MES'].fillna(12)

    # agregando por estado e ano, somando os valores da coluna "total"
    combined_df = df4.groupby(["MUNICIPIO_CODIGO", 'TIPO_EQP', 'ANO', 'MES'], as_index=False).agg(
        {'TOTAL_EQUIPES': 'sum'})

    # fim preenchendo vazios

    # Junta os dataframes com base na coluna "MUNICIPIO_CODIGO"
    df_merged = pd.merge(df2, combined_df, on='MUNICIPIO_CODIGO', how='left')
    df_merged['TOTAL_EQUIPES'] = df_merged['TOTAL_EQUIPES'].fillna(0)

    # Converte a coluna "MUNICIPIO_CODIGO" para o tipo de dados inteiro
    df_merged['MUNICIPIO_CODIGO'] = df_merged['MUNICIPIO_CODIGO'].astype(int)
    df_merged['ANO'] = df_merged['ANO'].astype('Int64')
    df_merged['MES'] = df_merged['MES'].astype('Int64')
    df_merged['TOTAL_EQUIPES'] = df_merged['TOTAL_EQUIPES'].astype('Int64')

    # Write the combined dataframe to a new CSV file
    output_file = os.path.join('gold/', 'cnes-ep.csv')
    df_merged.to_csv(output_file, sep=';', index=False)


def agrupar_consultorios_rua():
    # Set the path to the directory containing the CSV files
    path = 'silver/'

    # Get a list of all the CSV files in the directory that start with 'sim'
    csv_files = [f for f in os.listdir(path) if f.startswith('cnes-ep-consultorio-rua') and f.endswith('.csv')]

    # Create an empty list to hold the dataframes
    dfs = []

    # Loop through each CSV file and read it into a dataframe, then append it to the list of dataframes
    for file in csv_files:
        df = pd.read_csv(os.path.join(path, file), sep=";")
        dfs.append(df)

    # Concatenate all the dataframes in the list into a single dataframe
    combined_df = pd.concat(dfs, ignore_index=True)

    df2 = pd.read_csv('gold/municipios.csv', delimiter=';')

    # inicio preenchendo vazios
    # criando um dataframe com todos os pares de estado/ano possíveis
    municipios = df2['MUNICIPIO_CODIGO'].unique()
    ano_min = combined_df['ANO'].min()
    ano_max = combined_df['ANO'].max()
    anos = pd.date_range(start=str(int(ano_min)), end=str(int(ano_max) + 1), freq='Y').year
    tipos_equipe = [73]

    df3 = pd.DataFrame([(e, t, a) for e in municipios for a in anos for t in tipos_equipe],
                       columns=['MUNICIPIO_CODIGO', 'TIPO_EQP', 'ANO'])

    # fazendo um left join entre o dataframe criado acima e o dataframe original "df2"
    df4 = pd.merge(df3, combined_df, how='left', on=['MUNICIPIO_CODIGO', 'TIPO_EQP', 'ANO'])
    # preenchendo com 0 os valores nulos da coluna "total"
    df4['TOTAL_EQUIPES'] = df4['TOTAL_EQUIPES'].fillna(0)
    df4['MES'] = df4['MES'].fillna(12)

    # agregando por estado e ano, somando os valores da coluna "total"
    combined_df = df4.groupby(["MUNICIPIO_CODIGO", 'TIPO_EQP', 'ANO', 'MES'], as_index=False).agg(
        {'TOTAL_EQUIPES': 'sum'})

    # fim preenchendo vazios

    # Junta os dataframes com base na coluna "MUNICIPIO_CODIGO"
    df_merged = pd.merge(df2, combined_df, on='MUNICIPIO_CODIGO', how='left')
    df_merged['TOTAL_EQUIPES'] = df_merged['TOTAL_EQUIPES'].fillna(0)

    # Converte a coluna "MUNICIPIO_CODIGO" para o tipo de dados inteiro
    df_merged['MUNICIPIO_CODIGO'] = df_merged['MUNICIPIO_CODIGO'].astype(int)
    df_merged['ANO'] = df_merged['ANO'].astype('Int64')
    df_merged['MES'] = df_merged['MES'].astype('Int64')
    df_merged['TOTAL_EQUIPES'] = df_merged['TOTAL_EQUIPES'].astype('Int64')

    # Write the combined dataframe to a new CSV file
    output_file = os.path.join('gold/', 'cnes-ep-consultorio-rua.csv')
    df_merged.to_csv(output_file, sep=';', index=False)


def calcular_cobertura_municipio_sem_nasf():
    df = pd.read_csv('gold/cnes-ep.csv', delimiter=';')

    # Cria uma tabela dinâmica com as informações solicitadas
    table = pd.pivot_table(df,
                           values='TOTAL_EQUIPES',
                           index=["MUNICIPIO_CODIGO", "MUNICIPIO_NOME", "ESTADO_CODIGO", "ESTADO_SIGLA",
                                  "REGIAO_SAUDE_CODIGO", "REGIAO_SAUDE_NOME", "ESTADO_NOME", "MUNICIPIO_POPULACAO",
                                  "ANO", "MES"],
                           columns=['TIPO_EQP'],
                           aggfunc='sum')

    # Filtra as linhas que atendem aos critérios especificados
    filtro = (table[72] == 0) & (table[70] != 0)
    table_filtrada = table.loc[filtro]

    # Calcula a coluna COBERTURA_ESAF
    table_filtrada = table_filtrada.reset_index()
    table_filtrada['COBERTURA_ESAF'] = table_filtrada[70] * 3450 * 100 / table_filtrada['MUNICIPIO_POPULACAO']

    # Reorganiza a tabela final
    table_final = table_filtrada.reset_index()[
        ["MUNICIPIO_CODIGO", "MUNICIPIO_NOME", "ESTADO_CODIGO", "ESTADO_SIGLA",
         "REGIAO_SAUDE_CODIGO", "REGIAO_SAUDE_NOME", "ESTADO_NOME", "MUNICIPIO_POPULACAO",
         "ANO", "MES", 70, 72, "COBERTURA_ESAF"]]

    # Salva o resultado em um novo arquivo CSV
    table_final.to_csv('gold/cnes-ep-com-calculo.csv', sep=";", index=False)


def tratar_arquivo_json_geobr():
    # para remover o ultimo caracter do arquivo original
    with open('silver/geojs-100-mun-simplificado.json', encoding="UTF-8") as f:
        data = json.load(f)

    for feature in data['features']:
        feature['properties']['id'] = feature['properties']['id'][:-1]

    with open('gold/geojs-100-mun-simplificado.json', 'w', encoding="UTF-8") as f:
        json.dump(data, f)


if __name__ == "__main__":
    # sumarizar_arquivos_nasf_esf()
    # agrupar_arquivos_nasf_esf()
    # sumarizar_arquivos_consultorios_rua()
    # agrupar_consultorios_rua()
    # tratar_arquivo_json_geobr()
    calcular_cobertura_municipio_sem_nasf()
