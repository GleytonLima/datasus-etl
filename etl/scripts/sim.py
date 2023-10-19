import dataclasses
import glob
import os
from datetime import datetime

import pandas as pd

from base import Municipio


def path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


@dataclasses.dataclass
class Coluna:
    nome: str
    tipo: str
    descricao: str

    def get_dtype(self):
        return {self.nome: self.tipo}


@dataclasses.dataclass
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


@dataclasses.dataclass
class TransformarSimDO:
    nome_arquivo_saida: str
    causa_basica_list: tuple

    def gerar_caminho_arquivos_entrada(self):
        return f'{path("/data/bronze/datasus/SIM/DO")}/*.csv'

    def gerar_caminho_arquivo_saida(self):
        return f'{path("/data/gold/datasus/sim")}/{self.nome_arquivo_saida}'

    def sumarizar_por_causa_basica(self):
        all_files = glob.glob(self.gerar_caminho_arquivos_entrada())
        dfs = []
        for filename in all_files:
            df = pd.read_csv(filename, delimiter=';', quotechar=None, quoting=3, low_memory=False).applymap(
                lambda x: x.replace('"', '') if isinstance(x, str) else x)
            df.columns = df.columns.str.replace('"', '')
            df = df.infer_objects()
            df.dropna(subset=['CAUSABAS'], inplace=True)
            filtro = df['CAUSABAS'].str.startswith(self.causa_basica_list)
            df = df.loc[filtro]
            dfs.append(df)

        df = pd.concat(dfs, ignore_index=True)

        print(df.head())

        df['CAUSABAS'] = df['CAUSABAS'].str[:3]
        df['DTOBITO'] = df['DTOBITO'].apply(parse_date)

        df['MES'] = df['DTOBITO'].dt.month
        df['ANO'] = df['DTOBITO'].dt.year

        df = df.rename(columns={'CODMUNOCOR': 'MUNICIPIO_CODIGO'})

        df['MES_ANO'] = df.apply(lambda x: '{:02d}-{}'.format(x['MES'], x['ANO']), axis=1)

        result = df.groupby(['MUNICIPIO_CODIGO', 'MES_ANO', 'CAUSABAS'])['DTOBITO'].count().reset_index(
            name='TOTAL_OBITOS')

        df2 = pd.read_csv(Municipio().gerar_nome_arquivo_saida(),
                          sep=";",
                          dtype={'MUNICIPIO_CODIGO': object,
                                 'MUNICIPIO_POPULACAO': int
                                 })

        # Junta os dataframes com base na coluna "MUNICIPIO_CODIGO"
        df_merged = pd.merge(result, df2, on='MUNICIPIO_CODIGO')

        # Converte a coluna "MUNICIPIO_CODIGO" para o tipo de dados inteiro
        df_merged['MUNICIPIO_CODIGO'] = df_merged['MUNICIPIO_CODIGO'].astype(int)

        # Write the combined dataframe to a new CSV file
        df_merged.to_csv(self.gerar_caminho_arquivo_saida(), sep=';', index=False)
