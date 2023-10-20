import dataclasses
import glob
import os
from typing import List

import pandas as pd


def path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


@dataclasses.dataclass
class TransformarCnesLT:
    nome_arquivo_saida: str
    nome_totalizador: str
    codigos_tipo_unidade: List[int]
    codigos_tipo_leito: List[int]
    codigos_especialidade_leito: List[int]

    def gerar_caminho_arquivos_entrada(self):
        return f'{path("/data/bronze/datasus/cnes/lt")}/*.csv'

    def gerar_caminho_arquivo_saida(self):
        return f'{path("/data/gold/datasus/cnes")}/{self.nome_arquivo_saida}'

    def filtrar(self):
        # Obtém a lista de arquivos CSV na pasta "bronze"
        arquivos_csv = glob.glob(f'{self.gerar_caminho_arquivos_entrada()}')

        # Lista para armazenar os DataFrames filtrados de cada arquivo
        dfs_filtrados = []

        # Itera sobre cada arquivo CSV
        for arquivo in arquivos_csv:
            df = pd.read_csv(arquivo, delimiter=';', quotechar=None, quoting=3, low_memory=False).applymap(
                lambda x: x.replace('"', '') if isinstance(x, str) else x)

            df.columns = df.columns.str.replace('"', '')

            df = df.infer_objects()

            df['TP_UNID'] = df['TP_UNID'].astype(int)
            df['TP_LEITO'] = df['TP_LEITO'].astype(int)
            df['CODLEITO'] = df['CODLEITO'].astype(int)

            df_filtrado = df

            if self.codigos_tipo_unidade:
                df_filtrado = df_filtrado[df_filtrado['TP_UNID'].isin(self.codigos_tipo_unidade)]
            if self.codigos_tipo_leito:
                df_filtrado = df[df['TP_LEITO'].isin(self.codigos_tipo_leito)]

            if self.codigos_especialidade_leito:
                df_filtrado = df_filtrado[df_filtrado['CODLEITO'].isin(self.codigos_especialidade_leito)]

            df_filtrado['CODUF'] = df_filtrado['CODUFMUN'].astype(str).str[:2]
            df_filtrado['COMPETEN'] = df_filtrado['COMPETEN'].astype(str)
            df_filtrado['ANO'] = df_filtrado['COMPETEN'].str[:4]

            dfs_filtrados.append(df_filtrado)

        df_resultado = pd.concat(dfs_filtrados)

        contagem = df_resultado.groupby(
            ['CODUF', 'CODUFMUN', 'TP_UNID', 'TP_LEITO', 'CODLEITO', 'ANO'])['QT_SUS'].sum().reset_index(
            name=self.nome_totalizador)

        contagem.rename(columns={
            "TP_UNID": "TIPO_UNIDADE_CODIGO",
            "TP_LEITO": "TIPO_LEITO_CODIGO",
            "CODLEITO": "LEITO_ESPECIALIZACAO_CODIGO",
            "CODUF": "ESTADO_CODIGO",
            "CODUFMUN": "MUNICIPIO_CODIGO"
        }, inplace=True)

        contagem = contagem.dropna(subset=['ANO'])

        contagem.to_csv(self.gerar_caminho_arquivo_saida(), sep=';', index=False)

    @staticmethod
    def gerar_arquivo_auxiliar_tipo_unidade():
        # Dados do DataFrame
        data = {
            'TIPO_UNIDADE_CODIGO': [5, 7, 62],
            'TIPO_UNIDADE_DESCRICAO': ['Hospital Geral', 'Hospital Especializado', 'Hospital Dia']
        }
        # Cria o DataFrame
        df = pd.DataFrame(data)
        # Define o caminho do arquivo de saída
        caminho_saida = f'{path("/data/gold/sds")}/auxiliar-cnes-lt-tipo-unidade.csv'
        # Salva o DataFrame em um arquivo CSV
        df.to_csv(caminho_saida, sep=';', index=False)
        print(f"Arquivo {caminho_saida} gerado com sucesso!")

    @staticmethod
    def gerar_arquivo_auxiliar_especializacao_leito():
        # Dados do DataFrame
        data = {
            'LEITO_ESPECIALIZACAO_CODIGO': [5, 47, 73, 87],
            'LEITO_ESPECIALIZACAO_DESCRICAO': [
                'Psiquiatria',
                'Psiquiatria',
                'Saúde Mental',
                'Saúde Mental'
            ]
        }
        # Cria o DataFrame
        df = pd.DataFrame(data)
        # Define o caminho do arquivo de saída
        caminho_saida_classificacao = f'{path("/data/gold/sds")}/auxiliar-cnes-lt-especialidade-leito.csv'
        # Salva o DataFrame em um arquivo CSV
        df.to_csv(caminho_saida_classificacao, sep=';', index=False)
        print(f"Arquivo {caminho_saida_classificacao} gerado com sucesso!")
