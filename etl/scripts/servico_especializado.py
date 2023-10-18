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
class TransformarCnesSR:
    nome_arquivo_saida: str
    nome_totalizador: str
    codigos_servico_especializado: List[int]
    codigos_tipo_unidade: List[int]
    codigos_classificacao: List[int]

    def gerar_caminho_arquivos_entrada(self):
        return f'{path("/data/bronze/datasus/cnes/sr")}/*.csv'

    def gerar_caminho_arquivo_saida(self):

        return f'{path("/data/gold/datasus/cnes")}/{self.nome_arquivo_saida}'

    def calcular_quantitativo_sr(self):
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

            df['SERV_ESP'] = df['SERV_ESP'].astype(int)
            df['CLASS_SR'] = df['CLASS_SR'].astype(int)
            df['TP_UNID'] = df['TP_UNID'].astype(int)

            df_filtrado = df
            if self.codigos_servico_especializado:
                df_filtrado = df[df['SERV_ESP'].isin(self.codigos_servico_especializado)]

            if self.codigos_classificacao:
                df_filtrado = df_filtrado[df_filtrado['CLASS_SR'].isin(self.codigos_classificacao)]

            if self.codigos_tipo_unidade:
                df_filtrado = df_filtrado[df_filtrado['TP_UNID'].isin(self.codigos_tipo_unidade)]

            df_filtrado['CODUF'] = df_filtrado['CODUFMUN'].astype(str).str[:2]
            df_filtrado['COMPETEN'] = df_filtrado['COMPETEN'].astype(str)
            df_filtrado['ANO'] = df_filtrado['COMPETEN'].str[:4]

            dfs_filtrados.append(df_filtrado)

        df_resultado = pd.concat(dfs_filtrados)

        contagem = df_resultado.groupby(['CODUF', 'CODUFMUN', 'SERV_ESP', 'CLASS_SR', 'ANO']).size().reset_index(
            name=self.nome_totalizador)

        contagem.rename(columns={
            "SERV_ESP": "SERVICO_ESPECIALIZADO_CODIGO",
            "CLASS_SR": "SERVICO_ESPECIALIZADO_CLASSIFICACAO_CODIGO",
            "CODUF": "ESTADO_CODIGO",
            "CODUFMUN": "MUNICIPIO_CODIGO"
        }, inplace=True)

        contagem = contagem.dropna(subset=['ANO'])

        contagem.to_csv(self.gerar_caminho_arquivo_saida(), sep=';', index=False)

    def gerar_arquivo_auxiliar_servico_especializado(self):
        # Dados do DataFrame
        data = {
            'SERVICO_ESPECIALIZADO_CODIGO': [115],
            'SERVICO_ESPECIALIZADO_DESCRICAO': ['Serviço de Atenção Psicossocial']
        }
        # Cria o DataFrame
        df_servico_especializado = pd.DataFrame(data)
        # Define o caminho do arquivo de saída
        caminho_saida = 'gold/servico_especializado.csv'
        # Salva o DataFrame em um arquivo CSV
        df_servico_especializado.to_csv(caminho_saida, sep=';', index=False)

    def gerar_arquivo_auxiliar_classificacao_srt(self):
        # Dados do DataFrame
        data = {
            'SERVICO_ESPECIALIZADO_CLASSIFICACAO_CODIGO': [2, 3, 4, 5, 6, 7],
            'SERVICO_ESPECIALIZADO_CLASSIFICACAO_DESCRICAO': [
                'Atendimento Psicossocial',
                'Serviço Hospitalar para Saúde Mental',
                'Serviço Residencial Terapêutico SRT - Tipo I',
                'Serviço Residencial Terapêutico SRT - Tipo II',
                'UA Adulto',
                'UA Infantil']
        }
        # Cria o DataFrame
        df_classificacao_servico_especializado = pd.DataFrame(data)
        # Define o caminho do arquivo de saída
        caminho_saida_classificacao = 'gold/classificacao_servico_especializado.csv'
        # Salva o DataFrame em um arquivo CSV
        df_classificacao_servico_especializado.to_csv(caminho_saida_classificacao, sep=';', index=False)
