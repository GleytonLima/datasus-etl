import dataclasses
import os

import pandas as pd

from extract.download_util import DownloadGithubIbge, DownloadIBGEFtp, DownloadSageSaudeHttp


def path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


@dataclasses.dataclass
class RegiaoSaude:
    def arquivo_bronze(self):
        return DownloadSageSaudeHttp().arquivo_municipios_com_regiao_saude()

    def gerar_nome_arquivo_saida_regioes_saude_enriquecida(self):
        return "/data/gold/sds/regioes-saude-enriquecido.csv"

    def gerar_regioes_saude_enriquecido(self, municipios_enriquecidos):
        municipios_com_soma_populacao_regiao_saude = municipios_enriquecidos.groupby(
            ['ESTADO_CODIGO', 'ESTADO_SIGLA', 'ESTADO_NOME', 'REGIAO_SAUDE_CODIGO', 'REGIAO_SAUDE_NOME']).agg(
            {'MUNICIPIO_POPULACAO': 'sum'}).reset_index()
        municipios_com_soma_populacao_regiao_saude.rename(
            columns={'MUNICIPIO_POPULACAO': 'REGIAO_SAUDE_POPULACAO'}, inplace=True)
        municipios_com_soma_populacao_regiao_saude = municipios_com_soma_populacao_regiao_saude[
            ['ESTADO_CODIGO', 'ESTADO_SIGLA', 'ESTADO_NOME', 'REGIAO_SAUDE_CODIGO', 'REGIAO_SAUDE_NOME',
             "REGIAO_SAUDE_POPULACAO"]]
        municipios_com_soma_populacao_regiao_saude = municipios_com_soma_populacao_regiao_saude.drop_duplicates()
        municipios_com_soma_populacao_regiao_saude.to_csv(self.gerar_nome_arquivo_saida_regioes_saude_enriquecida(),
                                                          sep=";",
                                                          index=False)
        return municipios_com_soma_populacao_regiao_saude

    def gerar_arquivo_regiao_saude(self):
        municipios_enriquecidos = pd.read_csv(Municipio().gerar_nome_arquivo_saida(),
                                              sep=";",
                                              dtype={'MUNICIPIO_CODIGO': object,
                                                     'MUNICIPIO_POPULACAO': int
                                                     })

        self.gerar_regioes_saude_enriquecido(municipios_enriquecidos)


@dataclasses.dataclass
class Populacao:

    def gerar_nome_arquivo_populacao_estado_entrada(self):
        return DownloadIBGEFtp().arquivo_populacao_estado()

    def gerar_nome_arquivo_populacao_estado_saida(self):
        return f'{path("/data/silver/ibge/censo")}/POP2022_Brasil_e_UFs.csv'

    def gerar_nome_arquivo_populacao_municipio_entrada(self):
        return DownloadIBGEFtp().arquivo_populacao_estado()

    def gerar_nome_arquivo_populacao_municipio_saida(self):
        return f'{path("/data/silver/ibge/censo")}/POP2022_Municipios.csv'

    def converter_censo_uf_xls_em_csv(self):
        # Carrega o arquivo XLS em um DataFrame do Pandas
        df = pd.read_excel(self.gerar_nome_arquivo_populacao_estado_entrada(), skiprows=[0], header=[0],
                           usecols=lambda x: 'Unnamed' not in x)

        # Remove as linhas em que a primeira coluna está em branco
        df = df.dropna(subset=[df.columns[0]])

        nomes_estados_validos = [
            "Brasil",
            "Região Norte",
            "Rondônia",
            "Acre",
            "Amazonas",
            "Roraima",
            "Pará",
            "Amapá",
            "Tocantins",
            "Região Nordeste",
            "Maranhão",
            "Piauí",
            "Ceará",
            "Rio Grande do Norte",
            "Paraíba",
            "Pernambuco",
            "Alagoas",
            "Sergipe",
            "Bahia",
            "Região Sudeste",
            "Minas Gerais",
            "Espírito Santo",
            "Rio de Janeiro",
            "São Paulo",
            "Região Sul	",
            "Paraná",
            "Santa Catarina",
            "Rio Grande do Sul",
            "Região Centro-Oeste",
            "Mato Grosso do Sul",
            "Mato Grosso",
            "Goiás",
            "Distrito Federal"
        ]

        df = df[df['BRASIL E UNIDADES DA FEDERAÇÃO'].isin(nomes_estados_validos)]

        df.rename(
            columns={
                'BRASIL E UNIDADES DA FEDERAÇÃO': 'ESTADO_NOME',
                'POPULAÇÃO': 'ESTADO_POPULACAO'
            },
            inplace=True)

        # Salva o DataFrame resultante em um arquivo CSV
        df.to_csv(self.gerar_nome_arquivo_populacao_estado_saida(),
                  sep=";",
                  index=False)

    def converter_censo_municipio_xls_em_csv(self):
        df = pd.read_excel(self.gerar_nome_arquivo_populacao_municipio_entrada(), skiprows=[0], header=[0],
                           dtype={'COD. UF': str, 'COD. MUNIC': str})

        df = df.dropna(subset=[df.columns[0]])
        df = df[df['UF'].str.len() == 2]

        df['MUNICIPIO_CODIGO'] = df['COD. UF'].astype(str) + df['COD. MUNIC'].astype(str).str[:-1]
        df['MUNICIPIO_CODIGO'] = df['MUNICIPIO_CODIGO'].astype(str)
        df = df.drop(columns=['COD. UF', 'COD. MUNIC'])

        df.rename(
            columns={'POPULAÇÃO': 'MUNICIPIO_POPULACAO', "NOME DO MUNICÍPIO": "MUNICIPIO_NOME", "UF": "ESTADO_SIGLA"},
            inplace=True)
        df['MUNICIPIO_POPULACAO'] = df['MUNICIPIO_POPULACAO'].astype(str).replace(r'\s*\([^)]*\)\s*', '', regex=True)
        df['MUNICIPIO_POPULACAO'] = df['MUNICIPIO_POPULACAO'].str.replace('.', '', regex=False)
        df.to_csv(self.gerar_nome_arquivo_populacao_municipio_saida(),
                  sep=";",
                  index=False)


class Estado:
    def gerar_nome_arquivo_entrada(self):
        return DownloadGithubIbge().gerar_arquivo_saida_estados()

    def gerar_nome_arquivo_saida(self):
        return f'{path("/data/gold/sds")}/estados.csv'

    def enriquecer_estados(self):
        estados_com_codigo = pd.read_csv(self.gerar_nome_arquivo_entrada())
        estados_com_codigo.rename(
            columns={'codigo_uf': 'ESTADO_CODIGO', 'uf': 'ESTADO_SIGLA', 'nome': 'ESTADO_NOME',
                     'regiao': 'ESTADO_REGIAO'},
            inplace=True)
        estados_somente_nome_populacao = pd.read_csv(Populacao().gerar_nome_arquivo_populacao_estado_saida(),
                                                     sep=';',
                                                     dtype={'ESTADO_NOME': object, 'ESTADO_POPULACAO': int})
        combinacao = pd.merge(estados_somente_nome_populacao,
                              estados_com_codigo[['ESTADO_CODIGO', 'ESTADO_SIGLA', 'ESTADO_NOME']],
                              on='ESTADO_NOME',
                              how='left')
        combinacao.dropna(subset=['ESTADO_CODIGO'], inplace=True)
        combinacao.to_csv(self.gerar_nome_arquivo_saida(),
                          sep=';',
                          index=False,
                          float_format='%.0f')


class RegiaoAdministrativaDF:
    def arquivo_regioes(self):
        return f'{path("/data/gold/sds")}/municipios_df.csv'


class Municipio:
    def gerar_nome_arquivo_entrada(self):
        return DownloadGithubIbge().gerar_arquivo_saida_municipios()

    def gerar_nome_arquivo_saida(self):
        return f'{path("/data/gold/sds")}/municipios.csv'

    def enriquecer_municipios(self):
        # Carregue os arquivos CSV e parquet em dataframes pandas
        df_municipios = pd.read_csv(self.gerar_nome_arquivo_entrada())
        df_municipios['codigo_ibge'] = df_municipios['codigo_ibge'].astype(str).str[:-1]
        df_municipios.rename(
            columns={'codigo_ibge': 'MUNICIPIO_CODIGO', 'nome': 'MUNICIPIO_NOME', 'codigo_uf': 'ESTADO_CODIGO'},
            inplace=True)

        df_estados = pd.read_csv(Estado().gerar_nome_arquivo_saida(), sep=';')

        df_populacao_municipio = pd.read_csv(Populacao().gerar_nome_arquivo_populacao_municipio_saida(),
                                             sep=';',
                                             dtype={'MUNICIPIO_CODIGO': object})

        df_municipios_com_regionais_saude = pd.read_csv(RegiaoSaude().arquivo_bronze(),
                                                        dtype={'cidade': object,
                                                               'no_colegiado': object,
                                                               'ibge': object})

        # Renomeie as colunas para que correspondam
        df_municipios_com_regionais_saude.rename(
            columns={
                'uf': 'ESTADO_SIGLA',
                'cidade': 'MUNICIPIO_NOME',
                'no_colegiado': 'REGIAO_SAUDE_NOME',
                'co_colegiado': 'REGIAO_SAUDE_CODIGO',
                'ibge': 'MUNICIPIO_CODIGO'},
            inplace=True)
        df_municipios_com_regionais_saude = pd.merge(df_municipios_com_regionais_saude,
                                                     df_estados[
                                                         ['ESTADO_CODIGO', 'ESTADO_SIGLA', 'ESTADO_NOME']],
                                                     on='ESTADO_SIGLA',
                                                     how='left')

        df_municipios = pd.merge(df_municipios,
                                 df_municipios_com_regionais_saude[
                                     ['MUNICIPIO_CODIGO',
                                      'ESTADO_SIGLA',
                                      'REGIAO_SAUDE_CODIGO',
                                      'REGIAO_SAUDE_NOME',
                                      'ESTADO_NOME']],
                                 on='MUNICIPIO_CODIGO',
                                 how='left')
        df_municipios = pd.merge(df_municipios,
                                 df_populacao_municipio[['MUNICIPIO_CODIGO', 'MUNICIPIO_POPULACAO']],
                                 on='MUNICIPIO_CODIGO',
                                 how='left')

        # municipios ficticios do DF
        df_municipios_df_com_regionais_saude = pd.read_csv(RegiaoAdministrativaDF().arquivo_regioes(), sep=";")

        # deletando unico registro do DF para substituir pelas regioes administrativas
        df_municipios = df_municipios[df_municipios['MUNICIPIO_CODIGO'] != 530010]
        df_municipios = df_municipios[df_municipios['MUNICIPIO_CODIGO'] != "530010"]

        df_municipios = pd.concat([df_municipios, df_municipios_df_com_regionais_saude])

        df_municipios.to_csv(self.gerar_nome_arquivo_saida(),
                             sep=';',
                             index=False)
