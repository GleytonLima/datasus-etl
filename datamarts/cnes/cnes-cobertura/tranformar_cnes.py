import glob
from dataclasses import dataclass
from typing import List

import pandas as pd

ANOS_CONSIDERADOS = [2018, 2019, 2020, 2021, 2022]

MES_COMPETENCIA_CONSIDERADO = "12"

PASTA_CNES_DADOS_BRUTOS = "cnes-dadosbrutos"


@dataclass
class Coluna:
    nome: str
    tipo: str

    def get_dtype(self):
        return {self.nome: self.tipo}


@dataclass
class EstabelecimentoColuna:
    MES = Coluna(
        nome="MES",
        tipo="str"
    )
    ANO = Coluna(
        nome="ANO",
        tipo="str"
    )
    CO_TIPO_ESTABELECIMENTO = Coluna(
        nome="CO_TIPO_ESTABELECIMENTO",
        tipo="str"
    )
    CO_MUNICIPIO_GESTOR = Coluna(
        nome="CO_MUNICIPIO_GESTOR",
        tipo="str"
    )
    CO_ESTADO_GESTOR = Coluna(
        nome="CO_ESTADO_GESTOR",
        tipo="str"
    )
    CO_CEP = Coluna(
        nome="CO_CEP",
        tipo="str"
    )
    NO_FANTASIA = Coluna(
        nome="NO_FANTASIA",
        tipo="str"
    )
    CO_CNES = Coluna(
        nome="CO_CNES",
        tipo="str"
    )
    CO_UNIDADE = Coluna(
        nome="CO_UNIDADE",
        tipo="object"
    )
    CO_MOTIVO_DESAB = Coluna(
        nome="CO_MOTIVO_DESAB",
        tipo="str"
    )
    TP_UNIDADE = Coluna(
        nome="TP_UNIDADE",
        tipo="object"
    )


@dataclass
class Estabelecimento:
    anos: List[int]
    mes: str
    pasta: str
    nome = "tbEstabelecimento"
    chunksize: int = 1000
    colunas: EstabelecimentoColuna = EstabelecimentoColuna()
    codigo_caps = 70

    def gerar_dtype(self):
        return self.colunas.TP_UNIDADE.get_dtype() | self.colunas.CO_UNIDADE.get_dtype()

    def gerar_nome_base(self, ano, mes):
        return f"{self.nome}{ano}{mes}"

    def gerar_colunas_saida(self):
        return [
            self.colunas.CO_UNIDADE.nome,
            self.colunas.CO_CNES.nome,
            self.colunas.NO_FANTASIA.nome,
            self.colunas.CO_CEP.nome,
            self.colunas.TP_UNIDADE.nome,
            self.colunas.CO_ESTADO_GESTOR.nome,
            self.colunas.CO_MUNICIPIO_GESTOR.nome,
            self.colunas.CO_TIPO_ESTABELECIMENTO.nome,
            self.colunas.ANO.nome,
            self.colunas.MES.nome
        ]

    def gerar_nome_original(self, ano):
        return f'{self.pasta}/bronze/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/silver/{self.gerar_nome_base(ano, self.mes)}_caps.csv'

    def filtrar_caps(self):
        for ano in self.anos:
            reader = pd.read_csv(self.gerar_nome_original(ano),
                                 dtype=self.gerar_dtype(),
                                 sep=";",
                                 chunksize=self.chunksize)

            writer = pd.DataFrame(columns=reader.get_chunk().columns)

            for chunk in reader:
                filtered_chunk = chunk[(chunk[self.colunas.TP_UNIDADE.nome] == f'{self.codigo_caps}') & (
                    chunk[self.colunas.CO_MOTIVO_DESAB.nome].isna())]
                filtered_chunk = filtered_chunk.assign(ANO=ano, MES=self.mes)
                writer = pd.concat([writer, filtered_chunk])

            writer.to_csv(self.gerar_nome_saida(ano),
                          index=False,
                          sep=';',
                          columns=self.gerar_colunas_saida())


@dataclass
class SubtipoColuna:
    CO_SUB_TIPO = Coluna(
        nome="CO_SUB_TIPO",
        tipo="int"
    )
    CO_TIPO_UNIDADE = Coluna(
        nome="CO_TIPO_UNIDADE",
        tipo="int32"
    )
    DS_SUB_TIPO = Coluna(
        nome="DS_SUB_TIPO",
        tipo="object"
    )


@dataclass
class Subtipo:
    anos: List[int]
    mes: str
    pasta: str
    nome = "tbSubTipo"
    chunksize: int = 1000
    colunas: SubtipoColuna = SubtipoColuna()
    codigo_caps = 70
    nomes_arquivos: List[str] = None

    def gerar_dtype(self):
        return self.colunas.CO_TIPO_UNIDADE.get_dtype() | self.colunas.CO_SUB_TIPO.get_dtype()

    def gerar_nome_base(self, ano, mes):
        return f"{self.nome}{ano}{mes}"

    def gerar_nome_original(self, ano):
        return f'{self.pasta}/bronze/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/silver/{ano}_caps.csv'

    def filtrar_caps(self):
        for ano in self.anos:
            arquivo_tipo_subtipo = pd.read_csv(self.gerar_nome_original(ano),
                                               sep=";",
                                               dtype=self.gerar_dtype())
            arquivo_tipo_subtipo = arquivo_tipo_subtipo[
                arquivo_tipo_subtipo[self.colunas.CO_TIPO_UNIDADE.nome] == self.codigo_caps]

            arquivo_tipo_subtipo.to_csv(self.gerar_nome_saida(ano),
                                        sep=";",
                                        index=False)


class EstabelecimentoSubtipoColuna:
    CO_TIPO_UNIDADE = Coluna(
        nome="CO_TIPO_UNIDADE",
        tipo="int32"
    )
    CO_UNIDADE = Coluna(
        nome="CO_UNIDADE",
        tipo="object"
    )
    CO_SUB_TIPO = Coluna(
        nome="CO_UNIDADE",
        tipo="object"
    )
    CO_SUB_TIPO_UNIDADE = Coluna(
        nome="CO_SUB_TIPO_UNIDADE",
        tipo="int32"
    )


@dataclass
class EstabelecimentoSubtipo:
    anos: List[int]
    mes: str
    pasta: str
    nome: str = "rlEstabSubTipo"
    chunksize: int = 1000
    colunas: EstabelecimentoSubtipoColuna = EstabelecimentoSubtipoColuna()
    codigo_caps = 70
    nomes_arquivos: List[str] = None

    def gerar_dtype(self):
        return self.colunas.CO_TIPO_UNIDADE.get_dtype() | self.colunas.CO_UNIDADE.get_dtype() | self.colunas.CO_SUB_TIPO.get_dtype() | self.colunas.CO_SUB_TIPO_UNIDADE.get_dtype()

    def gerar_nomes_arquivos(self):
        self.nomes_arquivos = []
        for ano in self.anos:
            self.nomes_arquivos.append(f"{self.nome}{ano}{self.mes}")

    def gerar_nome_base(self, ano, mes):
        return f"{self.nome}{ano}{mes}"

    def gerar_nome_original(self, ano):
        return f'{self.pasta}/bronze/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/silver/{self.gerar_nome_base(ano, self.mes)}_caps.csv'

    def filtrar_caps(self):
        for ano in self.anos:
            df = pd.read_csv(self.gerar_nome_original(ano),
                             dtype=self.gerar_dtype(),
                             sep=";")

            filtered_df = df[df[self.colunas.CO_TIPO_UNIDADE.nome] == self.codigo_caps]

            filtered_df.to_csv(self.gerar_nome_saida(ano),
                               sep=';',
                               index=False)


class PopulacaoMunicipioColunas:
    UF = Coluna(
        nome="UF",
        tipo="str"
    )
    CO_CNES = Coluna(
        nome="CO_CNES",
        tipo="str"
    )
    NO_FANTASIA = Coluna(
        nome="NO_FANTASIA",
        tipo="str"
    )
    CO_CEP = Coluna(
        nome="CO_CEP",
        tipo="str"
    )


class CombinadoColunas:
    CO_UNIDADE = Coluna(
        nome="CO_UNIDADE",
        tipo="str"
    )
    CO_CNES = Coluna(
        nome="CO_CNES",
        tipo="str"
    )
    NO_FANTASIA = Coluna(
        nome="NO_FANTASIA",
        tipo="str"
    )
    CO_CEP = Coluna(
        nome="CO_CEP",
        tipo="str"
    )
    TP_UNIDADE = Coluna(
        nome="TP_UNIDADE",
        tipo="str"
    )
    CO_ESTADO_GESTOR = Coluna(
        nome="CO_ESTADO_GESTOR",
        tipo="str"
    )
    CO_MUNICIPIO_GESTOR = Coluna(
        nome="CO_MUNICIPIO_GESTOR",
        tipo="object"
    )
    CO_TIPO_ESTABELECIMENTO = Coluna(
        nome="CO_TIPO_ESTABELECIMENTO",
        tipo="str"
    )
    ANO = Coluna(
        nome="ANO",
        tipo="object"
    )
    MES = Coluna(
        nome="MES",
        tipo="str"
    )
    CO_SUB_TIPO_UNIDADE = Coluna(
        nome="CO_SUB_TIPO_UNIDADE",
        tipo="str"
    )
    DS_SUB_TIPO = Coluna(
        nome="DS_SUB_TIPO",
        tipo="str"
    )


@dataclass
class Combinado:
    anos: List[int]
    mes: str
    pasta: str
    tabela_estabelecimentos: Estabelecimento
    tabela_relacao: EstabelecimentoSubtipo
    tabela_subtipo: Subtipo
    colunas = CombinadoColunas()

    def gerar_dtype(self):
        return self.colunas.CO_MUNICIPIO_GESTOR.get_dtype()

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/silver/{ano}-cnes_filtrados.csv'

    def combinar_estabelecimento_subtipo(self):
        for ano in self.anos:
            arquivo_caps = pd.read_csv(self.tabela_estabelecimentos.gerar_nome_saida(ano),
                                       sep=";",
                                       dtype=self.tabela_estabelecimentos.gerar_dtype())
            arquivo_tipo_subtipo = pd.read_csv(self.tabela_subtipo.gerar_nome_saida(ano),
                                               sep=";",
                                               dtype=self.tabela_subtipo.gerar_dtype())
            arquivo_relacao = pd.read_csv(self.tabela_relacao.gerar_nome_saida(ano),
                                          sep=";",
                                          dtype=self.tabela_relacao.gerar_dtype())

            arquivo_tipo_subtipo.rename(columns={
                self.tabela_subtipo.colunas.CO_SUB_TIPO.nome: self.tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome
            }, inplace=True)
            arquivo_relacao = pd.merge(arquivo_relacao,
                                       arquivo_tipo_subtipo[[
                                           self.tabela_subtipo.colunas.CO_TIPO_UNIDADE.nome,
                                           self.tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome,
                                           self.tabela_subtipo.colunas.DS_SUB_TIPO.nome
                                       ]],
                                       on=[
                                           self.tabela_subtipo.colunas.CO_TIPO_UNIDADE.nome,
                                           self.tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome
                                       ],
                                       how="left")

            df_merge = pd.merge(arquivo_caps,
                                arquivo_relacao[[
                                    self.tabela_relacao.colunas.CO_UNIDADE.nome,
                                    self.tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome,
                                    self.tabela_subtipo.colunas.DS_SUB_TIPO.nome
                                ]],
                                on=self.tabela_relacao.colunas.CO_UNIDADE.nome,
                                how='left')
            df_merge[self.tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome] = df_merge[
                self.tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome].fillna(
                0).astype(int)
            df_merge = df_merge.drop_duplicates()
            df_merge.to_csv(self.gerar_nome_saida(ano),
                            sep=";",
                            index=False)

    def gerar_nome_arquivos_caps_combinados(self):
        return f'{PASTA_CNES_DADOS_BRUTOS}/silver/cnes_filtrados.csv'

    def combinar_arquivos_ano(self):
        file_list = glob.glob(f'{PASTA_CNES_DADOS_BRUTOS}/silver/*-cnes_filtrados.csv')

        df_final = pd.DataFrame()

        for file in file_list:
            df = pd.read_csv(file, sep=";")
            df_final = pd.concat([df_final, df])

        df_final.to_csv(self.gerar_nome_arquivos_caps_combinados(),
                        sep=";",
                        index=False)


@dataclass
class CombinadoEnriquecido:
    combinado: Combinado

    def gerar_nome_arquivo_saida(self):
        return f'gold/{ANOS_CONSIDERADOS[0]}-{ANOS_CONSIDERADOS[-1]}-cnes-enriquecido.csv'

    def gerar_nome_arquivo_saida_regioes_saude_enriquecida(self):
        return "gold/regioes-saude-enriquecido.csv"

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

    def enriquecer_cnes(self):
        df_arquivos_caps_original = pd.read_csv(self.combinado.gerar_nome_arquivos_caps_combinados(),
                                                sep=";",
                                                dtype=self.combinado.gerar_dtype())
        df_arquivos_caps_original.rename(columns={"CO_MUNICIPIO_GESTOR": "MUNICIPIO_CODIGO",
                                                  "DS_SUB_TIPO": "TIPO",
                                                  "CO_ESTADO_GESTOR": "ESTADO_CODIGO"},
                                         inplace=True)

        estados_com_codigo = pd.read_csv('gold/estados.csv', sep=";")

        municipios_enriquecidos = pd.read_csv('gold/municipios.csv',
                                              sep=";",
                                              dtype={'MUNICIPIO_CODIGO': object,
                                                     'MUNICIPIO_POPULACAO': int
                                                     })

        municipios_com_soma_populacao_regiao_saude = self.gerar_regioes_saude_enriquecido(municipios_enriquecidos)

        municipios_enriquecidos = pd.merge(municipios_enriquecidos,
                                           municipios_com_soma_populacao_regiao_saude[
                                               ['ESTADO_CODIGO', 'ESTADO_SIGLA', 'ESTADO_NOME', 'REGIAO_SAUDE_CODIGO',
                                                'REGIAO_SAUDE_NOME',
                                                "REGIAO_SAUDE_POPULACAO"]],
                                           on=['ESTADO_CODIGO', 'ESTADO_SIGLA', 'ESTADO_NOME', "REGIAO_SAUDE_CODIGO",
                                               "REGIAO_SAUDE_NOME"],
                                           how="left")

        df_merge = pd.merge(df_arquivos_caps_original,
                            estados_com_codigo[['ESTADO_CODIGO', 'ESTADO_NOME', 'ESTADO_POPULACAO']],
                            on='ESTADO_CODIGO',
                            how='left')

        df_merge = pd.merge(df_merge, municipios_enriquecidos[
            ['MUNICIPIO_CODIGO',
             "MUNICIPIO_NOME",
             "MUNICIPIO_POPULACAO",
             'ESTADO_SIGLA',
             'REGIAO_SAUDE_CODIGO',
             'REGIAO_SAUDE_NOME',
             'REGIAO_SAUDE_POPULACAO']],
                            on='MUNICIPIO_CODIGO',
                            how='left')

        # Remover linhas duplicadas em todas as colunas
        df_merge = df_merge.drop_duplicates()

        # Escreva o dataframe no arquivo parquet
        df_merge.to_csv(self.gerar_nome_arquivo_saida(),
                        sep=";",
                        index=False,
                        float_format='%.0f')


def criar_arquivo_lista_tipo_caps():
    data = {
        'TIPO': [
            'CAPS AD IV',
            'CAPS ALCOOL E DROGA',
            'CAPS ALCOOL E DROGAS III - MUNICIPAL',
            'CAPS ALCOOL E DROGAS III - REGIONAL',
            'CAPS I',
            'CAPS II',
            'CAPS III',
            'CAPS INFANTO/JUVENIL'
        ],
        'Descrição': [
            'CAPS AD IV',
            'CAPS ALCOOL E DROGA',
            'CAPS ALCOOL E DROGAS III - MUNICIPAL',
            'CAPS ALCOOL E DROGAS III - REGIONAL',
            'CAPS I', 'CAPS II',
            'CAPS III',
            'CAPS INFANTO/JUVENIL'
        ]
    }

    df = pd.DataFrame(data)

    df.to_csv('gold/caps-tipos.csv', sep=";", index=False)


def criar_arquivo_lista_anos():
    data = {'ANO': ANOS_CONSIDERADOS}

    df = pd.DataFrame(data)

    df.to_csv('gold/anos.csv', sep=";", index=False)


if __name__ == "__main__":
    criar_arquivo_lista_tipo_caps()
    criar_arquivo_lista_anos()

    relacao = EstabelecimentoSubtipo(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    # relacao.filtrar_caps()

    estabelecimento = Estabelecimento(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    # estabelecimento.filtrar_caps()

    subtipo = Subtipo(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    # subtipo.filtrar_caps()

    combinado = Combinado(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO,
        tabela_relacao=relacao,
        tabela_subtipo=subtipo,
        tabela_estabelecimentos=estabelecimento
    )
    # combinado.combinar_estabelecimento_subtipo()
    # combinado.combinar_arquivos_ano()

    combinado = CombinadoEnriquecido(
        combinado=combinado
    )
    combinado.enriquecer_cnes()
