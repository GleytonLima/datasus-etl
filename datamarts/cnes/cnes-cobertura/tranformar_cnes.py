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
        tipo="object"
    )
    CO_TIPO_UNIDADE = Coluna(
        nome="CO_TIPO_UNIDADE",
        tipo="object"
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
                (arquivo_tipo_subtipo[self.colunas.CO_TIPO_UNIDADE.nome] == f'{self.codigo_caps}')]

            arquivo_tipo_subtipo.to_csv(self.gerar_nome_saida(ano),
                                        sep=";",
                                        index=False)


class EstabelecimentoSubtipoColuna:
    CO_TIPO_UNIDADE = Coluna(
        nome="CO_TIPO_UNIDADE",
        tipo="object"
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
        tipo="object"
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
            reader = pd.read_csv(self.gerar_nome_original(ano),
                                 dtype=self.gerar_dtype(),
                                 sep=";",
                                 chunksize=self.chunksize)

            writer = pd.DataFrame(columns=reader.get_chunk().columns)

            for chunk in reader:
                filtered_chunk = chunk[chunk[self.colunas.CO_TIPO_UNIDADE.nome] == f'{self.codigo_caps}']
                writer = pd.concat([writer, filtered_chunk])

            writer.to_csv(self.gerar_nome_saida(ano),
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
    colunas = CombinadoColunas()

    def gerar_dtype(self):
        return self.colunas.CO_MUNICIPIO_GESTOR.get_dtype()

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/silver/{ano}-cnes_filtrados.csv'

    def combinar_estabelecimento_subtipo(self):
        for ano in self.anos:
            tabela_estabelecimentos = Estabelecimento(
                pasta=self.pasta,
                anos=self.anos,
                mes=self.mes
            )
            tabela_relacao = EstabelecimentoSubtipo(
                pasta=self.pasta,
                anos=self.anos,
                mes=self.mes
            )
            tabela_subtipo = Subtipo(
                pasta=self.pasta,
                anos=self.anos,
                mes=self.mes
            )

            arquivo_caps = pd.read_csv(tabela_estabelecimentos.gerar_nome_saida(ano),
                                       sep=";",
                                       dtype=tabela_estabelecimentos.gerar_dtype())
            arquivo_tipo_subtipo = pd.read_csv(tabela_subtipo.gerar_nome_saida(ano),
                                               sep=";",
                                               dtype=tabela_subtipo.gerar_dtype())
            arquivo_relacao = pd.read_csv(tabela_relacao.gerar_nome_saida(ano),
                                          sep=";",
                                          dtype=tabela_relacao.gerar_dtype())

            arquivo_tipo_subtipo = arquivo_tipo_subtipo.rename(columns={
                tabela_subtipo.colunas.CO_SUB_TIPO.nome: tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome
            })
            arquivo_relacao = pd.merge(arquivo_relacao,
                                       arquivo_tipo_subtipo[[
                                           tabela_subtipo.colunas.CO_TIPO_UNIDADE.nome,
                                           tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome,
                                           tabela_subtipo.colunas.DS_SUB_TIPO.nome
                                       ]],
                                       on=[
                                           tabela_subtipo.colunas.CO_TIPO_UNIDADE.nome,
                                           tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome
                                       ],
                                       how="left")

            df_merge = pd.merge(arquivo_caps,
                                arquivo_relacao[[
                                    tabela_relacao.colunas.CO_UNIDADE.nome,
                                    tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome,
                                    tabela_subtipo.colunas.DS_SUB_TIPO.nome
                                ]],
                                on=tabela_relacao.colunas.CO_UNIDADE.nome,
                                how='left')
            df_merge[tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome] = df_merge[
                tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome].fillna(
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

    def enriquecer_cnes(self):
        df_parquet = pd.read_csv(self.combinado.gerar_nome_arquivos_caps_combinados(),
                                 dtype=self.combinado.gerar_dtype())
        df_parquet = df_parquet.rename(columns={"CO_MUNICIPIO_GESTOR": "CODUFMUN", "DS_SUB_TIPO": "TIPO"})

        df_municipios_com_regionais_saude = pd.read_csv('estados-cidades/municipios-com-nome-regiao-saude.csv',
                                                        dtype={'Município': object, 'Nome da Região de Saúde': object,
                                                               'Cód IBGE': object})
        df_populacao_municipio_csv = pd.read_csv('populacao/POP2022_Municipios.csv',
                                                 dtype={'CODUFMUN': object, 'POPULACAO': object})
        df_estados = pd.read_csv('estados-cidades/estados.csv',
                                 dtype={'ESTADO_NOME': object, 'POPULACAO_UF': object})
        df_estados = df_estados.rename(columns={"codigo_uf": "CO_ESTADO_GESTOR", "nome": "ESTADO_NOME"})
        df_populacao_estado_csv = pd.read_csv('populacao/POP2022_Brasil_e_UFs.csv',
                                              dtype={'ESTADO_NOME': object, 'POPULACAO_UF': object})
        df_populacao_estado_csv = pd.merge(df_populacao_estado_csv, df_estados, on="ESTADO_NOME", how="left")

        # Renomeie as colunas para que correspondam
        df_municipios_com_regionais_saude.rename(
            columns={
                'Município': 'MUNICIPIO_NOME',
                'Nome da Região de Saúde': 'NOME_REGIAO_SAUDE',
                'Cód Região de Saúde': 'COD_REGIAO_SAUDE',
                'Cód IBGE': 'CODUFMUN'},
            inplace=True)
        df_municipios_com_regionais_saude = pd.merge(df_municipios_com_regionais_saude,
                                                     df_populacao_municipio_csv[['CODUFMUN', 'POPULACAO']],
                                                     on='CODUFMUN',
                                                     how='left')

        # Converter a coluna "POPULACAO" em um tipo numérico
        df_municipios_com_regionais_saude["POPULACAO"] = pd.to_numeric(df_municipios_com_regionais_saude["POPULACAO"])

        # Agrupar por NOME_REGIAO_SAUDE, ANO, MES e TIPO e calcular a soma de POPULACAO
        df_grouped = df_municipios_com_regionais_saude.groupby(['UF', 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE']).agg(
            {'POPULACAO': 'sum'}).reset_index()

        # Renomear a coluna POPULACAO para POPULACAO_REGIAO_SAUDE
        df_grouped = df_grouped.rename(columns={'POPULACAO': 'POPULACAO_REGIAO_SAUDE'})

        df_regioes_saude_enriquecido = df_grouped[
            ['UF', 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE', "POPULACAO_REGIAO_SAUDE"]]
        df_regioes_saude_enriquecido = df_regioes_saude_enriquecido.drop_duplicates()
        df_regioes_saude_enriquecido.to_csv("estados-cidades/regioes-saude-enriquecido.csv", index=False)

        # Selecionar as colunas desejadas
        df_municipios_com_regionais_saude = pd.merge(df_municipios_com_regionais_saude,
                                                     df_regioes_saude_enriquecido[
                                                         ['UF', 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE',
                                                          "POPULACAO_REGIAO_SAUDE"]],
                                                     on=["UF", "COD_REGIAO_SAUDE", "NOME_REGIAO_SAUDE"],
                                                     how="left")

        # Use o método merge para combinar os dataframes com base na coluna 'CODUFMUN'
        df_merge = pd.merge(df_parquet, df_populacao_municipio_csv[['CODUFMUN', 'POPULACAO']], on='CODUFMUN',
                            how='left')

        # Use o método merge para combinar os dataframes com base na coluna 'ESTADO_NOME'
        df_merge = pd.merge(df_merge,
                            df_populacao_estado_csv[['CO_ESTADO_GESTOR', 'ESTADO_NOME', 'POPULACAO_UF']],
                            on='CO_ESTADO_GESTOR',
                            how='left')

        # Use o método merge para combinar os dataframes com base na coluna 'ESTADO_NOME'
        df_merge = pd.merge(df_merge, df_municipios_com_regionais_saude[
            ['CODUFMUN', "MUNICIPIO_NOME", 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE', 'POPULACAO_REGIAO_SAUDE']],
                            on='CODUFMUN', how='left')

        # Remover linhas duplicadas em todas as colunas
        df_merge = df_merge.drop_duplicates()

        # Escreva o dataframe no arquivo parquet
        df_merge.to_csv('2018-2022-cnes-enriquecido.csv', index=False)


if __name__ == "__main__":
    relacao = EstabelecimentoSubtipo(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    relacao.filtrar_caps()

    estabelecimento = Estabelecimento(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    estabelecimento.filtrar_caps()

    subtipo = Subtipo(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    subtipo.filtrar_caps()

    combinado = Combinado(
        pasta=PASTA_CNES_DADOS_BRUTOS,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    combinado.combinar_estabelecimento_subtipo()
    combinado.combinar_arquivos_ano()

    CombinadoEnriquecido(
        combinado=combinado
    )
