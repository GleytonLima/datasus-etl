import dataclasses
import glob
import os
from dataclasses import dataclass

import pandas as pd

from base import ColunasSds, Municipio
from caps import Coluna
from extract.download_util import DowloadDataSusFtp, DonwloadDataSusConfig

pd.options.display.max_columns = None

def path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


@dataclass
class EquipeColunas:
    TIPO_EQP = Coluna(
        nome="TIPO_EQP",
        tipo="str",
        descricao="Código do tipo de equipe"
    )
    COMPETEN = Coluna(
        nome="COMPETEN",
        tipo="str",
        descricao="Competencia no formato YYYYMM"
    )
    CODUFMUN = Coluna(
        nome="CODUFMUN",
        tipo="str",
        descricao="Códigos de municipio"
    )
    TOTAL_EQUIPES = Coluna(
        nome="TOTAL_EQUIPES",
        tipo="Int64",
        descricao="Total de equipes"
    )
    TIPO_EQP_CLASSIFICACAO = Coluna(
        nome="TIPO_EQP_CLASSIFICACAO",
        tipo="str",
        descricao="Tipo de classificação das equipes"
    )


@dataclasses.dataclass
class TiposEquipe:
    NASF = Coluna(
        nome="NASF",
        tipo="str",
        descricao="Núcleo de Apoio à Saúde da Família"
    )
    ESF = Coluna(
        nome="ESF",
        tipo="str",
        descricao="Equipe de Saúde da Família"
    )


@dataclasses.dataclass
class TransformarEquipesCnes:
    classificacao_equipe_esf_eap: dict
    nome_arquivo_saida: str
    download_cnes_ep = DowloadDataSusFtp(config=DonwloadDataSusConfig(
        system="CNES",
        subsystem="EP"
    ))

    def gerar_nome_arquivo_saida_silver(self):
        return f'{path("/data/silver/datasus/cnes")}/{self.nome_arquivo_saida}'

    def gerar_nome_arquivo_saida_gold(self):
        return f'{path("/data/gold/datasus/cnes")}/{self.nome_arquivo_saida}'

    def filtrar_tipos_equipes(self):
        all_files = glob.glob(f"{self.download_cnes_ep.gerar_path_arquivos_saida()}/*.csv")
        dfs = []
        for filename in all_files:
            df = pd.read_csv(filename, sep=';')
            dfs.append(df)

        df = pd.concat(dfs, ignore_index=True)

        df.dropna(subset=[EquipeColunas.TIPO_EQP.nome], inplace=True)
        df.dropna(subset=[EquipeColunas.COMPETEN.nome], inplace=True)
        keys = list(self.classificacao_equipe_esf_eap.keys())
        print(f"lista de codigos de equipe: {keys}")
        df = df[df[EquipeColunas.TIPO_EQP.nome].isin(keys)]

        # Converter a coluna COMPETEN para string
        df[EquipeColunas.COMPETEN.nome] = df[EquipeColunas.COMPETEN.nome].astype("str")

        df[ColunasSds.ANO.nome] = df[EquipeColunas.COMPETEN.nome].str.slice(stop=4).astype(int)
        # Filtrar as linhas com anos entre 2013 e 2022
        df = df[df[ColunasSds.ANO.nome].between(2013, 2099)]

        # Criar coluna mes com ultimos digitos de COMPETEN
        df[ColunasSds.MES.nome] = df[EquipeColunas.COMPETEN.nome].str.slice(start=4)

        # Converter a coluna "MES" para inteiro
        df[ColunasSds.MES.nome] = pd.to_numeric(df[ColunasSds.MES.nome], errors="coerce").fillna(0).astype(int)

        # Excluir a coluna COMPETEN original
        df.drop(EquipeColunas.COMPETEN.nome, axis=1, inplace=True)

        df = df.rename(columns={EquipeColunas.CODUFMUN.nome: ColunasSds.MUNICIPIO_CODIGO.nome})

        result = df.groupby([ColunasSds.MUNICIPIO_CODIGO.nome, ColunasSds.ANO.nome, ColunasSds.MES.nome,
                             EquipeColunas.TIPO_EQP.nome]).size().reset_index(
            name=ColunasSds.TOTAL_EQUIPES.nome)

        result.to_csv(self.gerar_nome_arquivo_saida_silver(), sep=';', index=False)

        print(f"#ETL0024 Arquivo {self.gerar_nome_arquivo_saida_silver()} salvo com sucesso!")
        print(result.head())

    def enriquecer_tipos_equipes(self):
        df_equipes_silver = pd.read_csv(self.gerar_nome_arquivo_saida_silver(), delimiter=";")
        print(f"#ETL0025 dataframe com os tipos de equipe")
        print(df_equipes_silver.head())

        df_municipios = pd.read_csv(Municipio().gerar_nome_arquivo_saida(), delimiter=';')

        # inicio preenchendo vazios
        # criando um dataframe com todos os pares de estado/ano possíveis
        codigos_municipios = df_municipios[ColunasSds.MUNICIPIO_CODIGO.nome].unique()
        ano_min = df_equipes_silver[ColunasSds.ANO.nome].min()
        ano_max = df_equipes_silver[ColunasSds.ANO.nome].max()
        anos = pd.date_range(start=str(int(ano_min)), end=str(int(ano_max) + 1), freq='Y').year
        tipos_equipe = list(self.classificacao_equipe_esf_eap.keys())
        df3 = pd.DataFrame(
            [(m, t, a) for m in codigos_municipios for a in anos for t in tipos_equipe],
            columns=[ColunasSds.MUNICIPIO_CODIGO.nome, EquipeColunas.TIPO_EQP.nome, ColunasSds.ANO.nome])
        print(f"#ETL0026 dataframe com os tipos de equipe selecionados")
        print(df3.head())
        # fazendo um left join entre o dataframe criado acima e o dataframe original "df2"
        df4 = pd.merge(df3, df_equipes_silver, how='left',
                       on=[ColunasSds.MUNICIPIO_CODIGO.nome, EquipeColunas.TIPO_EQP.nome, ColunasSds.ANO.nome])
        # preenchendo com 0 os valores nulos da coluna "total"
        df4[EquipeColunas.TOTAL_EQUIPES.nome] = df4[EquipeColunas.TOTAL_EQUIPES.nome].fillna(0)
        df4[ColunasSds.MES.nome] = df4[ColunasSds.MES.nome].fillna(12)

        print(f"#ETL0027 dataframe com os tipos de equipe selecionados e preenchidos com 0")
        print(df4.head())

        # agregando por estado e ano, somando os valores da coluna "total"
        df_equipes_silver = df4.groupby(
            [ColunasSds.MUNICIPIO_CODIGO.nome, EquipeColunas.TIPO_EQP.nome, ColunasSds.ANO.nome, ColunasSds.MES.nome],
            as_index=False).agg(
            {ColunasSds.TOTAL_EQUIPES.nome: 'sum'})

        # fim preenchendo vazios
        print("df_municipios")
        print(df_municipios)
        print("df_equipes")
        print(df_equipes_silver)
        # Junta os dataframes com base na coluna "MUNICIPIO_CODIGO"
        df_merged = pd.merge(df_municipios, df_equipes_silver, on=ColunasSds.MUNICIPIO_CODIGO.nome, how='left')
        df_merged[ColunasSds.TOTAL_EQUIPES.nome] = df_merged[ColunasSds.TOTAL_EQUIPES.nome].fillna(0)

        # Converte a coluna "MUNICIPIO_CODIGO" para o tipo de dados inteiro
        df_merged[ColunasSds.MUNICIPIO_CODIGO.nome] = df_merged[ColunasSds.MUNICIPIO_CODIGO.nome].astype(int)
        df_merged[ColunasSds.ANO.nome] = df_merged[ColunasSds.ANO.nome].astype('Int64')
        df_merged[ColunasSds.MES.nome] = df_merged[ColunasSds.MES.nome].astype('Int64')
        df_merged[ColunasSds.TOTAL_EQUIPES.nome] = df_merged[ColunasSds.TOTAL_EQUIPES.nome].astype('Int64')

        df_merged[EquipeColunas.TIPO_EQP_CLASSIFICACAO.nome] = df_merged[EquipeColunas.TIPO_EQP.nome].map(
            self.classificacao_equipe_esf_eap)

        df_merged.to_csv(self.gerar_nome_arquivo_saida_gold(), sep=';', index=False)
        print(f"#ETL0028 Arquivo {self.gerar_nome_arquivo_saida_gold()} salvo com sucesso!")
        print(df_merged.head())


@dataclasses.dataclass
class CalculoCobertura:
    arquivo_equipes_esf_nasf: str
    nome_arquivo_saida: str

    def gerar_nome_arquivo_saida_gold(self):
        return f'{path("/data/gold/datasus/cnes")}/{self.nome_arquivo_saida}'

    def calcular_cobertura_municipio_sem_nasf(self):
        print(f"Calculando a cobertura de ESF sem NASF para o arquivo {self.arquivo_equipes_esf_nasf}...")
        df = pd.read_csv(self.arquivo_equipes_esf_nasf, delimiter=';')

        # Cria uma tabela dinâmica com as informações solicitadas
        table = pd.pivot_table(df,
                               values=ColunasSds.TOTAL_EQUIPES.nome,
                               index=[ColunasSds.MUNICIPIO_CODIGO.nome,
                                      ColunasSds.MUNICIPIO_NOME.nome,
                                      ColunasSds.MUNICIPIO_POPULACAO.nome,
                                      ColunasSds.ESTADO_CODIGO.nome,
                                      ColunasSds.ESTADO_SIGLA.nome,
                                      ColunasSds.ESTADO_NOME.nome,
                                      ColunasSds.REGIAO_SAUDE_CODIGO.nome,
                                      ColunasSds.REGIAO_SAUDE_NOME.nome,
                                      ColunasSds.ANO.nome,
                                      ColunasSds.MES.nome],
                               columns=[ColunasSds.TIPO_EQP_CLASSIFICACAO.nome],
                               aggfunc='sum')
        # imprime primeiras linhas da tabela
        print("tabela pivot")
        print(table.head())

        # Filtra as linhas que atendem aos critérios especificados
        filtro = (table[TiposEquipe.NASF.nome] == 0) & (table[TiposEquipe.ESF.nome] != 0)
        table_filtrada = table.loc[filtro]

        print("table_filtrada")
        print(table_filtrada.head())
        # Calcula a coluna COBERTURA_ESAF
        table_filtrada = table_filtrada.reset_index()
        table_filtrada[ColunasSds.COBERTURA_ESAF.nome] = table_filtrada[TiposEquipe.ESF.nome] * 3450 * 100 / \
                                                         table_filtrada[ColunasSds.MUNICIPIO_POPULACAO.nome]

        # Reorganiza a tabela final
        table_final = table_filtrada.reset_index()[
            [
                ColunasSds.MUNICIPIO_CODIGO.nome,
                ColunasSds.MUNICIPIO_NOME.nome,
                ColunasSds.MUNICIPIO_POPULACAO.nome,
                ColunasSds.ESTADO_CODIGO.nome,
                ColunasSds.ESTADO_SIGLA.nome,
                ColunasSds.ESTADO_NOME.nome,
                ColunasSds.REGIAO_SAUDE_CODIGO.nome,
                ColunasSds.REGIAO_SAUDE_NOME.nome,
                ColunasSds.ANO.nome,
                ColunasSds.MES.nome,
                TiposEquipe.ESF.nome,
                TiposEquipe.NASF.nome,
                ColunasSds.COBERTURA_ESAF.nome]
        ]

        # Salva o resultado em um novo arquivo CSV
        table_final.to_csv(self.gerar_nome_arquivo_saida_gold(), sep=";", index=False)
        print(f"#ETL0029 Arquivo {self.gerar_nome_arquivo_saida_gold()} salvo com sucesso!")
