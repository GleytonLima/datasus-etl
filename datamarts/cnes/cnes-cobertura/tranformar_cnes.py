from dataclasses import dataclass
from typing import List

import pandas as pd

ANOS_CONSIDERADOS = [2018, 2019, 2020, 2021, 2022]

MES_COMPETENCIA_CONSIDERADO = "12"

pasta_com_dados_brutos_cnes = "cnes-dadosbrutos"


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
        return f'{self.pasta}/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/{self.gerar_nome_base(ano, self.mes)}_caps.csv'

    def filtrar_caps(self):
        for ano in self.anos:
            reader = pd.read_csv(self.gerar_nome_original(ano),
                                 dtype=self.gerar_dtype(),
                                 sep=";",
                                 chunksize=self.chunksize)

            writer = pd.DataFrame(columns=reader.get_chunk().columns)

            for chunk in reader:
                filtered_chunk = chunk[(chunk[self.colunas.TP_UNIDADE.nome] == self.codigo_caps) & (
                    chunk[self.colunas.CO_MOTIVO_DESAB.nome].isna())]
                filtered_chunk = filtered_chunk.assign(ANO=ano, MES=self.mes)
                writer = pd.concat([writer, filtered_chunk])

            writer.to_csv(self.gerar_nome_saida(ano),
                          index=False,
                          sep=';',
                          columns=self.gerar_colunas_saida())


@dataclass
class SubtipoColuna:
    CO_UNIDADE = Coluna(
        nome="CO_UNIDADE",
        tipo="object"
    )
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
        return self.colunas.CO_UNIDADE.get_dtype() | self.colunas.CO_SUB_TIPO.get_dtype() | self.colunas.CO_TIPO_UNIDADE.get_dtype()

    def gerar_nome_base(self, ano, mes):
        return f"{self.nome}{ano}{mes}"

    def gerar_nome_original(self, ano):
        return f'{self.pasta}/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/{ano}-cnes_filtrados.csv'

    def combinar_estabelecimento_com_subtipo(self):
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

            arquivo_caps = pd.read_csv(tabela_estabelecimentos.gerar_nome_saida(ano),
                                       sep=";",
                                       dtype=tabela_estabelecimentos.gerar_dtype())
            arquivo_tipo_subtipo = pd.read_csv(self.gerar_nome_original(ano),
                                               sep=";",
                                               dtype=self.gerar_dtype())
            arquivo_relacao = pd.read_csv(tabela_relacao.gerar_nome_saida(ano),
                                          sep=";",
                                          dtype=tabela_relacao.gerar_dtype())

            arquivo_tipo_subtipo = arquivo_tipo_subtipo.rename(columns={
                self.colunas.CO_SUB_TIPO.nome: tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome
            })
            arquivo_relacao = pd.merge(arquivo_relacao,
                                       arquivo_tipo_subtipo[[
                                           self.colunas.CO_TIPO_UNIDADE.nome,
                                           tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome,
                                           self.colunas.DS_SUB_TIPO.nome
                                       ]],
                                       on=[
                                           self.colunas.CO_TIPO_UNIDADE.nome,
                                           tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome
                                       ],
                                       how="left")

            df_merge = pd.merge(arquivo_caps,
                                arquivo_relacao[[
                                    self.colunas.CO_UNIDADE.nome,
                                    tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome,
                                    self.colunas.DS_SUB_TIPO.nome
                                ]],
                                on=self.colunas.CO_UNIDADE.nome,
                                how='left')
            df_merge[tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome] = df_merge[
                tabela_relacao.colunas.CO_SUB_TIPO_UNIDADE.nome].fillna(
                0).astype(int)
            df_merge = df_merge.drop_duplicates()
            df_merge.to_csv(self.gerar_nome_saida(ano), index=False)


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
        return f'{self.pasta}/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'{self.pasta}/{self.gerar_nome_base(ano, self.mes)}.csv'

    def filtrar_caps(self):
        for ano in self.anos:
            reader = pd.read_csv(self.gerar_nome_original(ano),
                                 dtype=self.gerar_dtype(),
                                 sep=";",
                                 chunksize=self.chunksize)

            writer = pd.DataFrame(columns=reader.get_chunk().columns)

            for chunk in reader:
                filtered_chunk = chunk[chunk[self.colunas.CO_TIPO_UNIDADE.nome] == self.codigo_caps]
                writer = pd.concat([writer, filtered_chunk])

            writer.to_csv(self.gerar_nome_saida(ano),
                          sep=';',
                          index=False)


def filtrar_subtipos():
    tabela = EstabelecimentoSubtipo(
        pasta=pasta_com_dados_brutos_cnes,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    tabela.filtrar_caps()


def filtrar_caps():
    tabela = Estabelecimento(
        pasta=pasta_com_dados_brutos_cnes,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    tabela.filtrar_caps()


def combinar_caps_com_tipo():
    tabela = Subtipo(
        pasta=pasta_com_dados_brutos_cnes,
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    tabela.combinar_estabelecimento_com_subtipo()


if __name__ == "__main__":
    filtrar_subtipos()
    filtrar_caps()
    combinar_caps_com_tipo()
