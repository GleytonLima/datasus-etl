import glob
from dataclasses import dataclass
from typing import List

import pandas as pd

ANOS_CONSIDERADOS = [2018, 2019, 2020, 2021, 2022]

PREFIXO_NOME_ANOS = f'{ANOS_CONSIDERADOS[0]}-{ANOS_CONSIDERADOS[-1]}'

ANOS_LEITOS_CONSIDERADOS = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]

MES_COMPETENCIA_CONSIDERADO = "12"


class Populacao:
    def gerar_nome_arquivo_populacao_estado_entrada(self):
        return 'bronze/POP2022_Brasil_e_UFs.xls'

    def gerar_nome_arquivo_populacao_estado_saida(self):
        return 'silver/POP2022_Brasil_e_UFs.csv'

    def gerar_nome_arquivo_populacao_municipio_entrada(self):
        return 'bronze/POP2022_Municipios.xls'

    def gerar_nome_arquivo_populacao_municipio_saida(self):
        return 'silver/POP2022_Municipios.csv'

    def obter_detalhes_populacao_por_estado(self):
        # Carrega o arquivo XLS em um DataFrame do Pandas
        df = pd.read_excel(self.gerar_nome_arquivo_populacao_estado_entrada(), skiprows=[0], header=[0],
                           usecols=lambda x: 'Unnamed' not in x)

        # Remove as linhas em que a primeira coluna está em branco
        df = df.dropna(subset=[df.columns[0]])

        df = df[df['BRASIL E UNIDADES DA FEDERAÇÃO'].isin(["Brasil",
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
                                                           ])]

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

    def obter_detalhes_populacao_por_municipio(self):
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
        return 'bronze/estados-originais.csv'

    def gerar_nome_arquivo_saida(self):
        return 'gold/estados.csv'

    def enriquecer_estados(self):
        estados_com_codigo = pd.read_csv(self.gerar_nome_arquivo_entrada())
        estados_com_codigo.rename(
            columns={'codigo_uf': 'ESTADO_CODIGO', 'uf': 'ESTADO_SIGLA', 'nome': 'ESTADO_NOME',
                     'regiao': 'ESTADO_REGIAO'},
            inplace=True)
        estados_somente_nome_populacao = pd.read_csv('silver/POP2022_Brasil_e_UFs.csv',
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


class Municipio:
    def gerar_nome_arquivo_entrada(self):
        return 'bronze/municipios-originais.csv'

    def gerar_nome_arquivo_saida(self):
        return 'gold/municipios.csv'

    def enriquecer_municipios(self):
        # Carregue os arquivos CSV e parquet em dataframes pandas
        df_municipios = pd.read_csv(self.gerar_nome_arquivo_entrada())
        df_municipios['codigo_ibge'] = df_municipios['codigo_ibge'].astype(str).str[:-1]
        df_municipios.rename(
            columns={'codigo_ibge': 'MUNICIPIO_CODIGO', 'nome': 'MUNICIPIO_NOME', 'codigo_uf': 'ESTADO_CODIGO'},
            inplace=True)

        df_estados = pd.read_csv(Estado().gerar_nome_arquivo_saida(), sep=';')

        df_populacao_municipio = pd.read_csv('silver/POP2022_Municipios.csv',
                                             sep=';',
                                             dtype={'MUNICIPIO_CODIGO': object})

        df_municipios_com_regionais_saude = pd.read_csv('bronze/municipios-com-nome-regiao-saude.csv',
                                                        dtype={'Município': object,
                                                               'Nome da Região de Saúde': object,
                                                               'Cód IBGE': object})

        # Renomeie as colunas para que correspondam
        df_municipios_com_regionais_saude.rename(
            columns={
                'UF': 'ESTADO_SIGLA',
                'Município': 'MUNICIPIO_NOME',
                'Nome da Região de Saúde': 'REGIAO_SAUDE_NOME',
                'Cód Região de Saúde': 'REGIAO_SAUDE_CODIGO',
                'Cód IBGE': 'MUNICIPIO_CODIGO'},
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

        df_municipios_df_com_regionais_saude = pd.read_csv('gold/municipios_df.csv', sep=";")

        # deletando unico registro do DF para substituir pelas regioes administrativas
        df_municipios = df_municipios[df_municipios['MUNICIPIO_CODIGO'] != 530010]
        df_municipios = df_municipios[df_municipios['MUNICIPIO_CODIGO'] != "530010"]

        df_municipios = pd.concat([df_municipios, df_municipios_df_com_regionais_saude])

        df_municipios.to_csv(self.gerar_nome_arquivo_saida(),
                             sep=';',
                             index=False)


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
        return f'bronze/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'silver/{self.gerar_nome_base(ano, self.mes)}_caps.csv'

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
        return f'bronze/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'silver/{ano}_caps.csv'

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
        return f'bronze/{self.gerar_nome_base(ano, self.mes)}.csv'

    def gerar_nome_saida(self, ano):
        return f'silver/{self.gerar_nome_base(ano, self.mes)}_caps.csv'

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
    tabela_estabelecimentos: Estabelecimento
    tabela_relacao: EstabelecimentoSubtipo
    tabela_subtipo: Subtipo
    colunas = CombinadoColunas()

    def gerar_dtype(self):
        return self.colunas.CO_MUNICIPIO_GESTOR.get_dtype()

    def gerar_nome_saida(self, ano):
        return f'silver/{ano}-cnes_filtrados.csv'

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

            # enriquecendo DF com codigos de municipios ficticios dado que o IBGE nao mapea as areas
            # administrativas de la
            # precisamos tratar as areas de saude do df
            df_cep_municipio_regiao_saude = pd.read_csv('gold/df_cep_municipio_regiao_saude.csv', sep=";")

            # fazendo o merge dos dataframes com base na coluna CO_CEP
            df_merged = pd.merge(df_merge, df_cep_municipio_regiao_saude[
                ['CO_CEP', 'CO_MUNICIPIO_GESTOR']], on='CO_CEP',
                                 how='left')

            # Preenche valores nulos nas colunas do lado direito com os valores originais nas colunas do lado esquerdo
            df_merged['CO_MUNICIPIO_GESTOR_y'].fillna(df_merged['CO_MUNICIPIO_GESTOR_x'], inplace=True)

            # renomeando as colunas para substituir as colunas originais do primeiro dataframe
            df_merged = df_merged.rename(
                columns={'CO_MUNICIPIO_GESTOR_y': 'CO_MUNICIPIO_GESTOR'})

            # excluindo as colunas originais do primeiro dataframe que foram substituídas
            df_merged = df_merged.drop(['CO_MUNICIPIO_GESTOR_x'], axis=1)

            df_merged['CO_MUNICIPIO_GESTOR'] = df_merged['CO_MUNICIPIO_GESTOR'].astype(int)

            df_merged.to_csv(self.gerar_nome_saida(ano),
                            sep=";",
                            index=False)

    def gerar_nome_arquivos_caps_combinados(self):
        return f'silver/cnes_filtrados.csv'

    def combinar_arquivos_ano(self):
        file_list = glob.glob(f'silver/*-cnes_filtrados.csv')

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
        return f'gold/{PREFIXO_NOME_ANOS}-cnes-enriquecido.csv'

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

        df_merge_merge_com_municipios_enriquecidos = pd.merge(df_merge, municipios_enriquecidos[
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
        df_merged = df_merge_merge_com_municipios_enriquecidos.drop_duplicates()

        # Escreva o dataframe no arquivo parquet
        df_merged.to_csv(self.gerar_nome_arquivo_saida(),
                        sep=";",
                        index=False,
                        float_format='%.0f')


multiplicador = {
    'CAPS I': 0.5,
    'CAPS II': 1,
    'CAPS ALCOOL E DROGAS III - MUNICIPAL': 1,
    'CAPS ALCOOL E DROGAS III - REGIONAL': 1,
    'CAPS AD IV': 5,
    'CAPS ALCOOL E DROGA': 1,
    'CAPS INFANTO/JUVENIL': 1,
    'CAPS III': 1.5,
    'Não informado': 0
}


class DatamartIndicadorCapsMunicipio:
    def calcular_valor_municipio(self, row):
        return row['MUNICIPIO_QTDE_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['MUNICIPIO_POPULACAO']

    def criar_datamart_com_indices_cobertura_caps_por_municipio(self):
        # Carrega o arquivo CSV em um DataFrame do Pandas
        df_cnes_enriquecido = pd.read_csv(f'gold/{PREFIXO_NOME_ANOS}-cnes-enriquecido.csv', sep=';')

        # df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)
        df_cnes_enriquecido['TIPO'].dropna()

        df_cnes_enriquecido['TIPO'] = df_cnes_enriquecido['TIPO'].replace(
            {'CAPS ALCOOL E DROGAS III - MUNICIPAL': 'CAPS ALCOOL E DROGA',
             'CAPS ALCOOL E DROGAS III - REGIONAL': 'CAPS ALCOOL E DROGA'})

        # Agrupa as linhas e conta o número de linhas em cada grupo
        df_grouped = df_cnes_enriquecido.groupby(
            ['MUNICIPIO_CODIGO',
             'CO_CEP',
             'MUNICIPIO_NOME',
             'ESTADO_CODIGO',
             'ESTADO_SIGLA',
             'ESTADO_NOME',
             'REGIAO_SAUDE_CODIGO',
             'REGIAO_SAUDE_NOME',
             'ANO',
             'MES',
             'MUNICIPIO_POPULACAO',
             'TIPO']).size().reset_index(name='MUNICIPIO_QTDE_CAPS')

        # Imprime o DataFrame resultante
        # aplica a função para criar a nova coluna
        df_grouped['MUNICIPIO_IC'] = df_grouped.apply(self.calcular_valor_municipio, axis=1)

        print(df_grouped)
        ### preencher municipios vazios com zeros
        df_municipios = pd.read_csv('gold/municipios.csv',
                                    sep=";",
                                    dtype={'MUNICIPIO_CODIGO': object,
                                           'ESTADO_CODIGO': object,
                                           'REGIAO_SAUDE_CODIGO': object
                                           })
        # Merge dos DataFrames df_grouped e df_municipios
        # Convertendo as colunas-chave para o tipo correto
        df_grouped['MUNICIPIO_CODIGO'] = df_grouped['MUNICIPIO_CODIGO'].astype(int)
        df_grouped['ESTADO_CODIGO'] = df_grouped['ESTADO_CODIGO'].astype(int)
        df_grouped['REGIAO_SAUDE_CODIGO'] = df_grouped['REGIAO_SAUDE_CODIGO'].astype(int)

        df_municipios['MUNICIPIO_CODIGO'] = df_municipios['MUNICIPIO_CODIGO'].astype(int)
        df_municipios['ESTADO_CODIGO'] = df_municipios['ESTADO_CODIGO'].astype(int)
        df_municipios['REGIAO_SAUDE_CODIGO'] = df_municipios['REGIAO_SAUDE_CODIGO'].astype(int)

        # Merge dos DataFrames df_grouped e df_municipios
        df_merged = pd.merge(df_grouped, df_municipios, on=['MUNICIPIO_CODIGO', 'ESTADO_CODIGO', 'REGIAO_SAUDE_CODIGO'],
                             how='outer')

        # Selecionar as linhas que estão em df_municipios mas não em df_grouped
        novas_linhas = df_merged[df_merged['MUNICIPIO_NOME_x'].isnull()]

        # Lista de ANOS e CAPS
        caps = ['CAPS I']

        # Lista para armazenar as novas linhas
        novas_linhas_combinadas = []

        # Iterar sobre as novas linhas
        for _, row in novas_linhas.iterrows():
            municipio_codigo = row['MUNICIPIO_CODIGO']
            municipio_nome = row['MUNICIPIO_NOME_y']
            municipio_populacao = row["MUNICIPIO_POPULACAO_y"]
            estado_codigo = row['ESTADO_CODIGO']
            estado_sigla = row['ESTADO_SIGLA_y']
            estado_nome = row['ESTADO_NOME_y']
            codigo_cep = row['CO_CEP']
            regiao_saude_codigo = row['REGIAO_SAUDE_CODIGO']
            regiao_saude_nome = row['REGIAO_SAUDE_NOME_y']

            # Criar combinações de anos e tipos de CAPS
            for ano in ANOS_CONSIDERADOS:
                for cap in caps:
                    nova_linha = {
                        'MUNICIPIO_CODIGO': municipio_codigo,
                        'CO_CEP': codigo_cep,
                        'MUNICIPIO_NOME': municipio_nome,
                        'ESTADO_CODIGO': estado_codigo,
                        'ESTADO_SIGLA': estado_sigla,
                        'ESTADO_NOME': estado_nome,
                        'REGIAO_SAUDE_CODIGO': regiao_saude_codigo,
                        'REGIAO_SAUDE_NOME': regiao_saude_nome,
                        'ANO': ano,
                        'MES': 12,
                        'MUNICIPIO_POPULACAO': municipio_populacao,
                        'TIPO': cap,
                        'MUNICIPIO_QTDE_CAPS': 0,
                        'MUNICIPIO_IC': 0
                    }
                    novas_linhas_combinadas.append(nova_linha)

        # Converter a lista de novas linhas combinadas em um DataFrame
        novas_linhas_df = pd.DataFrame(novas_linhas_combinadas)

        # Adicionar as novas linhas ao df_grouped
        df_grouped = pd.concat([df_grouped, novas_linhas_df])

        # Resetar o índice do DataFrame resultante
        df_grouped = df_grouped.reset_index(drop=True)


        df_grouped.to_csv(F'gold/{PREFIXO_NOME_ANOS}-caps-agrupados-por-tipo.csv', sep=';', index=False, decimal=',')


class DatamartIndicadorCapsEstado:
    def calcular_valor_estado(self, row):
        return row['ESTADO_QTDE_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['ESTADO_POPULACAO']

    def criar_datamart_com_indices_cobertura_caps_por_estados(self):
        # Carrega o arquivo CSV em um DataFrame do Pandas
        df_cnes_enriquecido = pd.read_csv(f'gold/{PREFIXO_NOME_ANOS}-cnes-enriquecido.csv', sep=';')

        df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

        # Agrupa as linhas e conta o número de linhas em cada grupo
        df_grouped = df_cnes_enriquecido.groupby(
            ['ESTADO_CODIGO',
             'ESTADO_SIGLA',
             'ESTADO_NOME',
             'CO_CEP',
             'ANO',
             'MES',
             'ESTADO_POPULACAO',
             'TIPO']).size().reset_index(name='ESTADO_QTDE_CAPS')

        # Imprime o DataFrame resultante
        # aplica a função para criar a nova coluna
        df_grouped['ESTADO_IC'] = df_grouped.apply(self.calcular_valor_estado, axis=1)

        print(df_grouped)
        df_grouped.to_csv(f'gold/{PREFIXO_NOME_ANOS}-caps-agrupados-por-tipo-por-uf.csv', sep=';', index=False,
                          decimal=',')


class DatamartIndicadorCapsRegiaoSaude:
    def calcular_valor_regiao_saude(self, row):
        return row['REGIAO_SAUDE_QTDE_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['REGIAO_SAUDE_POPULACAO']

    def criar_datamart_com_indices_cobertura_caps_por_regiao_saude(self):
        # Carrega o arquivo CSV em um DataFrame do Pandas
        df_cnes_enriquecido = pd.read_csv(f'gold/{PREFIXO_NOME_ANOS}-cnes-enriquecido.csv', sep=';')
        df_cnes_enriquecido = df_cnes_enriquecido.rename(columns={'CO_ESTADO_GESTOR': 'codigo_uf'})
        df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

        # Agrupa as linhas e conta o número de linhas em cada grupo
        cols = ['ESTADO_CODIGO',
                'ESTADO_SIGLA',
                'ESTADO_NOME',
                'REGIAO_SAUDE_CODIGO',
                'REGIAO_SAUDE_NOME',
                'CO_CEP',
                'ANO',
                'MES',
                'REGIAO_SAUDE_POPULACAO',
                'TIPO']
        df_grouped = df_cnes_enriquecido.groupby(cols).size().reset_index(name='REGIAO_SAUDE_QTDE_CAPS')
        df_grouped = df_grouped[cols + ['REGIAO_SAUDE_QTDE_CAPS']]
        # Imprime o DataFrame resultante
        # aplica a função para criar a nova coluna
        df_grouped['REGIAO_SAUDE_IC'] = df_grouped.apply(self.calcular_valor_regiao_saude, axis=1)

        print(df_grouped)

        # TODO: Preencher os regionais vazios com zero
        ### preencher municipios vazios com zeros
        df_regioes_saude = pd.read_csv('gold/regioes-saude-enriquecido.csv',
                                    sep=";",
                                    dtype={'REGIAO_SAUDE_CODIGO': object,
                                           })
        # Merge dos DataFrames df_grouped e df_municipios
        # Convertendo as colunas-chave para o tipo correto
        df_grouped['REGIAO_SAUDE_CODIGO'] = df_grouped['REGIAO_SAUDE_CODIGO'].astype(int)

        df_regioes_saude['REGIAO_SAUDE_CODIGO'] = df_regioes_saude['REGIAO_SAUDE_CODIGO'].astype(int)

        # Merge dos DataFrames df_grouped e df_municipios
        df_merged = pd.merge(df_grouped, df_regioes_saude, on=['REGIAO_SAUDE_CODIGO'],
                             how='outer')

        # Selecionar as linhas que estão em df_municipios mas não em df_grouped
        novas_linhas = df_merged[df_merged['REGIAO_SAUDE_NOME_x'].isnull()]

        # Lista de ANOS e CAPS
        caps = ['CAPS I']

        # Lista para armazenar as novas linhas
        novas_linhas_combinadas = []

        # Iterar sobre as novas linhas
        for _, row in novas_linhas.iterrows():
            regiao_saude_populacao = row["REGIAO_SAUDE_POPULACAO_y"]
            estado_codigo = row['ESTADO_CODIGO_y']
            estado_sigla = row['ESTADO_SIGLA_y']
            estado_nome = row['ESTADO_NOME_y']
            codigo_cep = row['CO_CEP']
            regiao_saude_codigo = row['REGIAO_SAUDE_CODIGO']
            regiao_saude_nome = row['REGIAO_SAUDE_NOME_y']

            # Criar combinações de anos e tipos de CAPS
            for ano in ANOS_CONSIDERADOS:
                for cap in caps:
                    nova_linha = {
                        'ESTADO_CODIGO': estado_codigo,
                        'ESTADO_SIGLA': estado_sigla,
                        'ESTADO_NOME': estado_nome,
                        'REGIAO_SAUDE_CODIGO': regiao_saude_codigo,
                        'REGIAO_SAUDE_NOME': regiao_saude_nome,
                        'CO_CEP': codigo_cep,
                        'ANO': ano,
                        'MES': 12,
                        'REGIAO_SAUDE_POPULACAO': regiao_saude_populacao,
                        'TIPO': cap,
                        'REGIAO_SAUDE_QTDE_CAPS': 0,
                        'REGIAO_SAUDE_IC': 0
                    }
                    novas_linhas_combinadas.append(nova_linha)

        # Converter a lista de novas linhas combinadas em um DataFrame
        novas_linhas_df = pd.DataFrame(novas_linhas_combinadas)

        # Adicionar as novas linhas ao df_grouped
        df_grouped = pd.concat([df_grouped, novas_linhas_df])

        # Resetar o índice do DataFrame resultante
        df_grouped = df_grouped.reset_index(drop=True)

        df_grouped.to_csv(f'gold/{PREFIXO_NOME_ANOS}-caps-agrupados-por-tipo-por-regiao-saude.csv', sep=';',
                          index=False,
                          decimal=',')


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


def criar_arquivo_lista_anos_leitos():
    data = {'ANO': ANOS_LEITOS_CONSIDERADOS}

    df = pd.DataFrame(data)

    df.to_csv('gold/anos_leitos.csv', sep=";", index=False)


def combinar_arquivos_cepaberto():
    file_list = glob.glob(f'bronze/df.cepaberto*.csv')

    df_final = pd.DataFrame()

    col_names = ['CEP', 'LOGRADOURO', "COLUNA_SEM_DEFINICAO", 'REGIAO_ADMINISTRATIVA', 'CIDADE_ID', 'ESTADO_ID']

    for file in file_list:
        df = pd.read_csv(file, sep=",", header=None, names=col_names)
        df_final = pd.concat([df_final, df])
    df_final["MUNICIPIO_CODIGO"] = "530010"
    df_final["ESTADO_CODIGO"] = "53"
    df_final["ESTADO_SIGLA"] = "DF"
    df_final.to_csv("silver/df_cep_aberto.csv",
                    sep=";",
                    index=False)


def method_name():
    global combinado
    criar_arquivo_lista_tipo_caps()
    criar_arquivo_lista_anos()
    criar_arquivo_lista_anos_leitos()
    populacao = Populacao()
    populacao.obter_detalhes_populacao_por_municipio()
    populacao.obter_detalhes_populacao_por_estado()
    estado = Estado()
    estado.enriquecer_estados()
    municipio = Municipio()
    municipio.enriquecer_municipios()
    relacao = EstabelecimentoSubtipo(
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    relacao.filtrar_caps()
    estabelecimento = Estabelecimento(
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    estabelecimento.filtrar_caps()
    subtipo = Subtipo(
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO
    )
    subtipo.filtrar_caps()
    combinado = Combinado(
        anos=ANOS_CONSIDERADOS,
        mes=MES_COMPETENCIA_CONSIDERADO,
        tabela_relacao=relacao,
        tabela_subtipo=subtipo,
        tabela_estabelecimentos=estabelecimento
    )
    combinado.combinar_estabelecimento_subtipo()
    combinado.combinar_arquivos_ano()
    combinado = CombinadoEnriquecido(
        combinado=combinado
    )
    combinado.enriquecer_cnes()


if __name__ == "__main__":
    #method_name()

    datamart_indicador_caps_municipio = DatamartIndicadorCapsMunicipio()
    datamart_indicador_caps_municipio.criar_datamart_com_indices_cobertura_caps_por_municipio()

    datamart_indicador_caps_estado = DatamartIndicadorCapsEstado()
    datamart_indicador_caps_estado.criar_datamart_com_indices_cobertura_caps_por_estados()

    datamart_indicador_caps_regiao_saude = DatamartIndicadorCapsRegiaoSaude()
    datamart_indicador_caps_regiao_saude.criar_datamart_com_indices_cobertura_caps_por_regiao_saude()
