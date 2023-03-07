import glob

import pandas as pd


def verifica_faltantes_entre_bases(df_parquet):
    df_parquet = df_parquet.drop_duplicates(subset=['FANTASIA'])

    # Carregue os arquivos CSV e parquet em dataframes pandas
    df_csv = pd.read_csv('caps-por-tipo/caps-por-tipo.csv')

    # Renomeie as colunas para que correspondam
    df_csv.rename(columns={'Estabelecimento': 'FANTASIA', 'Tipo': 'TIPO'}, inplace=True)

    # Use o método merge para combinar os dataframes com base na coluna 'FANTASIA'
    df_merge = pd.merge(df_csv, df_parquet, on='FANTASIA', how='outer', indicator=True)

    # Separe os registros faltantes em cada arquivo
    df_only_csv = df_merge.loc[df_merge['_merge'] == 'left_only']
    df_only_parquet = df_merge.loc[df_merge['_merge'] == 'right_only']

    # Imprima os resultados
    print('Registros apenas no CSV:\n', df_only_csv["FANTASIA"])
    print('Registros apenas no parquet:\n', df_only_parquet["FANTASIA"])


def enriquecer_parquet():
    df_parquet = pd.read_csv("cnes_filtrados.csv", dtype={'CO_MUNICIPIO_GESTOR': object})
    df_parquet = df_parquet.rename(columns={"CO_MUNICIPIO_GESTOR": "CODUFMUN", "DS_SUB_TIPO": "TIPO"})

    df_municipios_com_regionais_saude = pd.read_csv('estados-cidades/municipios-com-nome-regiao-saude.csv',
                                                    dtype={'Município': object, 'Nome da Região de Saúde': object,
                                                           'Cód IBGE': object})
    df_populacao_municipio_csv = pd.read_csv('populacao/POP2022_Municipios.csv',
                                             dtype={'CODUFMUN': object, 'POPULACAO': object})
    df_populacao_municipio_csv = df_populacao_municipio_csv.rename(columns={"NOME DO MUNICÍPIO": "munResNome"})
    df_estados = pd.read_csv('estados-cidades/estados.csv',
                             dtype={'munResUf': object, 'POPULACAO_UF': object})
    df_estados = df_estados.rename(columns={"codigo_uf": "CO_ESTADO_GESTOR", "nome": "munResUf"})
    df_populacao_estado_csv = pd.read_csv('populacao/POP2022_Brasil_e_UFs.csv',
                                          dtype={'munResUf': object, 'POPULACAO_UF': object})
    df_populacao_estado_csv = pd.merge(df_populacao_estado_csv, df_estados, on="munResUf", how="left")

    # Renomeie as colunas para que correspondam
    df_municipios_com_regionais_saude.rename(
        columns={
            'Município': 'munResNome',
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

    df_regioes_saude_enriquecido = df_grouped[['UF', 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE', "POPULACAO_REGIAO_SAUDE"]]
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
    df_merge = pd.merge(df_parquet, df_populacao_municipio_csv[['CODUFMUN', 'POPULACAO']], on='CODUFMUN', how='left')

    # Use o método merge para combinar os dataframes com base na coluna 'munResUf'
    df_merge = pd.merge(df_merge,
                        df_populacao_estado_csv[['CO_ESTADO_GESTOR', 'munResUf', 'POPULACAO_UF']],
                        on='CO_ESTADO_GESTOR',
                        how='left')

    # Use o método merge para combinar os dataframes com base na coluna 'munResUf'
    df_merge = pd.merge(df_merge, df_municipios_com_regionais_saude[
        ['CODUFMUN', "munResNome", 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE', 'POPULACAO_REGIAO_SAUDE']],
                        on='CODUFMUN', how='left')

    # Remover linhas duplicadas em todas as colunas
    df_merge = df_merge.drop_duplicates()

    # Escreva o dataframe no arquivo parquet
    df_merge.to_csv('2018-2022-cnes-enriquecido.csv', index=False)


def combinar_cnes_filtrados():
    file_list = glob.glob('cnes-dadosbrutos/*-cnes_filtrados.csv')

    df_final = pd.DataFrame()

    for file in file_list:
        df = pd.read_csv(file)
        df_final = pd.concat([df_final, df])

    print(df_final)
    df_final.to_csv("cnes_filtrados.csv", index=False)


# define o multiplicador para cada TIPO_CAPS
multiplicador = {
    "CAPS I": 0.5,
    "CAPS II": 1,
    "CAPS ALCOOL E DROGAS III - MUNICIPAL": 1,
    "CAPS ALCOOL E DROGAS III - REGIONAL": 1,
    "CAPS AD IV": 5,
    "CAPS ALCOOL E DROGA": 1,
    "CAPS INFANTO/JUVENIL": 1,
    "CAPS III": 1.5,
    "Não informado": 0
}


# define a função personalizada para calcular o valor
def calcular_valor_municipio(row):
    return row['TOTAL_POR_TIPO_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['POPULACAO']


# define a função personalizada para calcular o valor
def calcular_valor_estado(row):
    return row['TOTAL_POR_TIPO_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['POPULACAO_UF']


def calcular_valor_regiao_saude(row):
    return row['TOTAL_POR_TIPO_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['POPULACAO_REGIAO_SAUDE']


def criar_datamart_com_indices_cobertura_caps_por_municipio():
    # Carrega o arquivo CSV em um DataFrame do Pandas
    df_cnes_enriquecido = pd.read_csv('2018-2022-cnes-enriquecido.csv')

    df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

    # Agrupa as linhas e conta o número de linhas em cada grupo
    df_grouped = df_cnes_enriquecido.groupby(
        ['CODUFMUN', 'munResNome', 'munResUf', 'ANO', 'MES', 'POPULACAO',
         'TIPO']).size().reset_index(name="TOTAL_POR_TIPO_CAPS")

    # Imprime o DataFrame resultante
    # aplica a função para criar a nova coluna
    df_grouped['INDICE_COBERTURA_CAPS'] = df_grouped.apply(calcular_valor_municipio, axis=1)

    print(df_grouped)
    df_grouped.to_csv("2018-2022-caps-agrupados-por-tipo.csv", index=False)


def criar_datamart_com_indices_cobertura_caps_por_estados():
    # Carrega o arquivo CSV em um DataFrame do Pandas
    df_cnes_enriquecido = pd.read_csv('2018-2022-cnes-enriquecido.csv')

    df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

    # Agrupa as linhas e conta o número de linhas em cada grupo
    df_grouped = df_cnes_enriquecido.groupby(
        ['munResUf', 'ANO', 'MES', 'POPULACAO_UF',
         'TIPO']).size().reset_index(name="TOTAL_POR_TIPO_CAPS")

    # Imprime o DataFrame resultante
    # aplica a função para criar a nova coluna
    df_grouped['INDICE_COBERTURA_CAPS'] = df_grouped.apply(calcular_valor_estado, axis=1)

    print(df_grouped)
    df_grouped.to_csv("2018-2022-caps-agrupados-por-tipo-por-uf.csv", index=False)


def criar_datamart_com_indices_cobertura_caps_por_regiao_saude():
    # Carrega o arquivo CSV em um DataFrame do Pandas
    df_cnes_enriquecido = pd.read_csv('2018-2022-cnes-enriquecido.csv')
    df_cnes_enriquecido = df_cnes_enriquecido.rename(columns={"CO_ESTADO_GESTOR": "codigo_uf"})
    df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

    # Agrupa as linhas e conta o número de linhas em cada grupo
    cols = ['munResUf', "COD_REGIAO_SAUDE", "NOME_REGIAO_SAUDE", 'ANO', 'MES', 'POPULACAO_REGIAO_SAUDE',
            'TIPO', 'codigo_uf']
    df_grouped = df_cnes_enriquecido.groupby(cols).size().reset_index(name="TOTAL_POR_TIPO_CAPS")
    df_grouped = df_grouped[cols + ["TOTAL_POR_TIPO_CAPS"]]
    # Imprime o DataFrame resultante
    # aplica a função para criar a nova coluna
    df_grouped['INDICE_COBERTURA_CAPS'] = df_grouped.apply(calcular_valor_regiao_saude, axis=1)

    print(df_grouped)
    df_grouped.to_csv("2018-2022-caps-agrupados-por-tipo-por-regiao-saude.csv", index=False)


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    #combinar_cnes_filtrados()
    #enriquecer_parquet()
    #criar_datamart_com_indices_cobertura_caps_por_estados()
    #criar_datamart_com_indices_cobertura_caps_por_municipio()
    criar_datamart_com_indices_cobertura_caps_por_regiao_saude()
