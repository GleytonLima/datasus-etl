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
        ['CODUFMUN', 'MUNICIPIO_NOME', 'ESTADO_NOME', 'ANO', 'MES', 'POPULACAO',
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
        ['ESTADO_NOME', 'ANO', 'MES', 'POPULACAO_UF',
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
    cols = ['ESTADO_NOME', "COD_REGIAO_SAUDE", "NOME_REGIAO_SAUDE", 'ANO', 'MES', 'POPULACAO_REGIAO_SAUDE',
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
    enriquecer_cnes()
    # criar_datamart_com_indices_cobertura_caps_por_estados()
    # criar_datamart_com_indices_cobertura_caps_por_municipio()
    criar_datamart_com_indices_cobertura_caps_por_regiao_saude()
