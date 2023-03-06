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
    # Carregue os arquivos CSV e parquet em dataframes pandas
    df_parquet = pd.read_parquet("cnes_filtrados.parquet")

    df_caps_com_tipo_csv = pd.read_csv('caps-por-tipo/caps-por-tipo.csv')
    df_municipios_com_regionais_saude = pd.read_csv('estados-cidades/municipios-com-nome-regiao-saude.csv',
                                                    dtype={'Município': object, 'Nome da Região de Saúde': object,
                                                           'Cód IBGE': object})
    df_populacao_municipio_csv = pd.read_csv('populacao/POP2022_Municipios.csv',
                                             dtype={'CODUFMUN': object, 'POPULACAO': object})
    df_populacao_estado_csv = pd.read_csv('populacao/POP2022_Brasil_e_UFs.csv',
                                          dtype={'munResUf': object, 'POPULACAO_UF': object})

    # Renomeie as colunas para que correspondam
    df_caps_com_tipo_csv.rename(columns={'Estabelecimento': 'FANTASIA', 'Tipo': 'TIPO'}, inplace=True)

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
                                                 df_regioes_saude_enriquecido[['UF', 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE',
                                                             "POPULACAO_REGIAO_SAUDE"]],
                                                 on=["UF", "COD_REGIAO_SAUDE", "NOME_REGIAO_SAUDE"],
                                                 how="left")

    # Use o método merge para combinar os dataframes com base na coluna 'FANTASIA'
    df_merge = pd.merge(df_parquet, df_caps_com_tipo_csv[['FANTASIA', 'TIPO']], on='FANTASIA', how='left')

    # Use o método merge para combinar os dataframes com base na coluna 'CODUFMUN'
    df_merge = pd.merge(df_merge, df_populacao_municipio_csv[['CODUFMUN', 'POPULACAO']], on='CODUFMUN', how='left')

    # Use o método merge para combinar os dataframes com base na coluna 'munResUf'
    df_merge = pd.merge(df_merge, df_populacao_estado_csv[['munResUf', 'POPULACAO_UF']], on='munResUf', how='left')

    # Use o método merge para combinar os dataframes com base na coluna 'munResUf'
    df_merge = pd.merge(df_merge, df_municipios_com_regionais_saude[
        ['CODUFMUN', 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE', 'POPULACAO_REGIAO_SAUDE']],
                        on='CODUFMUN', how='left')

    # Remover linhas duplicadas em todas as colunas
    df_merge = df_merge.drop_duplicates()

    # Escreva o dataframe no arquivo parquet
    df_merge.to_csv('2018-2022-cnes-enriquecido.csv', index=False)


def gerar_datamart():
    # lista todos os arquivos Parquet na pasta
    file_list = glob.glob('cnes-dadosbrutos/*.parquet')

    # cria um DataFrame vazio
    df_final = pd.DataFrame()

    # itera sobre a lista de arquivos e carrega cada um em um DataFrame temporário
    for file in file_list:
        columns = ["CNES", "TP_UNID", "COMPETEN", "CODUFMUN", "FANTASIA", "munResLat", "munResLon", "munResNome",
                   "munResUf"]
        df = pd.read_parquet(file, columns=columns, engine="pyarrow", use_threads=True)
        # filtra os itens que contém a String "Centro de atenção psicosocial" na coluna TP_UNID
        # e que a coluna COMPETEN termina com "12"
        filtered_df = df[(df['TP_UNID'].str.contains('Centro de atenção psicosocial')) &
                         (df['COMPETEN'].str.endswith('12'))]
        # separa a coluna COMPETEN em duas colunas ANO e MES
        filtered_df[['ANO', 'MES']] = filtered_df['COMPETEN'].str.extract(r'(\d{4})(\d{2})')
        # remove a coluna COMPETEN
        filtered_df = filtered_df.drop(columns=['COMPETEN'])
        # adiciona o DataFrame temporário ao DataFrame principal
        df_final = pd.concat([df_final, filtered_df])

    # exibe o DataFrame resultante
    print(df_final)
    df_final.to_parquet("cnes_filtrados.parquet")


# define o multiplicador para cada TIPO_CAPS
multiplicador = {
    "CAPS I": 0.5,
    "CAPS II": 1,
    "CAPS AD III": 1.5,
    "CAPS AD": 1,
    "CAPS i": 1,
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
        ['TP_UNID', 'CODUFMUN', 'munResLat', 'munResLon', 'munResNome', 'munResUf', 'ANO', 'MES', 'POPULACAO',
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
        ['TP_UNID', 'munResUf', 'ANO', 'MES', 'POPULACAO_UF',
         'TIPO']).size().reset_index(name="TOTAL_POR_TIPO_CAPS")

    # Imprime o DataFrame resultante
    # aplica a função para criar a nova coluna
    df_grouped['INDICE_COBERTURA_CAPS'] = df_grouped.apply(calcular_valor_estado, axis=1)

    print(df_grouped)
    df_grouped.to_csv("2018-2022-caps-agrupados-por-tipo-por-uf.csv", index=False)


def criar_datamart_com_indices_cobertura_caps_por_regiao_saude():
    # Carrega o arquivo CSV em um DataFrame do Pandas
    df_cnes_enriquecido = pd.read_csv('2018-2022-cnes-enriquecido.csv')

    df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

    # Agrupa as linhas e conta o número de linhas em cada grupo
    cols = ['TP_UNID', 'munResUf', "COD_REGIAO_SAUDE", "NOME_REGIAO_SAUDE", 'ANO', 'MES', 'POPULACAO_REGIAO_SAUDE',
            'TIPO']
    df_grouped = df_cnes_enriquecido.groupby(cols).size().reset_index(name="TOTAL_POR_TIPO_CAPS")
    df_grouped = df_grouped[cols + ["TOTAL_POR_TIPO_CAPS"]]
    # Imprime o DataFrame resultante
    # aplica a função para criar a nova coluna
    df_grouped['INDICE_COBERTURA_CAPS'] = df_grouped.apply(calcular_valor_regiao_saude, axis=1)

    print(df_grouped)
    df_grouped.to_csv("2018-2022-caps-agrupados-por-tipo-por-regiao-saude.csv", index=False)


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    if False:
        df_parquet = pd.read_csv("2018-2022-cnes-enriquecido.csv")
        df_parquet = df_parquet.drop_duplicates(subset=['TIPO'])
        print(df_parquet)
        exit()
    # gerar_datamart()
    enriquecer_parquet()
    #criar_datamart_com_indices_cobertura_caps_por_estados()
    #criar_datamart_com_indices_cobertura_caps_por_municipio()
    criar_datamart_com_indices_cobertura_caps_por_regiao_saude()
