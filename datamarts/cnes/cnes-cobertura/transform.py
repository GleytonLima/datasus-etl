import pandas as pd

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


# define a função personalizada para calcular o valor
def calcular_valor_municipio(row):
    return row['MUNICIPIO_QTDE_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['MUNICIPIO_POPULACAO']


# define a função personalizada para calcular o valor
def calcular_valor_estado(row):
    return row['ESTADO_QTDE_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['ESTADO_POPULACAO']


def calcular_valor_regiao_saude(row):
    return row['REGIAO_SAUDE_QTDE_CAPS'] * 100000.00 * multiplicador[row['TIPO']] / row['REGIAO_SAUDE_POPULACAO']


def criar_datamart_com_indices_cobertura_caps_por_municipio():
    # Carrega o arquivo CSV em um DataFrame do Pandas
    df_cnes_enriquecido = pd.read_csv('gold/2018-2022-cnes-enriquecido.csv', sep=';')

    df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

    # Agrupa as linhas e conta o número de linhas em cada grupo
    df_grouped = df_cnes_enriquecido.groupby(
        ['MUNICIPIO_CODIGO',
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
    df_grouped['MUNICIPIO_IC'] = df_grouped.apply(calcular_valor_municipio, axis=1)

    print(df_grouped)
    df_grouped.to_csv('gold/2018-2022-caps-agrupados-por-tipo.csv', sep=';', index=False, decimal=',')


def criar_datamart_com_indices_cobertura_caps_por_estados():
    # Carrega o arquivo CSV em um DataFrame do Pandas
    df_cnes_enriquecido = pd.read_csv('gold/2018-2022-cnes-enriquecido.csv', sep=';')

    df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

    # Agrupa as linhas e conta o número de linhas em cada grupo
    df_grouped = df_cnes_enriquecido.groupby(
        ['ESTADO_CODIGO',
         'ESTADO_SIGLA',
         'ESTADO_NOME',
         'ANO',
         'MES',
         'ESTADO_POPULACAO',
         'TIPO']).size().reset_index(name='ESTADO_QTDE_CAPS')

    # Imprime o DataFrame resultante
    # aplica a função para criar a nova coluna
    df_grouped['ESTADO_IC'] = df_grouped.apply(calcular_valor_estado, axis=1)

    print(df_grouped)
    df_grouped.to_csv('gold/2018-2022-caps-agrupados-por-tipo-por-uf.csv', sep=';', index=False, decimal=',')


def criar_datamart_com_indices_cobertura_caps_por_regiao_saude():
    # Carrega o arquivo CSV em um DataFrame do Pandas
    df_cnes_enriquecido = pd.read_csv('gold/2018-2022-cnes-enriquecido.csv', sep=';')
    df_cnes_enriquecido = df_cnes_enriquecido.rename(columns={'CO_ESTADO_GESTOR': 'codigo_uf'})
    df_cnes_enriquecido['TIPO'].fillna('Não informado', inplace=True)

    # Agrupa as linhas e conta o número de linhas em cada grupo
    cols = ['ESTADO_CODIGO',
            'ESTADO_SIGLA',
            'ESTADO_NOME',
            'REGIAO_SAUDE_CODIGO',
            'REGIAO_SAUDE_NOME',
            'ANO',
            'MES',
            'REGIAO_SAUDE_POPULACAO',
            'TIPO']
    df_grouped = df_cnes_enriquecido.groupby(cols).size().reset_index(name='REGIAO_SAUDE_QTDE_CAPS')
    df_grouped = df_grouped[cols + ['REGIAO_SAUDE_QTDE_CAPS']]
    # Imprime o DataFrame resultante
    # aplica a função para criar a nova coluna
    df_grouped['REGIAO_SAUDE_IC'] = df_grouped.apply(calcular_valor_regiao_saude, axis=1)

    print(df_grouped)
    df_grouped.to_csv('gold/2018-2022-caps-agrupados-por-tipo-por-regiao-saude.csv', sep=';', index=False, decimal=',')


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    criar_datamart_com_indices_cobertura_caps_por_estados()
    criar_datamart_com_indices_cobertura_caps_por_municipio()
    criar_datamart_com_indices_cobertura_caps_por_regiao_saude()
