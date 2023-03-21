import pandas as pd


def enriquecer_estados():
    estados_com_codigo = pd.read_csv("estados-cidades/estados-originais.csv")
    estados_com_codigo.rename(
        columns={"codigo_uf": "ESTADO_CODIGO", "uf": "ESTADO_SIGLA", "nome": "ESTADO_NOME", "regiao": "ESTADO_REGIAO"},
        inplace=True)
    estados_somente_nome_populacao = pd.read_csv('estados-cidades/populacao/POP2022_Brasil_e_UFs.csv',
                                                 sep=";",
                                                 dtype={'ESTADO_NOME': object, 'ESTADO_POPULACAO': int})
    combinacao = pd.merge(estados_somente_nome_populacao,
                          estados_com_codigo[["ESTADO_CODIGO", "ESTADO_SIGLA", "ESTADO_NOME"]],
                          on="ESTADO_NOME",
                          how="left")
    combinacao.dropna(subset=["ESTADO_CODIGO"], inplace=True)
    combinacao.to_csv("gold/estados.csv",
                      sep=";",
                      index=False,
                      float_format='%.0f')


def enriquecer_municipios():
    # Carregue os arquivos CSV e parquet em dataframes pandas
    df_municipios = pd.read_csv("estados-cidades/municipios-originais.csv")
    df_municipios['codigo_ibge'] = df_municipios['codigo_ibge'].astype(str).str[:-1]
    df_municipios.rename(
        columns={'codigo_ibge': 'MUNICIPIO_CODIGO', 'nome': 'MUNICIPIO_NOME', 'codigo_uf': "ESTADO_CODIGO"},
        inplace=True)

    df_estados = pd.read_csv("estados-cidades/estados.csv", sep=";")

    df_populacao_municipio = pd.read_csv("estados-cidades/populacao/POP2022_Municipios.csv",
                                         sep=";",
                                         dtype={"MUNICIPIO_CODIGO": object})

    df_municipios_com_regionais_saude = pd.read_csv('estados-cidades/municipios-com-nome-regiao-saude.csv',
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
                                                     ["ESTADO_CODIGO", "ESTADO_SIGLA", 'ESTADO_NOME']],
                                                 on='ESTADO_SIGLA',
                                                 how='left')

    df_municipios = pd.merge(df_municipios,
                             df_municipios_com_regionais_saude[
                                 ['MUNICIPIO_CODIGO',
                                  "ESTADO_SIGLA",
                                  'REGIAO_SAUDE_CODIGO',
                                  'REGIAO_SAUDE_NOME',
                                  'ESTADO_NOME']],
                             on='MUNICIPIO_CODIGO',
                             how='left')
    df_municipios = pd.merge(df_municipios,
                             df_populacao_municipio[["MUNICIPIO_CODIGO", 'MUNICIPIO_POPULACAO']],
                             on="MUNICIPIO_CODIGO",
                             how="left")

    df_municipios.to_csv('gold/municipios.csv',
                         sep=";",
                         index=False)


if __name__ == "__main__":
    enriquecer_estados()
    enriquecer_municipios()
