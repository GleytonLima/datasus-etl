import pandas as pd


def enriquecer_municipios():
    # Carregue os arquivos CSV e parquet em dataframes pandas
    df_municipios = pd.read_csv("municipios-originais.csv")
    df_estados = pd.read_csv("estados.csv")
    df_estados = df_estados.rename(columns={"uf": "UF", "nome": "munResUf"})
    df_populacao_municipio = pd.read_csv("../populacao/POP2022_Municipios.csv", dtype={"CODUFMUN": object})
    df_municipios['codigo_ibge'] = df_municipios['codigo_ibge'].astype(str).str[:-1]
    df_municipios = df_municipios.rename(columns={'codigo_ibge': 'CODUFMUN'})

    df_municipios_com_regionais_saude = pd.read_csv('municipios-com-nome-regiao-saude.csv',
                                                    dtype={'Município': object,
                                                           'Nome da Região de Saúde': object,
                                                           'Cód IBGE': object})

    # Renomeie as colunas para que correspondam
    df_municipios_com_regionais_saude.rename(
        columns={
            'Município': 'munResNome',
            'Nome da Região de Saúde': 'NOME_REGIAO_SAUDE',
            'Cód Região de Saúde': 'COD_REGIAO_SAUDE',
            'Cód IBGE': 'CODUFMUN'},
        inplace=True)
    df_municipios_com_regionais_saude = pd.merge(df_municipios_com_regionais_saude,
                                                 df_estados[
                                                     ["UF", 'munResUf']],
                                                 on='UF',
                                                 how='left')

    df_municipios = pd.merge(df_municipios,
                             df_municipios_com_regionais_saude[
                                 ['CODUFMUN', "UF", 'COD_REGIAO_SAUDE', 'NOME_REGIAO_SAUDE', 'munResUf']],
                             on='CODUFMUN',
                             how='left')
    df_municipios = pd.merge(df_municipios,
                             df_populacao_municipio[["CODUFMUN", "POPULACAO"]],
                             on="CODUFMUN",
                             how="left")
    # df_municipios['nome'] = df_municipios['nome'] + ', ' + df_municipios['munResUf'] + ', Brasil'
    # Escreva o dataframe no arquivo csv
    df_municipios.to_csv('municipios.csv', index=False)


if __name__ == "__main__":
    enriquecer_municipios()
