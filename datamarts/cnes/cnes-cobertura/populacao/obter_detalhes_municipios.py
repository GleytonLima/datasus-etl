import pandas as pd


def obter_detalhes_populacao_por_estado():
    # Carrega o arquivo XLS em um DataFrame do Pandas
    df = pd.read_excel('POP2022_Brasil_e_UFs.xls', skiprows=[0], header=[0], usecols=lambda x: 'Unnamed' not in x)

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

    df.rename(columns={'BRASIL E UNIDADES DA FEDERAÇÃO': 'ESTADO_NOME'}, inplace=True)
    df.rename(columns={'POPULAÇÃO': 'POPULACAO_UF', "munResUf": "ESTADO_NOME"}, inplace=True)

    # Salva o DataFrame resultante em um arquivo CSV
    df.to_csv('POP2022_Brasil_e_UFs.csv', index=False)


def obter_detalhes_populacao_por_municipio():
    df = pd.read_excel('POP2022_Municipios.xls', skiprows=[0], header=[0], dtype={'COD. UF': str, 'COD. MUNIC': str})

    df = df.dropna(subset=[df.columns[0]])
    df = df[df['UF'].str.len() == 2]

    df['CODUFMUN'] = df['COD. UF'].astype(str) + df['COD. MUNIC'].astype(str).str[:-1]
    df['CODUFMUN'] = df['CODUFMUN'].astype(str)
    df = df.drop(columns=['COD. UF', 'COD. MUNIC'])

    df.rename(columns={'POPULAÇÃO': 'POPULACAO'}, inplace=True)
    df['POPULACAO'] = df['POPULACAO'].astype(str).replace(r'\s*\([^)]*\)\s*', '', regex=True)
    df['POPULACAO'] = df['POPULACAO'].str.replace('.', '', regex=False)
    df.rename(columns={"NOME DO MUNICÍPIO": "MUNICIPIO_NOME"})
    df.to_csv('POP2022_Municipios.csv',
              sep=";",
              index=False)


if __name__ == "__main__":
    obter_detalhes_populacao_por_estado()
