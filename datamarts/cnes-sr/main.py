import glob

import pandas as pd


def calcular_quantitativo_servicos_residenciais_terapeuticos():
    # Obtém a lista de arquivos CSV na pasta "bronze"
    arquivos_csv = glob.glob('bronze/*.csv')

    # Lista para armazenar os DataFrames filtrados de cada arquivo
    dfs_filtrados = []

    # Itera sobre cada arquivo CSV
    for arquivo in arquivos_csv:
        # Carrega o arquivo CSV em um DataFrame
        df = pd.read_csv(arquivo)

        # Filtra somente as linhas com SERV_ESP igual a 115
        df_filtrado = df[df['SERV_ESP'] == 115]

        # Filtra os CLASS_SR igual a 004 ou 005
        df_filtrado = df_filtrado[df_filtrado['CLASS_SR'].isin([4, 5])]

        # Cria a coluna CODUF com os dois primeiros dígitos da coluna CODUFMUN
        df_filtrado['CODUF'] = df_filtrado['CODUFMUN'].astype(str).str[:2]

        # Converte a coluna "COMPETEN" para o tipo string
        df_filtrado['COMPETEN'] = df_filtrado['COMPETEN'].astype(str)

        # Extrai os primeiros 4 caracteres da coluna "COMPETEN" para obter o ano
        df_filtrado['ANO'] = df_filtrado['COMPETEN'].str[:4]

        # Adiciona o DataFrame filtrado à lista
        dfs_filtrados.append(df_filtrado)

    # Concatena todos os DataFrames filtrados em um único DataFrame
    df_resultado = pd.concat(dfs_filtrados)

    # Conta as linhas agrupadas por CODUF, CODUFMUN, SERV_ESP e CLASS_SR
    contagem = df_resultado.groupby(['CODUF', 'CODUFMUN', 'SERV_ESP', 'CLASS_SR', 'ANO']).size().reset_index(
        name='TOTAL_RESIDENCIAS_TERAPEUTICAS')

    # Define o caminho do arquivo de saída
    caminho_saida = 'gold/quantitativo_servicos_residenciais_terapeuticos.csv'

    contagem.rename(columns={
        "SERV_ESP": "SERVICO_ESPECIALIZADO_CODIGO",
        "CLASS_SR": "SERVICO_ESPECIALIZADO_CLASSIFICACAO_CODIGO",
        "CODUF": "ESTADO_CODIGO",
        "CODUFMUN": "MUNICIPIO_CODIGO"
    }, inplace=True)

    contagem = contagem.dropna(subset=['ANO'])

    # Salva o DataFrame filtrado em um arquivo CSV separado por ponto e vírgula
    contagem.to_csv(caminho_saida, sep=';', index=False)


def calcular_quantitativo_ua_adulto_infantil():
    # Obtém a lista de arquivos CSV na pasta "bronze"
    arquivos_csv = glob.glob('bronze/*.csv')

    # Lista para armazenar os DataFrames filtrados de cada arquivo
    dfs_filtrados = []

    # Itera sobre cada arquivo CSV
    for arquivo in arquivos_csv:
        # Carrega o arquivo CSV em um DataFrame
        df = pd.read_csv(arquivo)

        # Filtra somente as linhas com SERV_ESP igual a 115
        df_filtrado = df[df['SERV_ESP'] == 115]

        # Filtra os CLASS_SR igual a 006 ou 007
        df_filtrado = df_filtrado[df_filtrado['CLASS_SR'].isin([6, 7])]

        # Cria a coluna CODUF com os dois primeiros dígitos da coluna CODUFMUN
        df_filtrado['CODUF'] = df_filtrado['CODUFMUN'].astype(str).str[:2]

        # Converte a coluna "COMPETEN" para o tipo string
        df_filtrado['COMPETEN'] = df_filtrado['COMPETEN'].astype(str)

        # Extrai os primeiros 4 caracteres da coluna "COMPETEN" para obter o ano
        df_filtrado['ANO'] = df_filtrado['COMPETEN'].str[:4]

        # Adiciona o DataFrame filtrado à lista
        dfs_filtrados.append(df_filtrado)

    # Concatena todos os DataFrames filtrados em um único DataFrame
    df_resultado = pd.concat(dfs_filtrados)

    # Conta as linhas agrupadas por CODUF, CODUFMUN, SERV_ESP e CLASS_SR
    contagem = df_resultado.groupby(['CODUF', 'CODUFMUN', 'SERV_ESP', 'CLASS_SR', 'ANO']).size().reset_index(
        name='TOTAL_RESIDENCIAS_TERAPEUTICAS')

    # Define o caminho do arquivo de saída
    caminho_saida = 'gold/quantitativo_ua_adulto_infantil.csv'

    contagem.rename(columns={
        "SERV_ESP": "SERVICO_ESPECIALIZADO_CODIGO",
        "CLASS_SR": "SERVICO_ESPECIALIZADO_CLASSIFICACAO_CODIGO",
        "CODUF": "ESTADO_CODIGO",
        "CODUFMUN": "MUNICIPIO_CODIGO"
    }, inplace=True)

    contagem = contagem.dropna(subset=['ANO'])

    # Salva o DataFrame filtrado em um arquivo CSV separado por ponto e vírgula
    contagem.to_csv(caminho_saida, sep=';', index=False)


def gerar_arquivo_servico_especializado():
    # Dados do DataFrame
    data = {
        'SERVICO_ESPECIALIZADO_CODIGO': [115],
        'SERVICO_ESPECIALIZADO_DESCRICAO': ['Serviço de Atenção Psicossocial']
    }
    # Cria o DataFrame
    df_servico_especializado = pd.DataFrame(data)
    # Define o caminho do arquivo de saída
    caminho_saida = 'gold/servico_especializado.csv'
    # Salva o DataFrame em um arquivo CSV
    df_servico_especializado.to_csv(caminho_saida, sep=';', index=False)


def gerar_arquivo_classificacao_srt():
    # Dados do DataFrame
    data = {
        'SERVICO_ESPECIALIZADO_CLASSIFICACAO_CODIGO': [4, 5, 6, 7],
        'SERVICO_ESPECIALIZADO_CLASSIFICACAO_DESCRICAO': ['Serviço Residencial Terapêutico SRT - Tipo I',
                                                          'Serviço Residencial Terapêutico SRT - Tipo II',
                                                          'UA Adulto',
                                                          'UA Infantil']
    }
    # Cria o DataFrame
    df_classificacao_servico_especializado = pd.DataFrame(data)
    # Define o caminho do arquivo de saída
    caminho_saida_classificacao = 'gold/classificacao_servico_especializado.csv'
    # Salva o DataFrame em um arquivo CSV
    df_classificacao_servico_especializado.to_csv(caminho_saida_classificacao, sep=';', index=False)


if __name__ == "__main__":
    calcular_quantitativo_ua_adulto_infantil()

    gerar_arquivo_servico_especializado()

    gerar_arquivo_classificacao_srt()
