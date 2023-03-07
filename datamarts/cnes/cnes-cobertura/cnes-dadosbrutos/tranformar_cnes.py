import pandas as pd


def filtrar_subtipos():
    chunksize = 1000
    nomes_arquivos = ["rlEstabSubTipo202212",
                      "rlEstabSubTipo202112",
                      "rlEstabSubTipo202012",
                      "rlEstabSubTipo201912",
                      "rlEstabSubTipo201812"]
    for nome_arquivo_cnes_dados_brutos in nomes_arquivos:
        # define o número de linhas a serem lidas de cada vez
        # pode ser ajustado de acordo com a memória disponível

        # cria um objeto de leitura do arquivo csv em partes
        reader = pd.read_csv(f'{nome_arquivo_cnes_dados_brutos}.csv',
                             dtype={'CO_TIPO_UNIDADE': 'int32'},
                             sep=";",
                             chunksize=chunksize)

        # cria um objeto de escrita do arquivo csv
        writer = pd.DataFrame(columns=reader.get_chunk().columns)
        # usa as colunas da primeira parte para criar o objeto de escrita

        for chunk in reader:
            # filtra as linhas com CO_ESTADO_GESTOR = 70 em cada parte
            filtered_chunk = chunk[chunk['CO_TIPO_UNIDADE'] == 70]
            # concatena as partes filtradas em um DataFrame
            writer = pd.concat([writer, filtered_chunk])

        # salva o DataFrame final como um novo arquivo csv
        writer.to_csv(f'{nome_arquivo_cnes_dados_brutos}_caps.csv',
                      sep=';',
                      index=False)


def filtrar_caps():
    chunksize = 1000
    arquivo = [
        {
            "nome": "tbEstabelecimento202212",
            "ano": "2022",
            "mes": "12"
        },
        {
            "nome": "tbEstabelecimento202112",
            "ano": "2021",
            "mes": "12"
        },
        {
            "nome": "tbEstabelecimento202012",
            "ano": "2020",
            "mes": "12"
        },
        {
            "nome": "tbEstabelecimento201912",
            "ano": "2019",
            "mes": "12"
        },
        {
            "nome": "tbEstabelecimento201812",
            "ano": "2018",
            "mes": "12"
        }
    ]
    for arquivo in arquivo:
        # define o número de linhas a serem lidas de cada vez
        # pode ser ajustado de acordo com a memória disponível

        # cria um objeto de leitura do arquivo csv em partes
        reader = pd.read_csv(f'{arquivo["nome"]}.csv',
                             dtype={'TP_UNIDADE': 'int32'},
                             sep=";",
                             chunksize=chunksize)

        # cria um objeto de escrita do arquivo csv
        writer = pd.DataFrame(columns=reader.get_chunk().columns)
        # usa as colunas da primeira parte para criar o objeto de escrita

        for chunk in reader:
            # filtra as linhas com CO_ESTADO_GESTOR = 70 em cada parte CO_MOTIVO_DESAB
            filtered_chunk = chunk[(chunk['TP_UNIDADE'] == 70) & (chunk['CO_MOTIVO_DESAB'].isna())].assign(ANO=arquivo["ano"], MES=arquivo["mes"])
            # concatena as partes filtradas em um DataFrame
            writer = pd.concat([writer, filtered_chunk])

        # salva o DataFrame final como um novo arquivo csv
        writer.to_csv(f'{arquivo["nome"]}_caps.csv',
                      index=False,
                      sep=';',
                      columns=["CO_UNIDADE", "CO_CNES", "NO_FANTASIA", "CO_CEP", "TP_UNIDADE", "CO_ESTADO_GESTOR",
                               "CO_MUNICIPIO_GESTOR", "CO_TIPO_ESTABELECIMENTO", "ANO", "MES"])


def combinar_caps_com_tipo():
    pares_arquivo = [
        {
            "nome_arquivo_caps": "tbEstabelecimento202212_caps.csv",
            "nome_arquivo_tipos": "rlEstabSubTipo202212_caps.csv",
            "nome_arquivo_tipo_subtipo": "tbSubTipo202212.csv",
            "ano": "2022"
        },
        {
            "nome_arquivo_caps": "tbEstabelecimento202112_caps.csv",
            "nome_arquivo_tipos": "rlEstabSubTipo202112_caps.csv",
            "nome_arquivo_tipo_subtipo": "tbSubTipo202112.csv",
            "ano": "2021"
        },
        {
            "nome_arquivo_caps": "tbEstabelecimento202012_caps.csv",
            "nome_arquivo_tipos": "rlEstabSubTipo202012_caps.csv",
            "nome_arquivo_tipo_subtipo": "tbSubTipo202012.csv",
            "ano": "2020"
        },
        {
            "nome_arquivo_caps": "tbEstabelecimento201912_caps.csv",
            "nome_arquivo_tipos": "rlEstabSubTipo201912_caps.csv",
            "nome_arquivo_tipo_subtipo": "tbSubTipo201912.csv",
            "ano": "2019"
        },
        {
            "nome_arquivo_caps": "tbEstabelecimento201812_caps.csv",
            "nome_arquivo_tipos": "rlEstabSubTipo201812_caps.csv",
            "nome_arquivo_tipo_subtipo": "tbSubTipo201812.csv",
            "ano": "2018"
        }
    ]
    for par_arquivo in pares_arquivo:
        arquivo_caps = pd.read_csv(par_arquivo["nome_arquivo_caps"],
                                   sep=";",
                                   dtype={"CO_UNIDADE": object})
        arquivo_tipos = pd.read_csv(par_arquivo["nome_arquivo_tipos"],
                                    sep=";",
                                    dtype={"CO_UNIDADE": object, "CO_SUB_TIPO_UNIDADE": 'int'})
        arquivo_tipo_subtipo = pd.read_csv(par_arquivo["nome_arquivo_tipo_subtipo"],
                                           sep=";",
                                           dtype={"CO_UNIDADE": object, "CO_SUB_TIPO": 'int'})
        arquivo_tipo_subtipo = arquivo_tipo_subtipo.rename(columns={"CO_SUB_TIPO": "CO_SUB_TIPO_UNIDADE"})
        arquivo_tipos = pd.merge(arquivo_tipos,
                                 arquivo_tipo_subtipo[["CO_TIPO_UNIDADE", "CO_SUB_TIPO_UNIDADE", "DS_SUB_TIPO"]],
                                 on=["CO_TIPO_UNIDADE", "CO_SUB_TIPO_UNIDADE"],
                                 how="left")
        df_merge = pd.merge(arquivo_caps, arquivo_tipos[["CO_UNIDADE", "CO_SUB_TIPO_UNIDADE", "DS_SUB_TIPO"]],
                            on='CO_UNIDADE',
                            how='left')
        df_merge['CO_SUB_TIPO_UNIDADE'] = df_merge['CO_SUB_TIPO_UNIDADE'].fillna(0).astype(int)
        df_merge = df_merge.drop_duplicates()
        df_merge.to_csv(f'{par_arquivo["ano"]}-cnes_filtrados.csv', index=False)


if __name__ == "__main__":
    filtrar_subtipos()
    filtrar_caps()
    combinar_caps_com_tipo()
