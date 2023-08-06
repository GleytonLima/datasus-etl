import pandas as pd

ANOS = ["18", "19", "20", "21", "22"]
MESES = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']


def gerar_arquivo_categorias():
    df_saida = pd.DataFrame()
    for ano in ANOS:
        for mes in MESES:
            df_sih = pd.read_csv(f"bronze/RDDF{ano}{mes}.csv", dtype={"DT_INTER": str, "UF_ZI": str})
            df_sih["DIAG_PRINC"] = df_sih["DIAG_PRINC"].str.slice(0, 3)
            df_sih = df_sih[
                (df_sih["DIAG_PRINC"].str.startswith("F")) | (
                        (df_sih['DIAG_PRINC'] >= 'X60') & (df_sih['DIAG_PRINC'] < 'X85'))]

            df_sih["ESTADO_CODIGO"] = df_sih["UF_ZI"].str.slice(0, 2)
            df_sih = df_sih[["ESTADO_CODIGO", "DIAG_PRINC", "ANO_CMPT", "MES_CMPT"]]
            df_saida = pd.concat([df_saida, df_sih], axis=0)

    totalizador = df_saida.groupby(["ESTADO_CODIGO", "DIAG_PRINC", "ANO_CMPT", "MES_CMPT"]).size().reset_index(
        name="TOTAL")
    totalizador.to_csv("silver/2018-2022-diag_princ-sih-df.csv", sep=";", index=False)


def agrupar_por_categoria():
    df_sih = pd.read_csv("silver/2018-2022-diag_princ-sih-df.csv", sep=";")
    df_sih["CATEGORIA_GERAL"] = df_sih["DIAG_PRINC"].apply(lambda x: "lesões autoprovocadas" if x.startswith(
        "X") else "transtornos mentais e comportamentais" if x.startswith("F") else "outro")
    totalizador = df_sih.groupby(["ESTADO_CODIGO", "CATEGORIA_GERAL", "ANO_CMPT", "MES_CMPT"])[
        "TOTAL"].sum().reset_index(
        name="TOTAL")
    totalizador.to_csv("silver/2018-2022-categoria_geral-sih-df.csv", sep=";", index=False)


def calcular_percentual():
    df = pd.read_csv("silver/2018-2022-diag_princ-sih-df.csv", delimiter=";")

    df_by_year = df.groupby("ANO_CMPT")["TOTAL"].sum()

    df["PERCENTUAL"] = df.apply(lambda x: (x["TOTAL"] / df_by_year[x["ANO_CMPT"]]) * 100, axis=1)

    df.to_csv("silver/2018-2022-percentual-sih-df.csv",
              sep=";",
              decimal=",")


if __name__ == "__main__":
    #gerar_arquivo_categorias()
    #agrupar_por_categoria()
    calcular_percentual()
