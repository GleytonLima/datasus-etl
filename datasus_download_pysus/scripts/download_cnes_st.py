from pysus.online_data import parquets_to_dataframe
from pysus.online_data.CNES import download


def get_file_name(file_path: str):
    parts = file_path.split('/')
    parquet_filename = parts[-1]
    csv_filename = parquet_filename.replace(".parquet", ".csv")
    print("gerado nome arquivo csv: " + csv_filename)
    return csv_filename


parquet_folders = download('ST', 'SP', 2021, 8)
print("arquivos parquet baixados " + str(parquet_folders))
print("arquivos parquet baixados " + str(type(parquet_folders)))


def gerar_arquivo_csv(parquet: str):
    print("Gerando arquivo para: "+ parquet)
    df = parquets_to_dataframe(parquet)
    print("Df resultante " + str(df))
    df.to_csv("/csv/" + get_file_name(parquet), index=False, sep=';')
    print("arquivo gerado: " + str(parquet))


if isinstance(parquet_folders, str):
    gerar_arquivo_csv(parquet_folders)
else:
    for parquet in parquet_folders:
        gerar_arquivo_csv(parquet)
