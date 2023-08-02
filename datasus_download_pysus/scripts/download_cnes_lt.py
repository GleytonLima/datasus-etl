from pysus.online_data import parquets_to_dataframe
from pysus.online_data.CNES import download


def get_file_name(file_path: str):
    parts = file_path.split('/')
    parquet_filename = parts[-1]
    csv_filename = parquet_filename.replace(".parquet", ".csv")
    print("gerado nome arquivo csv: " + csv_filename)
    return csv_filename


parquet_folders = download('LT', 'am', [2021], 12)
print("arquivos parquet baixados " + str(parquet_folders))
print("arquivos parquet baixados " + str(type(parquet_folders)))


def gerar_arquivo_csv(parquet: str):
    df = parquets_to_dataframe(parquet)
    df.to_csv("/csv/" + get_file_name(parquet), index=False, sep=';')
    print("arquivo gerado: " + parquet)


if type(parquet_folders) == str:
    gerar_arquivo_csv(parquet_folders)
else:
    for parquet in parquet_folders:
        gerar_arquivo_csv(parquet)
