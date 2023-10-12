from download_util import DownloadDataSusCids
from config import LoadConfig

config = LoadConfig().get_config()

download = DownloadDataSusCids(urls=config.urls)
download.download_tabela_cids()
download.extrair_arquivos()
