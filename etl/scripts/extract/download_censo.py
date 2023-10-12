from download_util import DownloadIBGEFtp
from config import LoadConfig

config = LoadConfig().get_config()

download = DownloadIBGEFtp(urls=config.urls)
download.download_censo_previa_2022()
