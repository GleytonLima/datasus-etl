from download_util import DownloadGithubIbge
from config import LoadConfig

config = LoadConfig().get_config()

download = DownloadGithubIbge(urls=config.urls)

download.download_estados_originais()
download.download_municipios_originais()
