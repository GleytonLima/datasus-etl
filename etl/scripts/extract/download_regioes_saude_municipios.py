from download_util import DownloadSageSaudeHttp
from config import LoadConfig

config = LoadConfig().get_config()

download = DownloadSageSaudeHttp(urls=config.urls)
download.download_municipios_com_regioes_saude()
