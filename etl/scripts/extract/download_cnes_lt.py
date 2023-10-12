from download_util import DowloadDataSusFtp, DonwloadDataSusConfig
from config import LoadConfig

config = LoadConfig().get_config()

ufs = config.ufs
years = config.anos
months = config.meses
urls = config.urls

config = DonwloadDataSusConfig(
    system="CNES",
    subsystem="LT"
)

download_cnes = DowloadDataSusFtp(config=config, urls=urls)
download_cnes.download_files_by_range(ufs=ufs, years=years, months=months)
download_cnes.convert_dbc_to_csv_with()
