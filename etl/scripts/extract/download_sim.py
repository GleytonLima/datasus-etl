from download_util import DowloadDataSusFtpSim, DonwloadDataSusConfig
from config import LoadConfig

config = LoadConfig().get_config()

ufs = config.ufs
years = config.anos
months = config.meses
urls = config.urls

config = DonwloadDataSusConfig(
    system="SIM",
    subsystem="DO"
)

download_sim = DowloadDataSusFtpSim(config=config, urls=urls)
download_sim.download_files_by_range(ufs=ufs, years=years)
download_sim.convert_dbc_to_csv_with()
