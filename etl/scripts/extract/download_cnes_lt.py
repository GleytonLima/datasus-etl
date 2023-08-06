from download_util import DowloadDataSusFtp, DonwloadDataSusConfig

ufs = ["AM"]
years = [2022]
months = [12]

config = DonwloadDataSusConfig(
    system="CNES",
    subsystem="LT"
)

download_cnes = DowloadDataSusFtp(config=config)
download_cnes.download_files_by_range(ufs=ufs, years=years, months=months)
download_cnes.convert_dbc_to_csv_with()
