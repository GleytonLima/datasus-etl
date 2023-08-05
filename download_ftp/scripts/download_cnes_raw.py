from download_util import DowloadDataSusCnesRawFtp

years = [2022]
months = [12]

download_cnes = DowloadDataSusCnesRawFtp()
download_cnes.download_files_by_range(years=years, months=months)
download_cnes.extract_december_files()
