from download_util import DowloadDataSusCnesRawFtp

download_cnes = DowloadDataSusCnesRawFtp()
download_cnes.download_from_bucket_s3()
download_cnes.extract_december_files()
