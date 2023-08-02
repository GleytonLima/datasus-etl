import dataclasses
import os
import subprocess
import urllib.request

urls_ftp = {
    "CNES": "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/"
}

system_config = {
    "CNES": {
        'LT': {"description": 'Leitos - A partir de Out/2005', "start_month": 10, "start_year": 2005},
        'ST': {"description": 'Estabelecimentos - A partir de Ago/2005', "start_month": 8, "start_year": 2005},
        'SR': {"description": 'Serviço Especializado - A partir de Ago/2005', "start_month": 8, "start_year": 2005},
        'EP': {"description": 'Equipes - A partir de Abr/2007', "start_month": 5, "start_year": 2007}
    }
}


@dataclasses.dataclass
class DonwloadDataSusConfig:
    system: str
    subsystem: str


@dataclasses.dataclass
class DowloadDataSusFtp:
    config: DonwloadDataSusConfig

    def __post_init__(self):
        self.base_url = f"{urls_ftp[self.config.system]}{self.config.subsystem}/"

    def download_file(self, url, file_name, destination_path="."):
        try:
            print(f"iniciando downaload do arquivo {url} {file_name} {destination_path}")
            full_file_path = os.path.join(destination_path, file_name)
            urllib.request.urlretrieve(url, full_file_path)
            print(f"Arquivo {file_name} baixado com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar o arquivo: {e}")

    def download_files_by_range(self, ufs, years, months):
        for uf in ufs:
            for year in years:
                for month in months:
                    file_name = f"{self.config.subsystem}{uf}{str(year)[-2:]}{month:02d}.dbc"
                    url = self.base_url + file_name
                    self.download_file(url, file_name, "/data")

    def convert_dbc_to_csv_with(self):
        try:
            subprocess.run("Rscript dbc_to_csv.R", check=True, shell=True)
        except subprocess.CalledProcessError as e:
            print(f"Erro ao executar o script R: {e}")
