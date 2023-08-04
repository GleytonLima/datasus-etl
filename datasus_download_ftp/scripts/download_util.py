import dataclasses
import glob
import os
import subprocess
import urllib.request
import zipfile

urls_ftp = {
    "CNES": "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/",
    "CNES_RAW": "ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_{}{}.ZIP"
}

system_config = {
    "CNES": {
        'LT': {"description": 'Leitos - A partir de Out/2005', "start_month": 10, "start_year": 2005},
        'ST': {"description": 'Estabelecimentos - A partir de Ago/2005', "start_month": 8, "start_year": 2005},
        'SR': {"description": 'Serviço Especializado - A partir de Ago/2005', "start_month": 8, "start_year": 2005},
        'EP': {"description": 'Equipes - A partir de Abr/2007', "start_month": 5, "start_year": 2007}
    },
    "CNES_RAW": {"description": 'Equipes - Dados crus a partir de 2018', "start_month": 12, "start_year": 2018}
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


@dataclasses.dataclass
class DowloadDataSusCnesRawFtp:
    def __post_init__(self):
        self.base_url = urls_ftp["CNES_RAW"]

    def download_file(self, url, file_name, destination_path="."):
        try:
            print(f"iniciando downaload do arquivo {url} {file_name} {destination_path}")
            full_file_path = os.path.join(destination_path, file_name)
            urllib.request.urlretrieve(url, full_file_path)
            print(f"Arquivo {file_name} baixado com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar o arquivo: {e}")

    def download_files_by_range(self, years, months):
        for year in years:
            for month in months:
                file_name = "BASE_DE_DADOS_CNES_{}{}.zip".format(year, f"{month:02d}")
                url = self.base_url.format(year, f"{month:02d}")
                self.download_file(url, file_name, "/data")

    def extract_december_files(self):
        # Padrão de busca para encontrar todos os arquivos .zip que correspondem ao padrão
        padrao_busca = 'BASE_DE_DADOS_CNES_*.zip'

        # Diretório onde os arquivos .zip estão localizados
        diretorio_arquivos_zip = '/data'

        # Diretório onde você deseja extrair o conteúdo dos arquivos .zip
        diretorio_destino = '/data'

        # Encontrar todos os arquivos .zip que correspondem ao padrão de busca
        arquivos_zip_encontrados = glob.glob(f'{diretorio_arquivos_zip}/{padrao_busca}')

        # Função para verificar se o arquivo .csv atende às condições desejadas
        def atende_condicoes(nome_arquivo):
            return (nome_arquivo.startswith(('rlEstabSubTipo', 'tbEstabelecimento', 'tbSubTipo'))
                    and nome_arquivo.endswith('12.csv'))

        # Extrair o conteúdo dos arquivos .zip que atendem às condições
        for arquivo_zip in arquivos_zip_encontrados:
            with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                for nome_arquivo in zip_ref.namelist():
                    if nome_arquivo.endswith('.csv') and atende_condicoes(nome_arquivo):
                        zip_ref.extract(nome_arquivo, diretorio_destino)

        print("Conteúdo dos arquivos .csv foi extraído com sucesso!")
