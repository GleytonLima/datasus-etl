import dataclasses
import glob
import io
import os
import ssl
import subprocess
import time
import urllib.request
import zipfile
from ftplib import FTP

import pandas as pd
import requests
import urllib3

def path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


urls = {
    "CNES": "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/",
    "CNES_RAW": "ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_{}{}.ZIP",
    "IBGE_UF": "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Previa_da_Populacao/POP2022_Brasil_e_UFs.xls",
    "IBGE_MUNICIPIO": "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Previa_da_Populacao/POP2022_Municipios_20230622.xls",
    "SAGE_REGIOES_SAUDE": "https://sage.saude.gov.br/paineis/regiaoSaude/lista.php?output=jsonbt&&order=asc",
    "CIDS": "http://www2.datasus.gov.br/cid10/V2008/downloads/CID10CSV.zip",
    "GITHUB_MUNICIPIOS": "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv",
    "GITHUB_ESTADOS": "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/estados.csv"
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
        self.base_url = f"{urls[self.config.system]}{self.config.subsystem}/"

    def download_file(self, url, file_name, destination_path="."):
        try:
            print(f"iniciando downaload do arquivo {url} {file_name} {destination_path}")
            full_file_path = os.path.join(destination_path, file_name)
            urllib.request.urlretrieve(url, full_file_path)
            print(f"Arquivo {file_name} baixado com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar o arquivo {file_name}: {e}")

    def download_files_by_range(self, ufs, years, months):
        for uf in ufs:
            for year in years:
                for month in months:
                    file_name = f"{self.config.subsystem}{uf}{str(year)[-2:]}{month:02d}.dbc"
                    url = self.base_url + file_name
                    self.download_file(url, file_name, "/tmp")

    def convert_dbc_to_csv_with(self):
        print("iniciando conversao dbc em csv")
        try:
            subprocess.run(
                f"Rscript /app/scripts/dbc_to_csv.R /tmp {self.gerar_path_arquivos_saida()}",
                check=True, shell=True)
        except subprocess.CalledProcessError as e:
            print(f"Erro ao executar o script R: {e}")

    def gerar_path_arquivos_saida(self):
        return f"/data/bronze/datasus/{self.config.system}/{self.config.subsystem}"


@dataclasses.dataclass
class DowloadDataSusCnesRawFtp:

    def arquivo_cnes_raw_path(self):
        return "/data/bronze/datasus/CNES/raw"

    def download_file(self, file_name):
        # Informações do servidor FTP e arquivo a ser baixado
        ftp_server = 'ftp.datasus.gov.br'
        ftp_path = f'/cnes/{file_name}'
        local_directory = '/cnesrawzip'
        local_filename = os.path.join(local_directory, file_name)

        # Criar o diretório local se não existir
        os.makedirs(local_directory, exist_ok=True)

        # Configurar a conexão FTP
        ftp = FTP(ftp_server)
        ftp.login()

        # Mudar para o diretório correto no servidor FTP
        ftp.cwd(os.path.dirname(ftp_path))

        # Determinar o tamanho do arquivo
        file_size = ftp.size(os.path.basename(ftp_path))

        # Definir os segundos de cada minuto para atualizações de progresso
        progress_seconds = [20, 40]

        # Iniciar o download
        with open(local_filename, 'wb') as local_file:
            bytes_downloaded = 0

            def callback(data):
                nonlocal bytes_downloaded
                local_file.write(data)
                bytes_downloaded += len(data)
                if time.localtime().tm_sec in progress_seconds:
                    print(
                        f"Baixando: {bytes_downloaded / 1024 / 1024:.2f} MB / {file_size / 1024 / 1024:.2f} MB ({bytes_downloaded / file_size:.2%})",
                        end='\r')

            ftp.retrbinary(f"RETR {os.path.basename(ftp_path)}", callback)

            print("\nDownload concluído.")

        # Fechar a conexão FTP
        ftp.quit()

    def download_from_bucket_s3(self):
        # URL pública do arquivo ZIP no bucket S3
        s3_public_url = 'https://datasus-etl.s3.amazonaws.com/bronze/datasus/raw/cnes_raw.zip'

        # Nome que você deseja dar ao arquivo localmente
        local_filename = f'{path("/cnesrawzip")}/BASE_DE_DADOS_CNES_TODOS.zip'

        # Fazer o download do arquivo usando requests
        response = requests.get(s3_public_url)

        if response.status_code == 200:
            with open(local_filename, 'wb') as file:
                file.write(response.content)
            print(f"Arquivo baixado como {local_filename}")
        else:
            print("Não foi possível baixar o arquivo")

    def download_files_by_range(self, years, months):
        for year in years:
            for month in months:
                file_name = "BASE_DE_DADOS_CNES_{}{}.zip".format(year, f"{month:02d}")
                self.download_file(file_name)

    # https://stackoverflow.com/a/11385480
    def fix_bad_file(self, zipFile):
        f = open(zipFile, 'r+b')
        data = f.read()
        pos = data.find(b'\x50\x4b\x05\x06')  # End of central directory signature
        if pos > 0:
            f.seek(pos + 22)  # size of 'ZIP end of central directory record'
            f.truncate()
            f.close()

    def extract_december_files(self):
        # Padrão de busca para encontrar todos os arquivos .zip que correspondem ao padrão
        padrao_busca = 'BASE_DE_DADOS_CNES_*.zip'

        print(f"iniciando extracao arquivos {padrao_busca}")

        # Diretório onde os arquivos .zip estão localizados
        diretorio_arquivos_zip = path('/cnesrawzip')

        # Encontrar todos os arquivos .zip que correspondem ao padrão de busca
        arquivos_zip_encontrados = glob.glob(f'{diretorio_arquivos_zip}/{padrao_busca}')

        # Função para verificar se o arquivo .csv atende às condições desejadas
        def atende_condicoes(nome_arquivo):
            return (nome_arquivo.startswith(('rlEstabSubTipo', 'tbEstabelecimento', 'tbSubTipo'))
                    and nome_arquivo.endswith('12.csv'))

        # Extrair o conteúdo dos arquivos .zip que atendem às condições
        for arquivo_zip in arquivos_zip_encontrados:
            print("arquivo zip cnes raw encontrado: " + str(arquivo_zip))
            tamanho_arquivo = os.path.getsize(arquivo_zip)
            print(f"Tamanho do arquivo: {tamanho_arquivo} bytes")
            self.fix_bad_file(arquivo_zip)
            with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                for nome_arquivo in zip_ref.namelist():
                    if nome_arquivo.endswith('.csv') and atende_condicoes(nome_arquivo):
                        zip_ref.extract(nome_arquivo, self.arquivo_cnes_raw_path())

        print(f"Conteúdo dos arquivos .csv datasus cnes raw foi extraído com sucesso!")


class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    '''Transport adapter" that allows us to use custom ssl_context.'''

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)

@dataclasses.dataclass
class DownloadIBGEFtp:
    def __post_init__(self):
        self.base_url_uf = urls["IBGE_UF"]
        self.base_url_municipio = urls["IBGE_MUNICIPIO"]

    def arquivo_populacao_estado(self):
        return f'{path("/data/bronze/ibge/censo")}/POP2022_Brasil_e_UFs.xls'

    def arquivo_populacao_municipio(self):
        return f'{path("/data/bronze/ibge/censo")}/POP2022_Municipios.xls'

    def download_populacao_municipio(self, url):
        try:
            print(f"Iniciando download do arquivo {url}")
            session = requests.session()
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ctx.options |= 0x4
            session.mount('https://', CustomHttpAdapter(ctx))
            response = session.get(url)
            if response.status_code == 200:
                with open(self.arquivo_populacao_municipio(), 'wb') as arquivo:
                    arquivo.write(response.content)
                print(f'O arquivo foi baixado e salvo em {self.arquivo_populacao_municipio()}')
            print(f"Arquivo {self.arquivo_populacao_municipio()} baixado com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar o arquivo de populacao municipios: {e}")

    def download_populacao_estado(self, url):
        try:
            print(f"Iniciando download do arquivo {url}")
            session = requests.session()
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ctx.options |= 0x4
            session.mount('https://', CustomHttpAdapter(ctx))
            response = session.get(url)
            if response.status_code == 200:
                with open(self.arquivo_populacao_estado(), 'wb') as arquivo:
                    arquivo.write(response.content)
                print(f'O arquivo foi baixado e salvo em { self.arquivo_populacao_estado()}')
            print(f"Arquivo {self.arquivo_populacao_estado()} baixado com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar o arquivo de populacao estado: {e}")

    def download_censo_previa_2022(self):
        self.download_populacao_municipio(self.base_url_municipio)
        self.download_populacao_estado(self.base_url_uf)


@dataclasses.dataclass
class DownloadSageSaudeHttp:
    def __post_init__(self):
        self.url = urls["SAGE_REGIOES_SAUDE"]

    def arquivo_municipios_com_regiao_saude(self):
        return f'{path("/data/bronze/sagesaude")}/municipios-com-nome-regiao-saude.csv'

    def download_municipios_com_regioes_saude(self):
        data = self._make_request()
        if data is not None:
            df = pd.DataFrame(data)

            df.to_csv(self.arquivo_municipios_com_regiao_saude(),
                      index=False,
                      sep=";",
                      encoding="utf-8")
            print("arquivo municipios-com-nome-regiao-saude.csv baixado com sucesso")

    def _make_request(self):
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making the request: {e}")
            return None


@dataclasses.dataclass
class DownloadDataSusCids:
    def __post_init__(self):
        self.base_url = urls["CIDS"]

    def tabelas_cid_path(self):
        return "/data/bronze/datasus/cids"

    def tabelas_cid(self):
        return f"/data/bronze/datasus/cids/CID-10-CATEGORIAS.CSV"

    def download_file(self):
        try:
            urllib.request.urlretrieve(self.base_url, "/tmp/CID10CSV.zip")
            print(f"Arquivo CID10CSV.zip baixado com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar o arquivo de tabela de cids: {e}")

    def download_tabela_cids(self):
        self.download_file()

    def extrair_arquivos(self):
        # Padrão de busca para encontrar todos os arquivos .zip que correspondem ao padrão
        padrao_busca = 'CID10CSV.zip'

        # Diretório onde os arquivos .zip estão localizados
        diretorio_arquivos_zip = '/tmp'

        # Encontrar todos os arquivos .zip que correspondem ao padrão de busca
        arquivos_zip_encontrados = glob.glob(f'{diretorio_arquivos_zip}/{padrao_busca}')

        # Função para verificar se o arquivo .csv atende às condições desejadas
        def atende_condicoes(nome_arquivo):
            return nome_arquivo == "CID-10-CATEGORIAS.CSV"

        # Extrair o conteúdo dos arquivos .zip que atendem às condições
        for arquivo_zip in arquivos_zip_encontrados:
            with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                for nome_arquivo in zip_ref.namelist():
                    if nome_arquivo.lower().endswith('.csv') and atende_condicoes(nome_arquivo):
                        zip_ref.extract(nome_arquivo, self.tabelas_cid_path())

        print("Conteúdo dos arquivos .csv das tabelas cid foi extraído com sucesso!")


@dataclasses.dataclass
class DownloadGithubIbge:
    def __post_init__(self):
        self.url_estados = urls["GITHUB_ESTADOS"]
        self.url_municipios = urls["GITHUB_MUNICIPIOS"]
        self.destination_path = "/data/bronze/github/ibge"

    def gerar_arquivo_saida_municipios(self):
        return f'{path("/data/bronze/github/ibge")}/municipios-originais.csv'

    def gerar_arquivo_saida_estados(self):
        return f'{path("/data/bronze/github/ibge")}/estados-originais.csv'

    def download_municipios_originais(self):
        data = self._make_request(self.url_municipios)
        if data is not None:
            df = pd.read_csv(io.StringIO(data.decode('utf-8')))

            df.to_csv(self.gerar_arquivo_saida_municipios(),
                      index=False,
                      sep=";",
                      encoding="utf-8")
            print(f"{self.gerar_arquivo_saida_municipios()} baixado com sucesso")

    def download_estados_originais(self):
        data = self._make_request(self.url_estados)
        if data is not None:
            df = pd.read_csv(io.StringIO(data.decode('utf-8')))

            df.to_csv(self.gerar_arquivo_saida_estados(),
                      index=False,
                      sep=";",
                      encoding="utf-8")
            print(f"{self.gerar_arquivo_saida_estados()} baixado com sucesso")

    def _make_request(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            print(f"Error making the request: {e}")
            return None


@dataclasses.dataclass
class DownloadCepAbertoHttp:
    def __post_init__(self):
        self.url = "TODO"

    def arquivo_cep_municipio_df(self):
        return f'{path("/data/gold/sds")}/df_cep_municipio_regiao_saude.csv'

    def download_municipios_cep(self):
        pass

    def _make_request(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            print(f"Error making the request: {e}")
            return None
