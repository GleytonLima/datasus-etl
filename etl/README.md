# Sobre o projeto

Este projeto tem como objetivo gerar arquivos que podem ser usados para gerar um painel de visualização do estado atual da saúde mental no Brasil.

O processo de geração dos arquivos é composto por 3 etapas:

1. Extração de dados
2. Transformação dos dados
3. Visualização dos dados

Já os arquivos gerados são organizados inicialmente em 3 pastas de acordo com o nível de refinamento dos dados, seguindo a chamada "arquitetura em medalhão", que é uma arquitetura de dados que organiza os dados em 3 níveis de refinamento, conforme tabela a seguir:

| Nível | Descrição |
| ----- | --------- |
| Bronze | Dados brutos |
| Silver | Dados refinados |
| Gold | Dados enriquecidos |

Os arquivos gerados em cada pasta são organizados em subpastas de acordo com a fonte de dados. A tabela a seguir mostra a organização dos arquivos nas pastas bronze, silver e gold.

| Pasta | Descrição |
| ----- | --------- |
| bronze/datasus | Arquivos brutos baixados do ftp do datasus |
| bronze/ibge | Arquivos brutos baixados das fontes de dados do IBGE |
| bronze/sagesaude | Arquivos brutos baixados do SAGE |
| bronze/github | Arquivos brutos baixados do github |
| silver/ibge/censo | Arquivos refinados do censo do IBGE |
| gold/caps | Arquivos enriquecidos com dados dos CAPS (Centro de Atenção Psicossocial) |
| gold/datasus | Arquivos enriquecidos com dados do datasus |
| gold/sds | Arquivos enriquecidos gerados pela própria SDS (Secretaria de Situação da UNB) para auxiliar na visualização dos dados |

Os arquivos das pastas bronze e silver não são versionados, pois são arquivos brutos e de maior tamanho. Já os arquivos da pasta gold são versionados, pois são arquivos enriquecidos e de menor tamanho.

## Extração de dados

A extração de dados é feita por meio de scripts python que baixam arquivos das diferentes fontes de dados. Os scripts estão na pasta [scripts/extract](scripts/extract).

Na tabela a seguir, estão listados os scripts de extração disponíveis e os arquivos que eles baixam.

| Script | Descrição |
| ------ | --------- |
| [download_censo.py](scripts/extract/download_censo.py) | Baixa os arquivos do censo do IBGE |
| [download_cid.py](scripts/extract/download_cid.py) | Baixa os arquivos da tabela CID-10 |
| [download_cnes_ep.py](scripts/extract/download_cnes_ep.py) | Baixa os arquivos datasus do CNES EP (Equipes) |
| [download_cnes_lt.py](scripts/extract/download_cnes_lt.py) | Baixa os arquivos datasus do CNES LT (Leitos) |
| [download_cnes_raw.py](scripts/extract/download_cnes_raw.py) | Baixa os arquivos CNES RAW (Estabelecimentos Bruto) |
| [download_cnes_sr.py](scripts/extract/download_cnes_sr.py) | Baixa os arquivos CNES SR (Serviços Especializados) |
| [download_cnes_st.py](scripts/extract/download_cnes_st.py) | Baixa os arquivos CNES ST (Establecimentos) |
| [download_ibge_github.py](scripts/extract/download_ibge_github.py) | Baixa os arquivos do IBGE referente a municipios e estados disponíveis no github |
| [download_regioes_saude_municipios.py](scripts/extract/download_regioes_saude_municipios.py) | Baixa os arquivos de regiões de saúde e municípios do SAGE |
| [download_sim.py](scripts/extract/download_sim.py) | Baixa os arquivos do SIM (Sistema de Informações de Mortalidade) |

Já os seguintes scripts são utilitários que auxiliam na extração de dados:

| Script | Descrição |
| ------ | --------- |
| [download_util.py](scripts/extract/download_util.py) | Contém funções utilitárias para download de arquivos |
| [dbc_to_csv.R](scripts/extract/dbc_to_csv.R) | Script em R que converte arquivos .dbc para .csv |


As configurações para execução dos scripts estão no seguinte arquivo:

| Arquivo | Descrição |
| ------ | --------- |
| [config.json](scripts/extract/config.json) | Arquivo de configuração com as urls dos arquivos a serem baixados e outras configurações |

### Sobre os arquivos .dbc disponibilizados pelo Datasus

Os arquivos que são disponibilizados pelo datasus são arquivos .dbc que corresponde a um arquivo DBF compactado. A sigla "DBF" significa "Data Base File", cujo tradução literal é "Arquivo de Base de Dados". Esse tipo de arquivo foi popularizado por um dos primeiros programas gerenciadores de bases de dados a serem lançados no mercado, o "dBase", conforme [descrito pelo Datasus](http://universus3.datasus.gov.br/universus/tabwin/unidade1/tema3/unid1_tema3_tela5.php#:~:text=O%20formato%20DBC%20corresponde%20a,DBF%20do%20programa%20TabWin.).

O formato de arquivo DBC é um formato proprietário e não é suportado por ferramentas de visualização de dados como o Microsoft Power BI, por exemplo. Por isso, é necessário converter os arquivos .dbc para .csv.
 Não existe uma ferramenta oficial para conversão de arquivos .dbc para .csv. Porém, existe uma biblioteca disponível no github chamada [read.dbc](https://github.com/danicat/read.dbc) que permite ler arquivos .dbc em .csv. Essa biblioteca é escrita em R.

Portanto, neste projeto utilizamos o python para baixar os arquivos do ftp do datasus e o R para converter os arquivos .dbc para .csv.

### Sobre o ambiente de desenvolvimento

Configurar um ambiente de desenvolvimento com python e R pode ser um pouco trabalhoso. Por isso, para facilitar o processo de extração de dados, criamos uma imagem docker que já possui todas as dependências necessárias para executar os scripts de extração de dados e conversão de arquivos .dbc para .csv. Desse modo, o principal requisito para executar os scripts de extração de dados é ter o docker instalado na máquina.

### Sobre o arquivo de configuração

Para externalizar as configurações de conexão com o ftp do datasus, criamos um arquivo de configuração chamado [config.json](scripts/extract/config.json). O arquivo de configuração possui a seguinte estrutura:

```json
{
  "agendamento_cron": "31 * * * *",
  "ufs": [
    "AC"
  ],
  "anos": [
    2018
  ],
  "meses": [
    12
  ],
  "urls": {
    "CNES": "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/"
  },
  "system_config": {
    "CNES": {
      "LT": {
        "description": "Leitos - A partir de Out/2005",
        "start_month": 10,
        "start_year": 2005
      }
  }
}
```

A tabela abaixo descreve as configurações do arquivo [config.json](scripts/extract/config.json):

| Configuração | Descrição | Exemplo |
| --- | --- | --- |
| agendamento_cron | Cron de agendamento da execução do script | "31 * * * *" |
| ufs | Lista de UFs para baixar os dados | ["AC"] |
| anos | Lista de anos para baixar os dados | [2018] |
| meses | Lista de meses para baixar os dados | [12] |
| urls | Lista de urls para baixar os dados | {"CNES": "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/"} |
| system_config | Configurações específicas de cada fonte de dados | {"CNES": {"LT": {"description": "Leitos - A partir de Out/2005", "start_month": 10, "start_year": 2005}}} |

## Transformação dos dados

Os scripts de transformação dos dados estão no diretório [scripts/transform](scripts/transform).

| Script | Descrição |
| ------ | --------- |
| [base.py](scripts/base.py) | Script base para transformação de dados |
| [caps.py](scripts/caps.py) | Script base para transformação dos dados de CAPS |
| [equipes.py](scripts/equipes.py) | Script base para transformação dos dados de equipes de saúde |
| [leitos.py](scripts/leitos.py) | Script base para transformação dos dados de leitos |
| [servico_especializado.py](scripts/servico_especializado.py) | Script base para transformação dos dados de serviços especializados |
| [sim.py](scripts/sim.py) | Script base para transformação dos dados de SIM |
| [transform_base.py](scripts/transform_base.py) | Script para execucao da transformação dos dados de municípios, UFs e regiões |
| [transform_caps.py](scripts/transform_caps.py) | Script para execucao da transformação dos dados de CAPS |
| [transform_cnes_ep_consultorio_rua.py](scripts/transform_cnes_ep_consultorio_rua.py) | Script para execucao da transformação dos dados de consultório na rua |
| [transform_cnes_ep_emaesm.py](scripts/transform_cnes_ep_emaesm.py) | Script para execucao da transformação dos dados das equipes multiprofissionais de atenção especializada em saúde mental |
| [transform_cnes_ep_esf_eap.py](scripts/transform_cnes_ep_esf_eap.py) | Script para execucao da transformação dos dados das equipes de atenção primária |
| [transform_cnes_esf_nasf.py](scripts/transform_cnes_esf_nasf.py) | Script para execucao da transformação dos dados das equipes de atenção primária e núcleo de apoio à saúde da família |
| [transform_cnes_lt_leitos.py](scripts/transform_cnes_lt_leitos.py) | Script para execucao da transformação dos dados de leitos |
| [transform_cnes_sr_sap.py](scripts/transform_cnes_sr_sap.py) | Script para execucao da transformação dos dados de serviços ambulatoriais psiquiátricos |
| [transform_cnes_sr_shsm.py](scripts/transform_cnes_sr_shsm.py) | Script para execucao da transformação dos dados de serviços hospitalares de saúde mental |
| [transform_cnes_sr_srt.py](scripts/transform_cnes_sr_srt.py) | Script para execucao da transformação dos dados de serviços residenciais terapêuticos |
| [transform_cnes_sr_ua.py](scripts/transform_cnes_sr_ua.py) | Script para execucao da transformação dos dados de unidades de acolhimento |
| [transform_cnessr_unb_sds.py](scripts/transform_cnessr_unb_sds.py) | Script para execucao da geração de arquivos auxiliares |
| [transform_sim_do.py](scripts/transform_sim_do.py) | Script para execucao da transformação dos dados de óbitos |

### Executando os scripts de transformação

Na pasta [docker-compose-files](docker-compose-files) existem arquivos de configuração para execução dos scripts de transformação dos dados via docker-compose.

Os arquivos docker-compose possuem a seguinte estrutura:

```yaml
version: '3.3'
services:
  datasusftp:
    container_name: datasus-etl
    volumes:
      - '../scripts/extract:/app/scripts'
      - '../data:/data'
    environment:
      - SCRIPT_NAME=download_cnes_st.py
    image: gleytonlima/datasusftp
```

Neste exemplo, trata-se de um arquivo docker-compose que executa um container com a imagem [gleytonlima/datasusftp](https://hub.docker.com/r/gleytonlima/datasusftp) que possui os volumes mapeados para a pasta [scripts/extract](scripts/extract) e para a pasta [data](data) e a variável de ambiente SCRIPT_NAME com o nome do script a ser executado.

Para executar os scripts de transformação dos dados, basta executar o comando abaixo na pasta [docker-compose-files](docker-compose-files):

```bash
docker-compose -f <arquivo de configuração> up
```

## Visualização dos dados

Para visualização dos dados foi utilizado o Microsoft Power BI. O painel modelo está disponível na pasta [painel-modelo](painel-modelo). O arquivo [painel-saude-mental.pbix](painel-modelo/painel-saude-mental.pbix) é o arquivo do painel modelo que se conecta aos arquivos de dados gerados pelos scripts de transformação. Ele foi testado com o Power BI Desktop Versão 2.118.1063.0 64-bit (junho de 2023).


# Execução dos Scripts com Docker

Conforme descrito anteriormente, os scripts de transformação dos dados podem ser executados via docker-compose. Para isso, a lista a seguir apresenta os nomes dos scripts e as variáveis de ambiente necessárias para execução dos scripts.

1. CNES-ST

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_st.py gleytonlima/datasusftp
```

2. CNES-LT

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_lt.py gleytonlima/datasusftp
```

3. CNES-SR

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_sr.py gleytonlima/datasusftp
```

4. CNES-EP

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_ep.py gleytonlima/datasusftp
```

5. CNES-RAW

Dados brutos do CNES. Útil para extrair dados que não estão nos arquivos padrão disponibilizados
pelo DATASUS

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_raw.py gleytonlima/datasusftp
```

6. IBGE - Previa Senso 2022

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_censo.py gleytonlima/datasusftp
```

7. SAGE - Regióes de Saúde e Municípios

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_regioes_saude_municipios.py gleytonlima/datasusftp
```

8. CID

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cid.py gleytonlima/datasusftp
```

9. IBGE - Github - Lista de Municipios e Estados

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_ibge_github.py gleytonlima/datasusftp
```

MODO ITERATIVO

Comando em modo interativo para navegar pelos arquivos do container em execução

```commandline
docker run -it --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_st.py gleytonlima/datasusftp bash
```

# Transformação - Executando com Docker

BASE

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_base.py gleytonlima/datasusftp
```

FICHA #1

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_ep_esf_eap.py gleytonlima/datasusftp
```

FICHA #2 e #3

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_ep_esf_nasf.py gleytonlima/datasusftp
```

FICHA #4

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_ep_consultorio_rua.py gleytonlima/datasusftp
```

CAPS FICHAS #5, #6, #7, #8 e #9

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_caps.py gleytonlima/datasusftp
```


FICHA #10

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_ep_emaesm.py gleytonlima/datasusftp
```

FICHA #11

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_sr_sap.py gleytonlima/datasusftp
```

FICHA #12

TODO: Manual a partir do tabnet

FICHA #13

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_sr_shsm.py gleytonlima/datasusftp
```

FICHA #14 e #15

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_sr_ua.py gleytonlima/datasusftp
```

FICHA #16

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_sr_srt.py gleytonlima/datasusftp
```

## Recriando a Imagem Docker

Caso seja necessário algum ajuste na imagem, é possível executar o comando:

```commandline
docker login
docker build -t gleytonlima/datasusftp .
docker push gleytonlima/datasusftp
```

Observe que este é apenas um exemplo. Pode ser usada qualquer conta do dockerhub ou quaisquer outros
repositórios de imagens.

## Sobre a aplicação criada para o ETL

Detalhes da aplicação criada para o ETL podem ser encontrados na pasta [app](../app/README.md)