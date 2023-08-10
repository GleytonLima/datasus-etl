# Baixando arquivos do DataSus com FTP e [read.dbc](https://github.com/danicat/read.dbc)

Os scripts desta pasta são exemplos de como obter e converter os arquivos dbc do ftp do datasus para csv.

Vale ressaltar que, para o processo de geração de dashboards, os dados contidos nestes arquivos precisam ser
enriquecidos para ser possível gerar paineis mais ricos como mapas e efetuar cálculos que envolvam dados
populacionais, por exemplo.

## Contexto

Neste método de download, efetuamos o download por meio do próprio python diretamente do ftp do datasus.

Para a conversão do arquivo .dbc em .csv utilizamos uma biblioteca em R [read.dbc](https://github.com/danicat/read.dbc)

## Extração - Executando com Docker

O [read.dbc](https://github.com/danicat/read.dbc) foi criado para ser usado em ambientes com o R em execução. 
Assim para conseguir usar suas funcionalidades, ele pode ser encapsulado em uma imagem docker e executado como container.

A imagem docker [gleytonlima/datasusftp](https://hub.docker.com/r/gleytonlima/datasusftp) foi criada a partir do arquivo
[Dockerfile](Dockerfile).

O nome do script deve ser passado por meio da variavel de embiente `SCRIPT_NAME`.

Para usar o script, por exemplo, para baixar arquivos do CNES-ST no formato CSV, é possível usar o seguinte
commando:

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_st.py gleytonlima/datasusftp
```

Esse comando:
1. passa os scripts a serem executados para o container por meio da flag `-v` (volume)
2. também faz um volume entre a pasta data e a pasta csv do container (onde os arquivos serão baixados)
3. passa a variavel de ambiente com o nome SCRIPT_NAME e valor o caminho relativo do arquivo `.py`

Outros exemplos:

CNES-ST

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_st.py gleytonlima/datasusftp
```

CNES-LT

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_lt.py gleytonlima/datasusftp
```

CNES-SR

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_sr.py gleytonlima/datasusftp
```

CNES-EP

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_ep.py gleytonlima/datasusftp
```

CNES-RAW

Dados brutos do CNES. Útil para extrair dados que não estão nos arquivos padrão disponibilizados
pelo DATASUS

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cnes_raw.py gleytonlima/datasusftp
```

IBGE - Previa Senso 2022

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_censo.py gleytonlima/datasusftp
```

SAGE - Regióes de Saúde e Municípios

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_regioes_saude_municipios.py gleytonlima/datasusftp
```

CID

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts/extract:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=download_cid.py gleytonlima/datasusftp
```

IBGE - Github - Lista de Municipios e Estados

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

CAPS

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_caps.py gleytonlima/datasusftp
```

FICHA #1

```commandline
docker run --rm --name datasus-etl -v $pwd/scripts:/app/scripts -v $pwd/data:/data -e SCRIPT_NAME=transform_cnes_ep_esf_eap.py gleytonlima/datasusftp
```


## Conversão do DBC em CSV

Para conversão do arquivo dbc baixado do FTP do Datasus é utilizado o script [dbc_to_csv.R](scripts/extract/dbc_to_csv.R).
Ele utiliza a biblioteca https://github.com/danicat/read.dbc para fazer a conversão.

## Recriando a Imagem Docker

Caso seja necessário algum ajuste na imagem, é possível executar o comando:

```commandline
docker login
docker build -t gleytonlima/datasusftp .
docker push gleytonlima/datasusftp
```

Observe que este é apenas um exemplo. Pode ser usada qualquer conta do dockerhub ou quaisquer outros
repositórios de imagens.