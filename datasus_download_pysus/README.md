# Baixando arquivos do DataSus com [PySUS](https://github.com/AlertaDengue/PySUS)

Este método usando a biblioteca PySUS falha para alguns arquivos como por exemplo:

- CNES-ST de dez/19, entre outros. 

É uma issue conhecida do projeto.

## Contexto

PySUS é um pacote reúne um conjunto de utilitários para manipulação de bancos de dados públicos publicados 
pelo DATASUS do Brasil.

## Executando o PySUS com Docker

O PySUS foi criado para ser usado em ambientes linux. Assim para conseguir usar suas funcionalidades,
ele pode ser encapsulado em uma imagem docker e executado como container.

A imagem docker [gleytonlima/pysusscripts](https://hub.docker.com/r/gleytonlima/pysusscripts) foi criada a partir do arquivo
[Dockerfile](Dockerfile).

O nome do script deve ser passado por meio da variavel de embiente `SCRIPT_NAME`.

Para usar o script, por exemplo, para baixar arquivos do CNES-EP no formato CSV, é possível usar o seguinte
commando:

```commandline
docker run  -v $pwd/scripts:/app/scripts -v $pwd/data:/csv -e SCRIPT_NAME=scripts/download_cnes_ep.py gleytonlima/pysusscripts
```

Esse comando:
1. passa os scripts a serem executados para o container por meio da flag `-v` (volume)
2. também faz um volume entre a pasta data e a pasta csv do container (onde os arquivos serão baixados)
3. passa a variavel de ambiente com o nome SCRIPT_NAME e valor o caminho relativo do arquivo `.py`

Outros exemplos:

```commandline
docker run  -v $pwd/scripts:/app/scripts -v $pwd/data:/csv -e SCRIPT_NAME=scripts/download_cnes_st.py gleytonlima/pysusscripts

docker run -it -v $pwd/scripts:/app/scripts -v $pwd/data:/csv -e SCRIPT_NAME=scripts/download_cnes_st.py gleytonlima/pysusscripts bash
```


## Recriando a Imagem Docker

Caso seja necessário algum ajuste na imagem, é possível executar o comando:

```commandline
docker login
docker build -t gleytonlima/pysusscripts .
docker push gleytonlima/pysusscripts
```

Observe que este é apenas um exemplo. Pode ser usada qualquer conta do dockerhub ou quaisquer outros
repositórios de imagens.