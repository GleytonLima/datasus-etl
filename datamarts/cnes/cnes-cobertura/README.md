# Extração dos dados CNES

Estes são arquivos para criação do dashboard de cobertura do CNES.

Foram considerados os anos de 2018 a 2012.

Foram considerados apenas os meses de dezembro de cada ano.

## Tipos de CAPS

O arquivo [gold/caps-tipos.csv](caps-por-tipo/caps-tipos.csv) possui um csv com a lista de tipos de CAPS.
O arquivo é disponibilizado via http.

Fonte: https://sage.saude.gov.br/paineis/planoCrack/lista_caps.php?output=html&

Dicionário de Dados

| Coluna    | Tipo  | Descrição         | Exemplo                                        |
| --------- | ----- | ----------------- | ---------------------------------------------- |
| TIPO      | Texto | Tipo de CAPS      | CAPS I, CAPS II                                |
| Descrição | Texto | Descrição do CAPS | Um campo opcional para com a descrição do CAPS |

## Dados CNES

A pastas [cnes-dadosbrutos](cnes-dadosbrutos) possui os arquivos brutos obtidos no datasus.
Os arquivos são disponibilizados via FTP.

Estes arquivos seguem uma convenção de ter um sufixo do ano e mês, por exemplo `rlEstabSubTipo202212`.

Fonte: http://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp

### Arquivos de Entrada

#### Arquivos rlEstabSubTipo

| Coluna                                      | Tipo     | Descrição         | Exemplo       |
| ------------------------------------------- | -------- | ----------------- | ------------- |
| CO_UNIDADE                                  | Texto    | Tipo de CAPS      | 3205209075151 |
| CO_TIPO_UNIDADE                             | Numérico | Descrição do CAPS | 36            |
| CO_SUB_TIPO_UNIDADE                         | Texto    |                   | 009           |
| TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')        | Data     |                   | "05/09/2019   |
| CO_USUARIO                                  | Texto    |                   | ROMEU         |
| TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY') | Data     |                   | 12/10/2016    |

#### Arquivos tbEstabelecimento

| Coluna                  | Tipo  | Descrição                                                        | Exemplo                                                 |
| ----------------------- | ----- | ---------------------------------------------------------------- | ------------------------------------------------------- |
| CO_UNIDADE              | Texto | Texto                                                            | 2512307567502                                           |
| CO_CNES                 | Texto | Numérico                                                         | 7567502                                                 |
| NO_FANTASIA             | Texto | Texto                                                            | UNIDADE DE ACOLHIMENTO INFANTO JUVENIL DR ZEZITO SERGIO |
| CO_CEP                  | Texto | Texto                                                            | 58755000                                                |
| TP_UNIDADE              | Texto | Numérico                                                         | 70                                                      |
| CO_ESTADO_GESTOR        | Texto | Numérico                                                         | 25                                                      |
| CO_MUNICIPIO_GESTOR     | Texto | Numérico                                                         | 251230                                                  |
| CO_TIPO_ESTABELECIMENTO | Texto | Numérico                                                         | 17                                                      |
| CO_MOTIVO_DESAB         | Texto | Motivo do CAPS estar desabilitado. Caso vazio, o CAPS está ativo | 08                                                      |

#### Arquivos tbSubTipo

| Coluna          | Tipo  | Descrição                    | Exemplo |
| --------------- | ----- | ---------------------------- | ------- |
| CO_TIPO_UNIDADE | Texto | Código do Tipo de Unidade    | 70      |
| CO_SUB_TIPO     | Texto | Código do Subtipo de Unidade | 001     |
| DS_SUB_TIPO     | Texto | Descrição do Subtipo         | CAPS I  |

### Arquivo de Saída

No arquivo de saída, gerado por ano, além da junção dos três arquivos acima, é enriquecido com o ano e mês.

Esses arquivos seguem a convenção de nome [ANO]-cnes_filtrados.csv, por exemplo, 2022-cnes-filtrados.

| Coluna                  | Tipo  | Descrição | Exemplo                                                 |
| ----------------------- | ----- |-----------| ------------------------------------------------------- |
| CO_UNIDADE              | Texto | Texto     | 2512307567502                                           |
| CO_CNES                 | Texto | Numérico  | 7567502                                                 |
| NO_FANTASIA             | Texto | Texto     | UNIDADE DE ACOLHIMENTO INFANTO JUVENIL DR ZEZITO SERGIO |
| CO_CEP                  | Texto | Texto     | 58755000                                                |
| TP_UNIDADE              | Texto | Numérico  | 70                                                      |
| CO_ESTADO_GESTOR        | Texto | Numérico  | 25                                                      |
| CO_MUNICIPIO_GESTOR     | Texto | Numérico  | 251230                                                  |
| CO_TIPO_ESTABELECIMENTO | Texto | Numérico  | 17                                                      |
| ANO                     | Texto | Ano       | 2022                                                    |
| MES                     | Texto |           | 12                                                      |
| CO_SUB_TIPO_UNIDADE     | Texto |           | 001                                                     |
| DS_SUB_TIPO             | Texto |           | CAPS I                                                  |

# População dos Municípios/Estados

## Arquivos de Entrada

Baseados na prévia do censo de 2022. Os arquivos estão disponibilizados em formato xls não padronizado (com colunas de
cabeçalho e rodapé)

Fonte: [Censo 2022 prévia](https://www.ibge.gov.br/estatisticas/sociais/saude/22827-censo-demografico-2022.html?=&t=resultados)

## Arquivos de Saída

O arquivo [silver/POP2022_Brasil_e_UFs.csv](silver/POP2022_Brasil_e_UFs.csv) possui os dados tratados de população dos
estados.

| Coluna           | Tipo     | Descrição           | Exemplo  |
| ---------------- | -------- | ------------------- | -------- |
| ESTADO_NOME      | Texto    | Nome do Estado      | Amazonas |
| ESTADO_POPULACAO | Numerico | Populacao do estado | 3952262  |

O arquivo [silver/POP2022_Municipios.csv](silver/POP2022_Municipios.csv) possui os dados tratados de população dos
municipios.

ESTADO_SIGLA;MUNICIPIO_NOME;MUNICIPIO_POPULACAO;MUNICIPIO_CODIGO

| Coluna              | Tipo     | Descrição                | Exemplo |
| ------------------- | -------- | ------------------------ | ------- |
| ESTADO_SIGLA        | Texto    | Sigla do Estado          | AM      |
| MUNICIPIO_NOME      | Texto    | Nome do Município        | Manaus  |
| MUNICIPIO_POPULACAO | Numérico | População do Município   | 2054731 |
| MUNICIPIO_CODIGO    | Texto    | Código IBGE do MUnicípio | 130260  |

# Municípios

Lista dos Municípios e estados do Brasil

https://github.com/kelvins/Municipios-Brasileiros/tree/main/csv

Lista de Municípios com os respectivos regionais de saúde

https://sage.saude.gov.br/paineis/regiaoSaude/lista.php?output=html&

# Map Box

Mapbox é um fornecedor americano de mapas online personalizados para sites e aplicativos como Foursquare, Lonely Planet,
Financial Times, The Weather Channel, Instacart Inc. e Snapchat.

Existe um [visual específico para utilização do Mapbox com Power BI](https://docs.mapbox.com/help/tutorials/power-bi/).

## Regionais da saúde GeoJSON

https://github.com/lansaviniec/shapefile_das_regionais_de_saude_sus/blob/master/BR_Regionais_Simplificado.geojson

## Exemplos criados

### Regionais de Saúde

Vector Tile Url Level 1: mapbox://gleyton.3tqw1k2u
Source Layer Name Level 1: BR_Regionais_Simplificado-0j2yyx
Vector Property Level 1: reg_id

### Estados

Vector Tile Url Level 1: mapbox://gleyton.bzhkkbhb
Source Layer Name Level 1: uf-90p7k4
Vector Property Level 1: GEOCODIGO

### Municipios

Vector Tile Url Level 1: gleyton.0bhm43es
Source Layer Name Level 1: geojs-100-mun-simplificado-2igowx
Vector Property Level 1: id

## Vídeos Mapbox

https://www.youtube.com/watch?v=0w589b5_Z3o

https://www.youtube.com/watch?v=0aX60Bq6ZXI

## Links Úteis

https://mapshaper.org/: Um link útil para conversão de [GeoJSON](https://geojson.org/)
para [TopoJSON](https://github.com/topojson/topojson).
Os mapas nativos do PowerBI somente suportam TopoJSON para poligonos customizados.

# Conteúdos Adicionais

Vídeos sobre configuração de Mapas com Power BI

https://www.youtube.com/watch?v=svw7DQIyJ58

https://www.youtube.com/watch?v=JWCPeKd0p8I

Videos sobre configuração de Mapas com Power BI e Mapbox

https://www.youtube.com/watch?v=0w589b5_Z3o
