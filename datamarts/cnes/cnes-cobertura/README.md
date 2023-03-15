# Extração dos dados CNES

Estes são arquivos para criação do dashboard de cobertura do CNES.

Foram considerados os anos de 2018 a 2012.

Foram considerados apenas os meses de dezembro de cada ano.

## Tipos de Caps

A pasta [caps-por-tipo/caps-tipos.csv](caps-por-tipo/caps-tipos.csv) possui um csv com a lista de tipos de CAPS.
O arquivo é disponibilizado via http.

Fonte: https://sage.saude.gov.br/paineis/planoCrack/lista_caps.php?output=html&

Dicionário de Dados

| Coluna    | Tipo  | Descrição         | Exemplo                                        |
|-----------|-------|-------------------|------------------------------------------------|
| TIPO      | Texto | Tipo de CAPS      | CAPS I, CAPS II                                |     
| Descrição | Texto | Descrição do CAPS | Um campo opcional para com a descrição do CAPS |

## Dados CNES

A pastas [cnes-dadosbrutos](cnes-dadosbrutos) possui os arquivos brutos obtidos no datasus.
Os arquivos são disponibilizados via FTP.

Estes arquivos seguem uma convenção de ter um sufixo do ano e mês, por exemplo `rlEstabSubTipo202212`.

Fonte: http://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp

### Arquivos de Entrda

#### Arquivos rlEstabSubTipo

| Coluna                                      | Tipo     | Descrição         | Exemplo       |
|---------------------------------------------|----------|-------------------|---------------|
| CO_UNIDADE                                  | Texto    | Tipo de CAPS      | 3205209075151 |     
| CO_TIPO_UNIDADE                             | Numérico | Descrição do CAPS | 36            |
| CO_SUB_TIPO_UNIDADE                         | Texto    |                   | 009           |
| TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')        | Data     |                   | "05/09/2019   |
| CO_USUARIO                                  | Texto    |                   | ROMEU         |
| TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY') | Data     |                   | 12/10/2016    |

#### Arquivos tbEstabelecimento

| Coluna                  | Tipo  | Descrição                                                        | Exemplo                                                 |
|-------------------------|-------|------------------------------------------------------------------|---------------------------------------------------------|
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
|-----------------|-------|------------------------------|---------|
| CO_TIPO_UNIDADE | Texto | Código do Tipo de Unidade    | 70      |     
| CO_SUB_TIPO     | Texto | Código do Subtipo de Unidade | 001     |
| DS_SUB_TIPO     | Texto | Descrição do Subtipo         | CAPS I  |

### Arquivo de Saída

No arquivo de saída, gerado por ano, além da junção dos três arquivos acima, é enriquecido com o ano e mês.

Esses arquivos seguem a convenção de nome [ANO]-cnes_filtrados.csv, por exemplo, 2022-cnes-filtrados.

| Coluna                  | Tipo  | Descrição                                                        | Exemplo                                                 |
|-------------------------|-------|------------------------------------------------------------------|---------------------------------------------------------|
| CO_UNIDADE              | Texto | Texto                                                            | 2512307567502                                           |
| CO_CNES                 | Texto | Numérico                                                         | 7567502                                                 |
| NO_FANTASIA             | Texto | Texto                                                            | UNIDADE DE ACOLHIMENTO INFANTO JUVENIL DR ZEZITO SERGIO |
| CO_CEP                  | Texto | Texto                                                            | 58755000                                                |
| TP_UNIDADE              | Texto | Numérico                                                         | 70                                                      |
| CO_ESTADO_GESTOR        | Texto | Numérico                                                         | 25                                                      |
| CO_MUNICIPIO_GESTOR     | Texto | Numérico                                                         | 251230                                                  |
| CO_TIPO_ESTABELECIMENTO | Texto | Numérico                                                         | 17                                                      |     
| ANO                     | Texto | Motivo do CAPS estar desabilitado. Caso vazio, o CAPS está ativo | 2022                                                    |
| MES                     | Texto |                                                                  | 12                                                      |
| CO_SUB_TIPO_UNIDADE     | Texto |                                                                  | 001                                                     |
| DS_SUB_TIPO             | Texto |                                                                  | CAPS I                                                  |

# Conteúdos Adicionais

Vídeos sobre configuração de Mapas

https://www.youtube.com/watch?v=svw7DQIyJ58

https://www.youtube.com/watch?v=JWCPeKd0p8I
