# Extração dos dados SIM

Estes são arquivos para criação do dashboard com dados relacionados com SIM.

Foram considerados os anos de 2013 a 2021.

Foram considerados todos os meses de cada ano.

## Dados SIM

O SIM é responsável por registrar e fornecer informações sobre as mortes ocorridas 
em todo o território nacional, permitindo a análise e monitoramento da mortalidade 
por diversas causas. Esses dados são importantes para a gestão e formulação de 
políticas públicas relacionadas à saúde.

Fonte: https://datasus.saude.gov.br/transferencia-de-arquivos/#

### Arquivos de Entrada

#### Arquivos DO

As colunas a seguir foram retiradas dos arquivos básicos de DO

| Coluna     | Tipo  | Descrição                                | Exemplo  |
|------------|-------|------------------------------------------|----------|
| CODMUNOCOR | Texto | Código do Munícpio                       | 110002   |     
| DTOBITO    | Data  | Data do óbito no formato ddmmaaaa        | 01012013 |
| CAUSABAS   | Texto | Código CID 10 da causa da morte básica.  | F103     |

### Arquivo de Saída

Os arquivos silver/sim-AAAA.csv contém os dados sumarizados

| Coluna           | Tipo  | Descrição                     | Exemplo |
|------------------|-------|-------------------------------|---------|
| MUNICIPIO_CODIGO | Texto | Texto                         | 110002  |
| MES_ANO          | Texto | Data no formato MM-AAAA       | 05-2015 |
| CAUSABAS         | Texto | Codigo CID 10 da Causa Básica | F10     |
| TOTAL_OBITOS     | Texto | Texto                         | 1       |

