# Extração dos dados CNES EP

Estes são arquivos para criação do dashboard com dados relacionados com CNES EP.

Foram considerados os anos de 2013 a 2022.

Foram considerados os meses de dezembro de cada ano

# Municípios do Brasil Mapa

Arquivo original: https://raw.githubusercontent.com/tbrugz/geodata-br/master/geojson/geojs-100-mun.json

O arquivo original tem 21MB. Mas pode ser simplificado por meio do site https://mapshaper.org/

O json foi analisado com https://geojsonlint.com/

E em seguida corrigido em https://mapstertech.github.io/mapster-right-hand-rule-fixer/

Alguns municipios podem ter ficado sem o polígono. É necessário corrigir manualmente de MultiPolygon para Polygon
de volta de copiar as coordenadas do arquivo original

# Fichas Associadas

## 1. Número e Cobertura populacional estimada de Equipes de Saúde da Família (eSF) e Atenção Primária (eAP)

### Código

1

### Tipo

Estrutura

### Interpretação

Estima a cobertura de equipes ESF e EAP

### Fonte

Sistema do Cadastro Nacional de Estabelecimentos (SCNES) / CNES-EP;
Instituto Brasileiro de Geografia e Estatística (IBGE).

### Variáveis

#### CNES-EP

TIPO_EQP - Tipo de equipe (código de equipe aqui)

DT_DESAT - Ano e Mês da Data de DESATIVAÇÃO da Equipe (considerar desativadas no mês e ano)

COMPETEN - Ano e Mês de competência da informação (AAAAMM)

CODUFMUN - Código do município do estabelecimento

COD_CEP - Código do CEP do estabelecimento (para o DF)

#### IBGE

- UF - Estado
- COD. UF - Código IBGE do Estado
- NOME DO MUNICÍPIO
- POPULAÇÃO ESTIMADA

### Periodicidade

Anual

### Método de cálculo

Filtrar os municípios que possuem menos de 15 mil habitantes e em seguida identificar o quantitativo de equipes do
Núcleo Ampliado de Saúde da Família e Equipes de Saúde da Família cadastradas.

### Observação

É relevante expor inclusive os municípios com população inferior a 15 mil habitantes e que não possuem NASF, nem ESF.

### Visualização no painel

Tabela com a visualização geral do Brasil e, possibilidade de filtro conforme o Estado, para demonstrar quais os
municípios pertencentes, com menos de 15 mil hab, possuem NASF e ESF ou não possuem.

### Arquivos Relacionados

[tranformar_cnes_ep_esf_eap.py](tranformar_cnes_ep_esf_eap.py)
[tranformar_cnes_ep_esf_eap.py](tranformar_cnes_ep_esf_eap.py)

#### Resumo do processo de extração

1. Para obter os arquivos bronze é possível baixar utilizando a
   imagem [gleytonlima/datasusftp](https://hub.docker.com/r/gleytonlima/datasusftp)