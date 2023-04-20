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