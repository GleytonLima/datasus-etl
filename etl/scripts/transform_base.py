from base import Populacao, Estado, Municipio, RegiaoSaude

print("iniciando tranformacao arquivos base")

populacao = Populacao()
populacao.converter_censo_municipio_xls_em_csv()
populacao.converter_censo_uf_xls_em_csv()
estado = Estado()
estado.enriquecer_estados()
municipio = Municipio()
municipio.enriquecer_municipios()
regiao_saude = RegiaoSaude()
regiao_saude.gerar_arquivo_regiao_saude()

print("finalizado tranformacao arquivos base")
