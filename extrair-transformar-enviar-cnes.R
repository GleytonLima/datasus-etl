if (!require("microdatasus")) {
  remotes::install_github("rfsaldanha/microdatasus")
}
library(arrow)
library(microdatasus)
library(lubridate)

sistema_informacao <- "CNES-ST"

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 2) {
  print("Dois argumentos obrigatórios: ano_mes (no formato YYYY-MM) e uf com dois bytes (AM, DF etc.)")
  stopifnot(FALSE)
}


ano <- args[1]
uf <- args[2]

print(ano)
print(uf)

nome_arquivo <- paste0(ano, "-", uf, "-", sistema_informacao, ".parquet")

dados_originais <- fetch_datasus(month_start = 12, month_end = 12, year_start = ano, year_end = ano, uf = uf, information_system = sistema_informacao)
dados_pre_processados <- process_cnes(dados_originais, information_system = sistema_informacao)

write_parquet(dados_pre_processados, nome_arquivo)
