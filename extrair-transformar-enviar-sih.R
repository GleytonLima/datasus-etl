if (!require("microdatasus")) {
    remotes::install_github("rfsaldanha/microdatasus")
}
library(arrow)
library(microdatasus)
library(lubridate)

sistema_informacao <- "SIH-RD"

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 2) {
    print("Dois argumentos obrigatórios: ano_mes (no formato YYYY-MM) e uf com dois bytes (AM, DF etc.)")
    stopifnot(FALSE)
}

uf <- args[2]
ano_mes_atual <- args[1]
ano <- substr(ano_mes_atual, 1, 4)
mes <- substr(ano_mes_atual, 6, 7)

nome_arquivo <- paste0(ano_mes_atual, "-", uf, "-", sistema_informacao, ".parquet")

dados_originais <- fetch_datasus(month_start = mes, month_end = mes, year_start = ano, year_end = ano, uf = uf, information_system = sistema_informacao)
dados_pre_processados <- process_sih(dados_originais)

write_parquet(
    dados_pre_processados <- process_sih(dados_originais),
    nome_arquivo
)
