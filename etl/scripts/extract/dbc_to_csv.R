library(read.dbc)

# Função para imprimir mensagens de log no console
log_message <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat("[DEBUG]", timestamp, message, "\n")
}

# Obter os argumentos da linha de comando
args <- commandArgs(trailingOnly = TRUE)

# Verificar se foram fornecidos argumentos suficientes
if (length(args) < 2) {
  stop("Forneça pelo menos dois argumentos: arquivo de entrada e arquivo de saída")
}

# Primeiro argumento é o local do arquivo de entrada
input_path <- args[1]

# Segundo argumento é o arquivo de saída (csv)
output_path <- args[2]

# define o caminho completo para a pasta em que os arquivos .dbc estão armazenados
caminho <- file.path(input_path)

log_message(paste("Arquivo de entrada: ", input_path))

# lista todos os arquivos .dbc na pasta
arquivos <- list.files(path = caminho, pattern = "\\.dbc$", full.names = TRUE)

log_message(paste("arquivos: ", arquivos))

# loop através de todos os arquivos na lista
for (i in 1:length(arquivos)) {

  # lê o arquivo .dbc
  dados <- read.dbc(arquivos[i])

  # converte os dados em um dataframe
  df <- as.data.frame(dados)

  # define o nome do arquivo CSV
  nome_csv <- basename(gsub("\\.dbc$", ".csv", arquivos[i]))

  log_message(paste("nome_csv: ", nome_csv))

  # Verifica se a pasta de saída existe, e se não, cria-a
  if (!file.exists(output_path)) {
    dir.create(output_path, recursive = TRUE)
    log_message(paste("Pasta de saída", output_path, "criada."))
  }

  # Define o caminho completo para a pasta "bronze"
  caminho_bronze <- file.path(output_path, nome_csv)
  
  # grava o dataframe em um arquivo CSV
  write.csv2(df, file = caminho_bronze, row.names = FALSE, fileEncoding = "utf-8")

}