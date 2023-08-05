library(read.dbc)

# define o caminho completo para a pasta em que os arquivos .dbc estão armazenados
caminho <- file.path("/data")

# lista todos os arquivos .dbc na pasta
arquivos <- list.files(path = caminho, pattern = "\\.dbc$", full.names = TRUE)

# loop através de todos os arquivos na lista
for (i in 1:length(arquivos)) {

  # lê o arquivo .dbc
  dados <- read.dbc(arquivos[i])

  # converte os dados em um dataframe
  df <- as.data.frame(dados)

  # define o nome do arquivo CSV
  nome_csv <- gsub("\\.dbc$", ".csv", arquivos[i])

  # Define o caminho completo para a pasta "bronze"
  caminho_bronze <- file.path("/data", basename(nome_csv))
  
  # grava o dataframe em um arquivo CSV
  write.csv2(df, file = caminho_bronze, row.names = FALSE, fileEncoding = "utf-8")

}