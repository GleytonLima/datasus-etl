const express = require("express");
const bodyParser = require("body-parser");
const jwt = require("jsonwebtoken");
const fs = require("fs");
const cors = require("cors");
const logger = require("./logger");
const corsOptions = {
  origin: "http://localhost:3000",
  methods: "GET,PUT,POST,DELETE",
};
const path = require("path");
const updateCronTask = require("./cron-task");
const { configFilePath, logFilePath } = require("./config");

const app = express();
app.use(cors(corsOptions));

const port = process.env.PORT || 3001;
const secretKey = process.env.SECRET_KEY || "secretpassword";

// Middleware para analisar JSON no corpo das solicitações
app.use(bodyParser.json());

// Obtém o caminho atual do diretório do script
const scriptDirectory = path.dirname(__filename);

// Calcula o caminho para a pasta "etl" duas pastas acima
const etlDirectory = path.resolve(scriptDirectory, "../../etl");

// Muda o diretório de trabalho para a pasta "etl"
process.chdir(etlDirectory);

// Middleware de autenticação
function authenticate(req, res, next) {
  const token = req.headers.authorization;
  if (!token) {
    return res.status(401).json({ message: "Token de autenticação ausente." });
  }

  try {
    jwt.verify(token, secretKey, (err, decoded) => {
      if (err) {
        return res
          .status(401)
          .json({ message: "Token de autenticação inválido." });
      }
      req.user = decoded.user;
      next();
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Erro ao verificar o token de autenticação." });
  }
}

// Endpoint para autenticação
app.post("/api/login", (req, res) => {
  const { username, password } = req.body;

  if (username === process.env.USER && password === process.env.TOKEN) {
    const token = jwt.sign({ user: username }, secretKey, { expiresIn: "1h" });
    return res.status(200).json({ token });
  }

  return res.status(401).json({ message: "Credenciais de login inválidas." });
});

// Endpoint para ler dados do arquivo config.json
app.get("/api/config", authenticate, (req, res) => {
  fs.readFile(configFilePath, "utf8", (err, data) => {
    if (err) {
      logger.info(`Erro ao ler o arquivo ${logFilePath}: `, err);
      return res.status(500).json({ message: "Erro ao ler os dados" });
    }
    try {
      const configData = JSON.parse(data);
      res.status(200).json(configData);
    } catch (parseError) {
      return res.status(500).json({ message: "Erro ao analisar os dados." });
    }
  });
});

// Endpoint para ler os logs
app.get("/api/logs", authenticate, (req, res) => {
  // Lê o arquivo de log
  fs.readFile(logFilePath, "utf8", (err, data) => {
    if (err) {
      return res.status(500).send("Erro ao recuperar registros de log");
    }
    // Divide os registros de log em linhas e converte-os em objetos JSON
    let logs = data
      .split("\n")
      .filter((log) => log) // Remove linhas vazias
      .map((log) => JSON.parse(log)); // Converte para objeto JSON

    // Ordena os registros em ordem decrescente com base na data/hora
    logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    // Define a quantidade de registros por página (pode ser personalizada)
    const logsPerPage = 10;

    // Recupera o número da página a partir dos parâmetros de consulta (query parameters)
    const page = parseInt(req.query.page) || 1;

    // Calcula o índice de início e fim da página atual
    const startIndex = (page - 1) * logsPerPage;
    const endIndex = startIndex + logsPerPage;

    // Divide os registros de log em páginas
    logs = logs.slice(startIndex, endIndex);

    res.send(logs);
  });
});

// Endpoint para atualizar dados no arquivo config.json
app.put("/api/config", authenticate, (req, res) => {
  if (!req.body || typeof req.body !== "object") {
    return res.status(400).json({ message: "Dados inválidos." });
  }
  // validar campo anos
  if (!req.body.anos || !Array.isArray(req.body.anos)) {
    return res
      .status(400)
      .json({ message: "O campo nome é obrigatório e deve ser uma lista" });
  }
  if (req.body.anos.length === 0) {
    return res.status(400).json({ message: "É necessário pelo menos um ano." });
  }
  // validar campo ufs
  if (!req.body.ufs || !Array.isArray(req.body.ufs)) {
    return res
      .status(400)
      .json({ message: "O campo ufs é obrigatório e deve ser uma lista" });
  }
  if (req.body.ufs.length === 0) {
    return res.status(400).json({ message: "É necessário pelo menos uma uf." });
  }
  // validar campo meses
  if (!req.body.meses || !Array.isArray(req.body.meses)) {
    return res
      .status(400)
      .json({ message: "O campo meses é obrigatório e deve ser uma lista" });
  }
  if (req.body.meses.length === 0) {
    return res.status(400).json({ message: "É necessário pelo menos um mês." });
  }

  fs.writeFile(configFilePath, JSON.stringify(req.body, null, 2), (err) => {
    if (err) {
      return res.status(500).json({ message: "Erro ao salvar os dados." });
    }
    updateCronTask();
    return res.status(200).json({ message: "Dados atualizados com sucesso." });
  });
});

// Iniciar o servidor
app.listen(port, () => {
  logger.info(`Servidor rodando na porta ${port}`);
});

updateCronTask();
