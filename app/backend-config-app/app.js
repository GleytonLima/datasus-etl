const express = require('express');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');
const fs = require('fs');
const cors = require('cors');
const corsOptions = {
    origin: 'http://localhost:3000',
    methods: 'GET,PUT,POST,DELETE',
};

const app = express();
app.use(cors(corsOptions));
const port = process.env.PORT || 3001;
const secretKey = 'secretpassword'; // Troque isso por uma chave secreta mais segura

// Middleware para analisar JSON no corpo das solicitações
app.use(bodyParser.json());

// Simulação de dados do arquivo config.json
let configData = {
  anos: [2020, 2021, 2022],
};

// Middleware de autenticação
function authenticate(req, res, next) {
  const token = req.headers.authorization;
  if (!token) {
    return res.status(401).json({ message: 'Token de autenticação ausente.' });
  }

  try {
    jwt.verify(token, secretKey, (err, decoded) => {
      if (err) {
        return res.status(401).json({ message: 'Token de autenticação inválido.' });
      }
      req.user = decoded.user;
      next();
    });
  } catch (error) {
    res.status(500).json({ message: 'Erro ao verificar o token de autenticação.' });
  }
}

// Endpoint para autenticação
app.post('/api/login', (req, res) => {
  const { username, password } = req.body;

  // Verifique se o usuário e a senha correspondem (substitua por lógica de autenticação real)
  if (username === 'teste' && password === 'teste') {
    const token = jwt.sign({ user: username }, secretKey, { expiresIn: '1h' });
    return res.status(200).json({ token });
  } else {
    return res.status(401).json({ message: 'Credenciais de login inválidas.' });
  }
});

// Endpoint para ler dados do arquivo config.json
app.get('/api/config', authenticate, (req, res) => {
  fs.readFile('config.json', 'utf8', (err, data) => {
    if (err) {
      return res.status(500).json({ message: 'Erro ao ler os dados.' });
    }    
    try {
      const configData = JSON.parse(data);
      res.status(200).json(configData);
    } catch (parseError) {
      return res.status(500).json({ message: 'Erro ao analisar os dados.' });
    }
  });
});

// Endpoint para atualizar dados no arquivo config.json
app.put('/api/config', authenticate, (req, res) => {
  const newData = req.body;

  // Atualize os dados no arquivo config.json (substitua por lógica real)
  configData = newData;

  // Salve os dados no arquivo
  fs.writeFile('config.json', JSON.stringify(configData, null, 2), (err) => {
    if (err) {
      return res.status(500).json({ message: 'Erro ao salvar os dados.' });
    }
    return res.status(200).json({ message: 'Dados atualizados com sucesso.' });
  });
});

// Iniciar o servidor
app.listen(port, () => {
  console.log(`Servidor rodando na porta ${port}`);
});
