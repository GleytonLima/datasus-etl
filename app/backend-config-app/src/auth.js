const jwt = require("jsonwebtoken");
const { secretKey } = require("./config");

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

module.exports = {
  authenticate,
};
