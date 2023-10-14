const { expect } = require("chai");
const sinon = require("sinon");
const jwt = require("jsonwebtoken");
const request = require("supertest");
const app = require("../src/app");
const { secretKey } = require("../src/config");

describe("Authentication Middleware", () => {
  let server;

  before(() => {
    server = app.app;
  });

  after(() => {
    app.shutdown(); // Encerrar o servidor após os testes
  });

  it("deve autenticar usuários com token JWT válido", async () => {
    // Criar um token JWT válido para um usuário fictício
    const token = jwt.sign({ user: "seu-username" }, secretKey);

    const response = await request(server)
      .get("/api/config")
      .set("Authorization", `${token}`);

    expect(response.status).to.equal(200);
    expect(response.body).to.have.property("anos");
  });

  it("não deve autenticar usuários com token JWT inválido", async () => {
    // Criar um token JWT inválido
    const token = "token-jwt-invalido";

    const response = await request(server)
      .get("/api/config")
      .set("Authorization", `${token}`);

    expect(response.status).to.equal(401);
    expect(response.body).to.have.property(
      "message",
      "Token de autenticação inválido."
    );
  });

  it("não deve autenticar usuários sem token JWT", async () => {
    const response = await request(server).get("/api/config");

    expect(response.status).to.equal(401);
    expect(response.body).to.have.property(
      "message",
      "Token de autenticação ausente."
    );
  });

  it("deve lidar com erros ao verificar o token JWT", async () => {
    const verifyStub = sinon.stub(jwt, "verify");
    verifyStub.callsArgWith(1, new Error("Erro ao verificar o token"));

    const token = "token-jwt-valido"; // Um token válido, mas a verificação irá falhar

    const response = await request(server)
      .get("/api/config")
      .set("Authorization", `${token}`);

    verifyStub.restore();

    expect(response.status).to.equal(500);
    expect(response.body).to.have.property(
      "message",
      "Erro ao verificar o token de autenticação."
    );
  });
});
