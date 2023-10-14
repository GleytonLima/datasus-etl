const request = require("supertest");
const { expect } = require("chai");
const api = require("../src/app.js");

describe("API Endpoints", () => {
  let server;
  before(() => {
    process.env.USER = "seu-username";
    process.env.TOKEN = "seu-password";
    server = api.app;
  });

  after(() => {
    api.shutdown();
  });

  it("deve responder a solicitações POST para /api/login", async () => {
    const response = await request(server)
      .post("/api/login")
      .send({ username: "seu-username", password: "seu-password" });

    expect(response.status).to.equal(200);
    expect(response.body).to.have.property("token");
  });

  it("deve responder a solicitações POST para /api/login com  401", async () => {
    const response = await request(server)
      .post("/api/login")
      .send({ username: "xpto", password: "wrong" });

    expect(response.status).to.equal(401);
  });

  it("deve responder a solicitações GET para /api/config", async () => {
    const responseAuth = await request(server)
      .post("/api/login")
      .send({ username: "seu-username", password: "seu-password" });

    const response = await request(server)
      .get("/api/config")
      .set("Authorization", responseAuth.body.token);

    expect(response.status).to.equal(200);
    expect(response.body).to.be.an("object");
  });

  it("deve responder a solicitações GET para /api/logs", async () => {
    const responseAuth = await request(server)
      .post("/api/login")
      .send({ username: "seu-username", password: "seu-password" });

    const response = await request(server)
      .get("/api/logs")
      .set("Authorization", responseAuth.body.token);

    expect(response.status).to.equal(200);
    expect(response.body).to.be.an("array");
  });

  it("deve responder a solicitações PUT para /api/config", async () => {
    const responseAuth = await request(server)
      .post("/api/login")
      .send({ username: "seu-username", password: "seu-password" });

    const requestData = {
      anos: [2022, 2023],
      ufs: ["SP", "RJ"],
      meses: ["Janeiro", "Fevereiro"],
    };

    const response = await request(server)
      .put("/api/config")
      .set("Authorization", responseAuth.body.token)
      .send(requestData);

    expect(response.status).to.equal(200);
    expect(response.body).to.have.property(
      "message",
      "Dados atualizados com sucesso."
    );
  });
});
