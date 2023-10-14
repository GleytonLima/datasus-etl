const { expect } = require("chai");
const sinon = require("sinon");
const fs = require("fs");
const util = require("util");
const logger = require("../src/logger");
const {
  executeSequentially,
  readCronFromJSON,
  updateCronTask,
  etlFunction,
} = require("../src/cron-task");

describe("Backend Testes", () => {
  describe("readCronFromJSON", () => {
    it("deve ler a expressão cron do arquivo JSON", () => {
      const configJson = '{"agendamento_cron": "*/5 * * * *"}';
      sinon.stub(fs, "readFileSync").returns(configJson);
      const cron = readCronFromJSON();
      expect(cron).to.equal("*/5 * * * *");
      fs.readFileSync.restore();
    });

    it("deve lidar com erros ao ler o arquivo JSON", () => {
      sinon
        .stub(fs, "readFileSync")
        .throws(new Error("Erro ao ler o arquivo JSON"));
      expect(readCronFromJSON).to.throw("Erro ao ler o arquivo JSON");
      fs.readFileSync.restore();
    });
  });

  describe("executeSequentially", () => {
    it("deve executar comandos sequencialmente", async () => {
      const execPromiseStub = sinon.stub().resolves({ stdout: "Resultado" });
      sinon.stub(fs, "readdirSync").returns(["file1.yaml", "file2.yaml"]);
      sinon.stub(util, "promisify").returns(execPromiseStub);
      const commands = ["command1", "command2"];
      await executeSequentially(commands);
      expect(execPromiseStub.calledTwice).to.be.true;
      util.promisify.restore();
      fs.readdirSync.restore();
    });

    it("deve lidar com erros ao executar comandos", async () => {
      const execPromiseStub = sinon
        .stub()
        .rejects(new Error("Erro ao executar comando"));
      sinon.stub(util, "promisify").returns(execPromiseStub);
      const commands = ["command1", "command2"];
      try {
        await executeSequentially(commands);
      } catch (error) {
        expect(error.message).to.include("Erro ao executar o comando");
      }
      util.promisify.restore();
    });
  });
});

describe("updateCronTask", () => {
  let task;

  beforeEach(() => {
    task = { stop: sinon.stub() };
  });

  it("deve atualizar a tarefa cron com base na expressão cron do arquivo JSON", () => {
    const readCronFromJSONStub = sinon.stub();
    readCronFromJSONStub.returns("*/5 * * * *"); // Simula a leitura de uma expressão cron válida do arquivo JSON
    const fs = require("fs");
    sinon
      .stub(fs, "readFileSync")
      .returns('{"agendamento_cron": "*/5 * * * *"}');

    // Substitui o módulo cron com o stub que criamos
    const cronModule = require("node-cron");
    sinon.stub(cronModule, "schedule").callsFake((cron, callback) => {
      task.callback = callback;
      return task;
    });

    updateCronTask();

    expect(task.stop.calledOnce).to.be.false;
    expect(cronModule.schedule.calledOnce).to.be.true;
    expect(task.callback).to.be.a("function");

    // Restaurar os stubs
    cronModule.schedule.restore();
    fs.readFileSync.restore();
  });

  it("deve cancelar a tarefa cron existente se houver", () => {
    const readCronFromJSONStub = sinon.stub();
    readCronFromJSONStub.returns("*/5 * * * *"); // Simula a leitura de uma expressão cron válida do arquivo JSON
    const fs = require("fs");
    sinon
      .stub(fs, "readFileSync")
      .returns('{"agendamento_cron": "*/5 * * * *"}');

    // Simular uma tarefa cron existente
    task.stop.returns(true);

    // Substitui o módulo cron com o stub que criamos
    const cronModule = require("node-cron");
    sinon.stub(cronModule, "schedule").returns(task);

    updateCronTask();

    expect(cronModule.schedule.calledOnce).to.be.true;
    expect(task.stop.calledOnce).to.be.false;

    // Restaurar os stubs
    cronModule.schedule.restore();
    fs.readFileSync.restore();
  });
});

describe("etlFunction", () => {
  let loggerInfoStub;
  let loggerErrorStub;

  beforeEach(() => {
    loggerInfoStub = sinon.stub(logger, "info");
    loggerErrorStub = sinon.stub(logger, "error");
  });

  afterEach(() => {
    loggerInfoStub.restore();
    loggerErrorStub.restore();
  });

  it("deve executar os comandos sequencialmente e registrar informações de sucesso", async () => {
    const fs = require("fs");
    const cronTask = require("../src/cron-task");
    sinon.stub(fs, "readdirSync").returns(["file1.yaml", "file2.yaml"]);
    sinon.stub(cronTask, "executeSequentially").resolves();

    await etlFunction();

    expect(fs.readdirSync.calledOnce).to.be.true;
    expect(cronTask.executeSequentially.calledOnce).to.be.true;
    expect(loggerInfoStub.calledWith("executando scripts de extração...")).to.be
      .true;
    expect(
      loggerInfoStub.calledWith(
        sinon.match("scripts de extração executados com sucesso")
      )
    ).to.be.true;
    expect(loggerErrorStub.called).to.be.false;
    fs.readdirSync.restore();
    cronTask.executeSequentially.restore();
  });

  it("deve lidar com erros durante a execução e registrar informações de erro", async () => {
    const error = new Error("Erro durante a execução");
    sinon.stub(fs, "readdirSync").returns(["file1.yaml", "file2.yaml"]);
    const cronTask = require("../src/cron-task");
    sinon.stub(cronTask, "executeSequentially").rejects(error);

    await etlFunction();

    expect(fs.readdirSync.calledOnce).to.be.true;
    expect(cronTask.executeSequentially.calledOnce).to.be.true;
    expect(loggerInfoStub.calledWith("executando scripts de extração...")).to.be
      .true;
    expect(
      loggerErrorStub.calledWith(
        sinon.match("scripts de extração executados com erro")
      )
    ).to.be.true;
  });
});
