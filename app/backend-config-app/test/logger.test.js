const { expect } = require("chai");
const sinon = require("sinon");

describe("Logger", () => {
  it("deve registrar mensagens no arquivo em ambiente de desenvolvimento", () => {
    process.env.NODE_ENV = "development";
    delete require.cache[require.resolve("../src/logger")];
    const logger = require("../src/logger");
    consoleLogStub = sinon.stub(logger.transports[1], "log");
    fileLogStub = sinon.stub(logger.transports[0], "log");

    logger.info("mensagem de teste");

    expect(logger.transports.length).to.be.equal(2);
    expect(fileLogStub.called).to.be.true;
    expect(consoleLogStub.called).to.be.true;
    consoleLogStub.restore();
    fileLogStub.restore();
  });

  it("deve registrar mensagens no arquivo em ambiente de produção", () => {
    process.env.NODE_ENV = "production";
    delete require.cache[require.resolve("../src/logger")];
    const logger = require("../src/logger");
    fileLogStub = sinon.stub(logger.transports[0], "log");

    logger.info("mensagem de teste");

    expect(logger.transports.length).to.be.equal(1);
    expect(fileLogStub.called).to.be.true;
    consoleLogStub.restore();
    fileLogStub.restore();
    process.env.NODE_ENV = "";
  });
});
