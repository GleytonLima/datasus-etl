const cron = require("node-cron");
const { exec } = require("child_process");
const { promisify } = require("util");
const fs = require("fs");
const logger = require("./logger");
const { configFilePath } = require("./config");

let task;
// Função para ler a expressão cron do arquivo JSON
function readCronFromJSON() {
  const configJson = fs.readFileSync(configFilePath, "utf8");
  const config = JSON.parse(configJson);
  return config.agendamento_cron;
}

// Função para executar comandos sequencialmente
async function executeSequentially(commands) {
  for (const command of commands) {
    logger.info(`executando o comando: ${command}`);
    const execPromise = promisify(exec);
    try {
      const { stdout } = await execPromise(command);
      logger.info(`comando executado com sucesso: ${command}: ${stdout}`);
    } catch (error) {
      logger.error(`Erro ao executar o comando ${command}: ${error}`);
    }
  }
}

// Função para atualizar a tarefa cron com base na expressão cron do arquivo JSON
async function updateCronTask() {
  const agendamentoCron = readCronFromJSON();
  logger.info(`expressão cron atualizada: ${agendamentoCron}`);

  // Cancele a tarefa cron existente (se houver)
  if (task) {
    task.stop();
  }

  // Crie uma nova tarefa cron com a expressão cron atualizada
  task = cron.schedule(agendamentoCron, async () => {
    logger.info("executando scripts de extração...");

    // obtem a lista de arquivos .yaml dentro da pasta docker-compose-files
    const list_of_files = fs
      .readdirSync("./docker-compose-files")
      .filter((file) => file.endsWith(".yaml"));

    const list_of_commands = list_of_files.map(
      (file) => `docker-compose -f ./docker-compose-files/${file} up`
    );

    // Execute comandos sequencialmente
    await executeSequentially(list_of_commands);
  });
}

module.exports = updateCronTask;
