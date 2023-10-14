module.exports = {
  configFilePath: "./scripts/extract/config.json",
  logFilePath: "./combined.log",
  secretKey: process.env.SECRET_KEY || "secretpassword",
};
