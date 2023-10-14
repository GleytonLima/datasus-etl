# Simples backend para gerenciar processo de ETL

## Iniciando o projeto backend-config-app

Para iniciar o projeto backend-config-app execute os comandos abaixo:

```bash
cd backend-config-app
export NODE_ENV=development
export USER=example
export TOKEN=supersecret
npm install
npm start
```

Executando testes unitários:

```bash
cd backend-config-app
npx mocha
```

## Iniciando o projeto frontend-config-app

Para iniciar o projeto frontend-config-app é necessário ter o backend-config-app rodando.

```bash
cd frontend-config-app
npm install
npm start
```

Executando testes unitários:

```bash
cd frontend-config-app
npm run test
```
