# Simples backend para gerenciar processo de ETL

## Contexto

O processo de ETL (Extract, Transform and Load) é um processo de extração, transformação e carga de dados. O processo de ETL é um processo fundamental para a maioria dos projetos de Business Intelligence (BI) e Data Warehousing (DW).

Considerando que o processo de ETL está dockerizado e que existe a necessidade de gerenciar o processo de ETL, foi desenvolvido uma aplicação que inclui um backend e um frontend para gerenciar o processo de ETL.

## Tecnologias utilizadas

### Backend

#### NodeJS

Node.js é um interpretador de código JavaScript com o código aberto, focado em migrar o Javascript do lado do cliente para servidores. O Node.js usa um modelo de I/O direcionada a evento não bloqueante que o torna leve e eficiente, ideal para aplicações em tempo real com troca intensa de dados através de dispositivos distribuídos.

### Frontend

#### ReactJS

React é uma biblioteca JavaScript de código aberto com foco em criar interfaces de usuário em páginas web. É mantido pelo Facebook, Instagram, outras empresas e uma comunidade de desenvolvedores individuais.

### Funcionalidades

- Autenticação
- Gerenciar Configurações
- Visualizar Logs

#### Autenticação

Para acessar o frontend é necessário informar um usuário e um token. O usuário e o token são definidos no backend.

#### Gerenciar Configurações

O usuário pode criar, editar e excluir configurações. Uma configuração é composta por um conjunto de parâmetros que são utilizados para executar o processo de ETL. Para maiores detalhes sobre os parâmetros, consulte o projeto [etl](../etl/README.md).

#### Visualizar Logs

O usuário pode visualizar os logs do processo de ETL. Os logs são exibidos em tempo real.


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

Os comandos acima executam o backend-config-app em modo de desenvolvimento, definem o usuário e token para autenticação e instalam as dependências do projeto.

Executando testes unitários:

```bash
cd backend-config-app
npx mocha
```

Os comandos acima executam os testes unitários do backend-config-app.

## Iniciando o projeto frontend-config-app

Para iniciar o projeto frontend-config-app é necessário ter o backend-config-app rodando.

```bash
cd frontend-config-app
npm install
npm start
```

Os comandos acima instalam as dependências do projeto e iniciam o frontend-config-app. Accessar o frontend-config-app em http://localhost:3000.

Faça login com o usuário e token definidos no backend-config-app.

Executando testes unitários:

```bash
cd frontend-config-app
npm run test
```

Os comandos acima executam os testes unitários do frontend-config-app.