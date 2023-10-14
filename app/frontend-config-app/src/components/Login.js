import axios from "axios";
import "bootstrap/dist/css/bootstrap.min.css";
import React, { useState } from "react";
import { Button, Container, Form } from "react-bootstrap";

function Login({ setToken }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();

    try {
      const response = await axios.post(
        `${"http://localhost:3001"}/api/login`,
        { username, password }
      );
      const { token } = response.data;
      localStorage.setItem("token", token);
      setToken(token);
    } catch (error) {
      console.error("Erro de login:", error);
      alert("Erro de login");
    }
  };

  return (
    <Container>
      <h2>Gerenciar Configuração de ETL</h2>
      <h3>Login</h3>
      <Form onSubmit={handleSubmit}>
        <Form.Group controlId="username">
          <Form.Label>Nome de usuário</Form.Label>
          <Form.Control
            type="text"
            placeholder="Digite seu nome de usuário"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />
        </Form.Group>

        <Form.Group controlId="password">
          <Form.Label>Senha</Form.Label>
          <Form.Control
            type="password"
            placeholder="Digite sua senha"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </Form.Group>

        <br />
        <Button variant="primary" type="submit">
          Login
        </Button>
      </Form>
    </Container>
  );
}

export default Login;
