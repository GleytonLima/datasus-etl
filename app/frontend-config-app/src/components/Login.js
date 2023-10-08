import axios from 'axios';
import React, { useState } from 'react';

function Login({ setToken }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    try {
      const response = await axios.post(`${'http://localhost:3001'}/api/login`, { username, password });
      const { token } = response.data;
      localStorage.setItem('token', token)
      setToken(token);
    } catch (error) {
      console.error('Erro de login:', error);
    }
  };

  return (
    <div>
      <h2>Faça login</h2>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          placeholder="Nome de usuário"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
        />
        <input
          type="password"
          placeholder="Senha"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        <button type="submit">Login</button>
      </form>
    </div>
  );
}

export default Login;
