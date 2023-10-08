import React, { useEffect, useState } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import './App.css';
import Config from './components/Config';
import Login from './components/Login';

function App() {
  const [token, setToken] = useState('');
  
  useEffect(() => {
    const storedToken = localStorage.getItem('token');
    if (storedToken) {
      setToken(storedToken);
    }
  }, []);

  return (
    <Routes>
        <Route path="/" exact element=<Navigate to="/login" /> />
        <Route
          path="/login"
          element={token ? <Navigate to="/config" /> : <Login setToken={setToken} />}
        />
        <Route
          path="/config"
          element={token ? <Config token={token} /> : <Navigate to="/login" />}
        />
    </Routes>
  );
}

export default App;
