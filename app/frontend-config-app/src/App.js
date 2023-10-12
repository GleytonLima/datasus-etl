import React, { useEffect, useState } from 'react';
import { Navigate, Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import './App.css';
import Config from './components/Config';
import Login from './components/Login';
import Logs from './components/Logs';

function App() {
  const [token, setToken] = useState('');
  
  useEffect(() => {
    const storedToken = localStorage.getItem('token');
    if (storedToken) {
      setToken(storedToken);
    }
  }, []);

  return (
    <Router>
      <Routes>
        <Route path="/" element={<Navigate to="/login" />} />
        <Route
          path="/login"
          element={token ? <Navigate to="/config" /> : <Login setToken={setToken} />}
        />
        <Route
          path="/config"
          element={token ? <Config token={token} /> : <Navigate to="/login" />}
        />
        <Route
          path="/logs"
          element={token ? <Logs token={token} /> : <Navigate to="/login" />}
        />
      </Routes>
    </Router>
  );
}

export default App;
