import axios from 'axios';
import 'bootstrap/dist/css/bootstrap.min.css';
import React, { useEffect, useState } from 'react';


function Config({ token }) {
  const [configData, setConfigData] = useState({});
  const [newData, setNewData] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axios.get(`${'http://localhost:3001'}/api/config`, {
          headers: { Authorization: token },
        });
        setConfigData(response.data);
      } catch (error) {
        console.error('Erro ao carregar os dados:', error);
      }
    };

    fetchData();
  }, [token]);

  const handleUpdate = async () => {
    try {
      const response = await axios.put(
        `${'http://localhost:3001'}/api/config`,
        { anos: [...configData.anos, newData] },
        { headers: { Authorization: token } }
      );
      console.log(response.data.message);
    } catch (error) {
      console.error('Erro ao atualizar os dados:', error);
    }
  };

  return (
    <div className="container mt-4">
      <h2>Configurações</h2>
      <div className="card mt-3">
        <div className="card-body">
          <h3 className="card-title">Lista de anos:</h3>
          <ul className="list-group">
            {configData.anos &&
              configData.anos.map((ano, i) => (
                <li key={i} className="list-group-item">
                  {ano}
                </li>
              ))}
          </ul>
          <div className="input-group mt-3">
            <input
              type="text"
              className="form-control"
              placeholder="Novo ano"
              value={newData}
              onChange={(e) => setNewData(e.target.value)}
            />
            <div className="input-group-append">
              <button
                className="btn btn-primary"
                onClick={handleUpdate}
              >
                Adicionar ano
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Config;
