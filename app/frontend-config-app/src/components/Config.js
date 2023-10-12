import axios from 'axios';
import 'bootstrap/dist/css/bootstrap.min.css';
import { JsonEditor as Editor } from 'jsoneditor-react';
import React, { useEffect, useState } from 'react';
import { Button, Col, Container, Row } from 'react-bootstrap';
import { Link } from 'react-router-dom';

function Config({ token }) {
  const [configData, setConfigData] = useState({});
  const [newData, setNewData] = useState('');
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axios.get(`${'http://localhost:3001'}/api/config`, {
          headers: { Authorization: token },
        });
        console.log(response.data);
        setConfigData(response.data);
        setIsLoading(false);
      } catch (error) {
        if (error.response.status === 401) {
          console.error('Token de autenticação inválido.');
          localStorage.removeItem('token');
          window.location.reload();
        } else {
          console.error('Erro ao carregar os dados:', error);
        }
      }
    };

    fetchData();
  }, [token]);

  const handleUpdate = async () => {
    try {
      const response = await axios.put(
        `${'http://localhost:3001'}/api/config`,
        { ...configData, ...newData },
        { headers: { Authorization: token } }
      );
      console.log(response.data.message);
      alert("Atualizado com sucesso");
    } catch (error) {
      alert("Erro ao atualizar, tente novamente");
      console.error('Erro ao atualizar os dados:', error);
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('token'); // Limpa o token da localStorage
    window.location.reload(); // Recarrega a página para que o usuário seja redirecionado para a página de login
  };

  return (
    <Container>
      <br/>
      <Row className="mb-4">
        <Col>
          <h1>Gerenciar Configuração de ETL</h1>
        </Col>
        <Col className="text-end">
          <Link to="/logs">
            <Button variant="info">Logs</Button>
          </Link>
          <span> </span>
          <Button variant="danger" onClick={handleLogout}>Logout</Button>
        </Col>
      </Row>
      {isLoading ? (
        <p>Carregando...</p>
      ) : (
        <Row className="mb-4">
          <Editor
            value={configData}
            onChange={setNewData}
          />
        </Row>
      )}
      <br/>
      <Button variant="primary" onClick={handleUpdate}>Salvar</Button>
    </Container>
  );
}

export default Config;
