import axios from 'axios';
import 'bootstrap/dist/css/bootstrap.min.css';
import React, { useEffect, useState } from 'react';
import { Button, Col, Container, Row } from 'react-bootstrap';
import Pagination from 'react-bootstrap/Pagination';
import Table from 'react-bootstrap/Table';
import { Link } from 'react-router-dom';

function LogViewer({ token }) {
  const [logs, setLogs] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);

  useEffect(() => {
    fetchLogs();

  }, [currentPage]);

  const fetchLogs = () => {
    axios.get(`${'http://localhost:3001'}/api/logs?page=${currentPage}`, {
      headers: { Authorization: token },
    })
      .then((response) => {
        // converte cada string em json
        const logsParseados = response.data.map((log) => {
          try { 
            return JSON.parse(log) 
          } catch (e) { 
            return log 
          }
        });
        setLogs(logsParseados);
      })
      .catch((error) => {
        console.error('Erro ao buscar registros de log', error);
      });
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
          <Link to="/config">
            <Button variant="info">Configuração</Button>
          </Link>
          <span> </span>
          <Button variant="danger" onClick={handleLogout}>Logout</Button>
        </Col>
      </Row>
      <h1>Registros de Log</h1>
      <Table striped bordered hover>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Nível</th>
            <th>Mensagem</th>
          </tr>
        </thead>
        <tbody>
          {logs.map((log, index) => (
            <tr key={index}>
              <td>{log.timestamp}</td>
              <td>{log.level}</td>
              <td>{log?.message ?? `${JSON.stringify(log)}` }</td>
            </tr>
          ))}
        </tbody>
      </Table>
      <Pagination>
        <Pagination.Prev
          onClick={() => setCurrentPage(currentPage - 1)}
          disabled={currentPage === 1}
        />
        <Pagination.Next onClick={() => setCurrentPage(currentPage + 1)} />
      </Pagination>
    </Container>
  );
}

export default LogViewer;
