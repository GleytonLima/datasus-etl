import { render, screen } from "@testing-library/react";
import App from "./App";

test("renders do login", () => {
  render(<App />);
  const linkElement = screen.getByText(/Gerenciar Configuração de ETL/i);
  expect(linkElement).toBeInTheDocument();
});
