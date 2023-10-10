import { render, screen } from '@testing-library/react';
import App from './App';

test('renders do login', () => {
  render(<App />);
  const linkElement = screen.getByText(/login/i);
  expect(linkElement).toBeInTheDocument();
});
