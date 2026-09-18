import { createRoot } from 'react-dom/client';
import { RouterProvider } from 'react-router/dom';
import { router } from './app/router';
import { initializeBrowser } from './shell/bootstrap';
import './styles/entry.css';
import { installNavigation } from './app/navigation';

initializeBrowser();
installNavigation();
const root = document.getElementById('tree-root');
if (!root) throw new Error('Missing application root');
createRoot(root).render(<RouterProvider router={router} />);
