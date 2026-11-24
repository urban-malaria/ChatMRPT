// Toolbar styles and the research-study buttons (Survey, Pre/Post Test) live in app/static and are
// bundled here so each build gets new file names; CloudFront caches unversioned /static files for a year.
import '../../app/static/css/toolbar.css';
import '../../app/static/js/survey_button.js';
import '../../app/static/js/prepost_button.js';
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './styles/index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
