import './App.css';
import UploadZone from './components/UploadZone';

function App() {
  return (
    <div className="container">
      <div className="header">
        <h1>🧹 Data Cleaner</h1>
        <p>Upload your messy CSV, get it cleaned instantly</p>
      </div>
      
      <div className="card">
        <UploadZone />
      </div>
    </div>
  );
}

export default App;