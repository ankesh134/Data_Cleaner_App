import { useState } from 'react';

function UploadZone() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [analysis, setAnalysis] = useState(null);
  const [error, setError] = useState(null);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      setFile(selectedFile);
      setAnalysis(null);
      setError(null);
    }
  };

  const analyzeFile = async () => {
    if (!file) return;

    setLoading(true);
    setError(null);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('http://localhost:8000/api/analyze', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error('Failed to analyze file');
      }

      const data = await response.json();
      setAnalysis(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const cleanFile = async () => {
    if (!file) return;

    setLoading(true);
    setError(null);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('http://localhost:8000/api/clean', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error('Failed to clean file');
      }

      // Download the cleaned file
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `cleaned_${file.name}`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      alert('File cleaned and downloaded!');
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-lg p-8 mt-8">
      {/* Upload Zone */}
      <div className="border-2 border-dashed border-gray-300 rounded-lg p-12 text-center hover:border-blue-500 transition">
        <input
          type="file"
          accept=".csv"
          onChange={handleFileChange}
          className="hidden"
          id="file-upload"
        />
        <label htmlFor="file-upload" className="cursor-pointer">
          <div className="text-6xl mb-4">📁</div>
          <p className="text-lg text-gray-700 mb-2">
            Click to upload CSV file
          </p>
          <p className="text-sm text-gray-500">Maximum file size: 10MB</p>
        </label>
      </div>

      {/* File Info */}
      {file && (
        <div className="mt-6 p-4 bg-blue-50 rounded-lg">
          <p className="text-sm text-gray-700">
            Selected: <span className="font-semibold">{file.name}</span>
          </p>
          <p className="text-xs text-gray-500 mt-1">
            Size: {(file.size / 1024).toFixed(2)} KB
          </p>
        </div>
      )}

      {/* Error Message */}
      {error && (
        <div className="mt-6 p-4 bg-red-50 border border-red-200 rounded-lg">
          <p className="text-sm text-red-600">Error: {error}</p>
        </div>
      )}

      {/* Buttons */}
      <div className="mt-6 flex gap-4">
        <button
          onClick={analyzeFile}
          disabled={!file || loading}
          className={`flex-1 py-3 rounded-lg font-semibold transition ${
            file && !loading
              ? 'bg-gray-600 hover:bg-gray-700 text-white cursor-pointer'
              : 'bg-gray-300 text-gray-500 cursor-not-allowed'
          }`}
        >
          {loading ? 'Analyzing...' : 'Analyze First'}
        </button>
        <button
          onClick={cleanFile}
          disabled={!file || loading}
          className={`flex-1 py-3 rounded-lg font-semibold transition ${
            file && !loading
              ? 'bg-blue-600 hover:bg-blue-700 text-white cursor-pointer'
              : 'bg-gray-300 text-gray-500 cursor-not-allowed'
          }`}
        >
          {loading ? 'Cleaning...' : 'Clean My Data'}
        </button>
      </div>

      {/* Analysis Results */}
      {analysis && (
        <div className="mt-8 p-6 bg-gray-50 rounded-lg">
          <h3 className="text-lg font-bold mb-4">Data Analysis Report</h3>
          
          <div className="grid grid-cols-2 gap-4 mb-4">
            <div className="bg-white p-4 rounded">
              <p className="text-sm text-gray-500">Total Rows</p>
              <p className="text-2xl font-bold text-blue-600">{analysis.rows}</p>
            </div>
            <div className="bg-white p-4 rounded">
              <p className="text-sm text-gray-500">Total Columns</p>
              <p className="text-2xl font-bold text-blue-600">{analysis.columns}</p>
            </div>
          </div>

          {analysis.duplicates > 0 && (
            <div className="mb-4 p-3 bg-yellow-50 border border-yellow-200 rounded">
              <p className="text-sm text-yellow-800">
                ⚠️ Found {analysis.duplicates} duplicate rows
              </p>
            </div>
          )}

          {Object.keys(analysis.missing_values).length > 0 && (
            <div className="mb-4">
              <p className="text-sm font-semibold mb-2">Missing Values:</p>
              {Object.entries(analysis.missing_values).map(([col, info]) => (
                <div key={col} className="text-sm text-gray-700 mb-1">
                  <span className="font-medium">{col}:</span> {info.count} ({info.percentage}%)
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default UploadZone;