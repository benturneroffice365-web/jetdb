12. frontend/src/App.tsx - COMPLETE REWRITE for v8.0
tsximport React, { useState, useEffect, useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import { createClient } from '@supabase/supabase-js';
import axios from 'axios';
import toast, { Toaster } from 'react-hot-toast';
import { DataGrid } from './components/DataGrid';
import { QueryWorkspace } from './components/QueryWorkspace';
import { MergeModal } from './components/MergeModal';
import './App.css';

const supabase = createClient(
  process.env.REACT_APP_SUPABASE_URL || '',
  process.env.REACT_APP_SUPABASE_KEY || ''
);

const API_BASE = process.env.REACT_APP_API_BASE || 'http://localhost:8000';

interface Dataset {
  id: string;
  filename: string;
  row_count: number;
  column_count: number;
  columns: string[];
  status: string;
  storage_format?: string;
}

const App: React.FC = () => {
  const [session, setSession] = useState<any>(null);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [isSignup, setIsSignup] = useState(false);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [selectedForMerge, setSelectedForMerge] = useState<string[]>([]);
  const [uploading, setUploading] = useState(false);
  const [showMergeModal, setShowMergeModal] = useState(false);
  const [view, setView] = useState<'grid' | 'query'>('grid');

  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => setSession(session));
    supabase.auth.onAuthStateChange((_event, session) => setSession(session));
  }, []);

  const getAuthHeaders = useCallback(() => ({
    'Authorization': `Bearer ${session?.access_token}`
  }), [session]);

  const fetchDatasets = async () => {
    if (!session) return;
    try {
      const { data } = await axios.get(`${API_BASE}/datasets`, {
        headers: getAuthHeaders()
      });
      setDatasets(data.datasets || []);
    } catch (error) {
      console.error('Failed to fetch datasets:', error);
    }
  };

  useEffect(() => {
    if (session) fetchDatasets();
  }, [session]);

  const handleAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      if (isSignup) {
        await supabase.auth.signUp({ email, password });
        toast.success('Check your email!');
      } else {
        await supabase.auth.signInWithPassword({ email, password });
        toast.success('Welcome back!');
      }
    } catch (error: any) {
      toast.error(error.message);
    }
  };

  const onDrop = useCallback(async (files: File[]) => {
    const file = files[0];
    if (!file || !session) return;

    setUploading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      const { data } = await axios.post(`${API_BASE}/upload`, formData, {
        headers: { ...getAuthHeaders(), 'Content-Type': 'multipart/form-data' }
      });
      toast.success(`Uploaded ${file.name}!`);
      fetchDatasets();
      setSelectedDataset(data.dataset_id);
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Upload failed');
    } finally {
      setUploading(false);
    }
  }, [session]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'text/csv': ['.csv'] },
    disabled: uploading || !session
  });

  const currentDataset = datasets.find(d => d.id === selectedDataset);

  if (!session) {
    return (
      <div className="auth-container">
        <Toaster />
        <div className="auth-box">
          <h1 className="logo">âš¡ JetDB</h1>
          <p style={{ marginBottom: '24px', color: '#a1a1aa' }}>v8.0 â€¢ Investor-Ready MVP</p>
          <form onSubmit={handleAuth} style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            <input type="email" placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} required className="auth-input" />
            <input type="password" placeholder="Password" value={password} onChange={(e) => setPassword(e.target.value)} required className="auth-input" />
            <button type="submit" className="auth-button">{isSignup ? 'Sign Up' : 'Sign In'}</button>
          </form>
          <p style={{ marginTop: '16px', fontSize: '14px', color: '#a1a1aa' }}>
            {isSignup ? 'Have an account?' : "Don't have an account?"}
            <button onClick={() => setIsSignup(!isSignup)} style={{ background: 'none', border: 'none', color: '#6366f1', marginLeft: '6px', cursor: 'pointer', fontWeight: 600 }}>
              {isSignup ? 'Sign In' : 'Sign Up'}
            </button>
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <Toaster />
      
      <header className="header">
        <div className="header-left">
          <h1 className="logo">âš¡ JetDB</h1>
          <span className="version">v8.0</span>
          {currentDataset && (
            <span className="current-file">
              {currentDataset.filename}
              <span className="row-count">{currentDataset.row_count?.toLocaleString()} rows</span>
            </span>
          )}
        </div>
        <div className="header-right">
          {selectedDataset && (
            <>
              <button onClick={() => setView(view === 'grid' ? 'query' : 'grid')} className="btn-secondary">
                {view === 'grid' ? 'ðŸ¤– Query' : 'ðŸ“Š Grid'}
              </button>
              <button onClick={() => { setSelectedForMerge(datasets.map(d => d.id)); setShowMergeModal(true); }} className="btn-primary" disabled={datasets.length < 2}>
                ðŸ”„ Merge ({datasets.length})
              </button>
            </>
          )}
          <button onClick={() => supabase.auth.signOut()} className="btn-secondary">Sign Out</button>
        </div>
      </header>

      <div className="main-content">
        {!selectedDataset ? (
          <div {...getRootProps()} className={`dropzone ${isDragActive ? 'dropzone-active' : ''}`}>
            <input {...getInputProps()} />
            {uploading ? (
              <div className="upload-progress">
                <div className="spinner"></div>
                <p>Uploading...</p>
              </div>
            ) : (
              <>
                <div className="dropzone-icon">ðŸ“Š</div>
                <h2>Drop CSV here</h2>
                <p>Up to 10GB â€¢ Automatic Parquet conversion</p>
              </>
            )}
          </div>
        ) : view === 'grid' ? (
          <DataGrid
            datasetId={selectedDataset}
            totalRows={currentDataset?.row_count || 0}
            apiBase={API_BASE}
            authHeaders={getAuthHeaders()}
          />
        ) : (
          <QueryWorkspace
            datasetId={selectedDataset}
            apiBase={API_BASE}
            authHeaders={getAuthHeaders()}
          />
        )}
      </div>

      {showMergeModal && (
        <MergeModal
          datasets={datasets}
          selectedIds={selectedForMerge}
          onClose={() => setShowMergeModal(false)}
          onComplete={() => {
            setShowMergeModal(false);
            fetchDatasets();
          }}
          apiBase={API_BASE}
          authHeaders={getAuthHeaders()}
        />
      )}
    </div>
  );
};

export default App;