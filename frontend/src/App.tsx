import React, { useState, useEffect, useCallback } from 'react';
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
  created_at: string;
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
  const [showDatasetList, setShowDatasetList] = useState(false);

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
    if (session) {
      fetchDatasets();
      // Poll for status updates every 3 seconds
      const interval = setInterval(fetchDatasets, 3000);
      return () => clearInterval(interval);
    }
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
      toast.success(`Uploaded ${file.name}! Processing...`);
      fetchDatasets();
      
      // Auto-select the uploaded dataset after a short delay
      setTimeout(() => {
        setSelectedDataset(data.dataset_id);
      }, 1000);
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Upload failed');
    } finally {
      setUploading(false);
    }
  }, [session, getAuthHeaders]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'text/csv': ['.csv'] },
    disabled: uploading || !session,
    multiple: false
  });

  const handleDatasetSelect = (datasetId: string) => {
    setSelectedDataset(datasetId);
    setShowDatasetList(false);
    setView('grid');
  };

  const handleMergeClick = () => {
    const readyDatasets = datasets.filter(d => d.status === 'ready');
    if (readyDatasets.length < 2) {
      toast.error('Need at least 2 ready datasets to merge');
      return;
    }
    setSelectedForMerge(readyDatasets.map(d => d.id));
    setShowMergeModal(true);
  };

  const currentDataset = datasets.find(d => d.id === selectedDataset);

  // Auth Screen
  if (!session) {
    return (
      <div className="auth-container">
        <Toaster position="top-right" />
        <div className="auth-box">
          <h1 className="logo">⚡ JetDB</h1>
          <p style={{ marginBottom: '24px', color: '#a1a1aa', textAlign: 'center' }}>
            v8.0 • Big Data Spreadsheet
          </p>
          <form onSubmit={handleAuth} style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            <input 
              type="email" 
              placeholder="Email" 
              value={email} 
              onChange={(e) => setEmail(e.target.value)} 
              required 
              className="auth-input" 
            />
            <input 
              type="password" 
              placeholder="Password" 
              value={password} 
              onChange={(e) => setPassword(e.target.value)} 
              required 
              className="auth-input" 
            />
            <button type="submit" className="auth-button">
              {isSignup ? 'Sign Up' : 'Sign In'}
            </button>
          </form>
          <p style={{ marginTop: '16px', fontSize: '14px', color: '#a1a1aa', textAlign: 'center' }}>
            {isSignup ? 'Have an account?' : "Don't have an account?"}
            <button 
              onClick={() => setIsSignup(!isSignup)} 
              style={{ 
                background: 'none', 
                border: 'none', 
                color: '#6366f1', 
                marginLeft: '6px', 
                cursor: 'pointer', 
                fontWeight: 600 
              }}
            >
              {isSignup ? 'Sign In' : 'Sign Up'}
            </button>
          </p>
        </div>
      </div>
    );
  }

  // Main App
  return (
    <div className="app">
      <Toaster position="top-right" />
      
      {/* Header */}
      <header className="header">
        <div className="header-left">
          <h1 className="logo">⚡ JetDB</h1>
          <span className="version">v8.0</span>
          
          {currentDataset && (
            <div className="current-file">
              <span>{currentDataset.filename}</span>
              {currentDataset.row_count > 0 && (
                <span className="row-count">{currentDataset.row_count?.toLocaleString()} rows</span>
              )}
              {currentDataset.status === 'processing' && (
                <span style={{ color: '#fbbf24', fontSize: '12px' }}>⏳ Processing...</span>
              )}
            </div>
          )}
        </div>
        
        <div className="header-right">
          {/* Dataset List Button */}
          <button 
            onClick={() => setShowDatasetList(!showDatasetList)} 
            className="btn-secondary"
          >
            📊 Datasets ({datasets.length})
          </button>

          {/* View Toggle (only show when dataset selected) */}
          {selectedDataset && currentDataset?.status === 'ready' && (
            <>
              <button 
                onClick={() => setView(view === 'grid' ? 'query' : 'grid')} 
                className="btn-secondary"
              >
                {view === 'grid' ? '🤖 Query' : '📊 Grid'}
              </button>
              
              <button 
                onClick={handleMergeClick} 
                className="btn-primary" 
                disabled={datasets.filter(d => d.status === 'ready').length < 2}
              >
                🔄 Merge ({datasets.filter(d => d.status === 'ready').length})
              </button>
            </>
          )}
          
          <button onClick={() => supabase.auth.signOut()} className="btn-secondary">
            Sign Out
          </button>
        </div>
      </header>

      {/* Dataset List Dropdown */}
      {showDatasetList && (
        <div style={{
          position: 'absolute',
          top: '70px',
          right: '24px',
          background: 'rgba(26, 26, 36, 0.95)',
          backdropFilter: 'blur(10px)',
          border: '1px solid #2d2d44',
          borderRadius: '12px',
          padding: '12px',
          minWidth: '300px',
          maxHeight: '400px',
          overflowY: 'auto',
          zIndex: 1000,
          boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)'
        }}>
          <div style={{ 
            display: 'flex', 
            justifyContent: 'space-between', 
            alignItems: 'center', 
            marginBottom: '12px',
            paddingBottom: '12px',
            borderBottom: '1px solid #2d2d44'
          }}>
            <h3 style={{ margin: 0, fontSize: '14px', color: '#e4e4e7', fontWeight: 600 }}>
              Your Datasets
            </h3>
            <button 
              onClick={() => setShowDatasetList(false)}
              style={{ 
                background: 'none', 
                border: 'none', 
                color: '#a1a1aa', 
                cursor: 'pointer',
                fontSize: '20px'
              }}
            >
              ×
            </button>
          </div>
          
          {datasets.length === 0 ? (
            <div style={{ padding: '20px', textAlign: 'center', color: '#a1a1aa', fontSize: '13px' }}>
              No datasets yet. Upload a CSV to get started!
            </div>
          ) : (
            datasets.map(ds => (
              <div
                key={ds.id}
                onClick={() => ds.status === 'ready' && handleDatasetSelect(ds.id)}
                style={{
                  padding: '12px',
                  marginBottom: '8px',
                  background: selectedDataset === ds.id ? 'rgba(99, 102, 241, 0.2)' : 'rgba(45, 45, 68, 0.5)',
                  border: `1px solid ${selectedDataset === ds.id ? '#6366f1' : '#2d2d44'}`,
                  borderRadius: '8px',
                  cursor: ds.status === 'ready' ? 'pointer' : 'default',
                  opacity: ds.status === 'ready' ? 1 : 0.6,
                  transition: 'all 0.2s'
                }}
              >
                <div style={{ 
                  display: 'flex', 
                  justifyContent: 'space-between', 
                  alignItems: 'start',
                  marginBottom: '4px'
                }}>
                  <span style={{ 
                    fontSize: '13px', 
                    fontWeight: 600, 
                    color: '#e4e4e7',
                    wordBreak: 'break-word'
                  }}>
                    {ds.filename}
                  </span>
                  {ds.status === 'processing' && (
                    <span style={{ fontSize: '10px', color: '#fbbf24' }}>⏳</span>
                  )}
                  {ds.status === 'ready' && (
                    <span style={{ fontSize: '10px', color: '#10b981' }}>✓</span>
                  )}
                  {ds.status === 'error' && (
                    <span style={{ fontSize: '10px', color: '#ef4444' }}>✗</span>
                  )}
                </div>
                <div style={{ fontSize: '11px', color: '#a1a1aa' }}>
                  {ds.row_count ? `${ds.row_count.toLocaleString()} rows` : 'Processing...'}
                  {ds.column_count && ` • ${ds.column_count} columns`}
                </div>
              </div>
            ))
          )}
        </div>
      )}

      {/* Main Content */}
      <div className="main-content">
        {!selectedDataset ? (
          // Upload Dropzone
          <div {...getRootProps()} className={`dropzone ${isDragActive ? 'dropzone-active' : ''}`}>
            <input {...getInputProps()} />
            {uploading ? (
              <div className="upload-progress">
                <div className="spinner"></div>
                <p>Uploading and processing...</p>
              </div>
            ) : (
              <>
                <div className="dropzone-icon">📊</div>
                <h2>Drop CSV here or click to browse</h2>
                <p>Up to 10GB • Automatic analysis and conversion</p>
              </>
            )}
          </div>
        ) : currentDataset?.status === 'processing' ? (
          // Processing State
          <div style={{ 
            display: 'flex', 
            flexDirection: 'column', 
            alignItems: 'center', 
            justifyContent: 'center', 
            height: '100%',
            gap: '16px'
          }}>
            <div className="spinner"></div>
            <h3 style={{ color: '#e4e4e7' }}>Processing {currentDataset.filename}...</h3>
            <p style={{ color: '#a1a1aa', fontSize: '14px' }}>
              Analyzing columns and counting rows
            </p>
          </div>
        ) : currentDataset?.status === 'error' ? (
          // Error State
          <div style={{ 
            display: 'flex', 
            flexDirection: 'column', 
            alignItems: 'center', 
            justifyContent: 'center', 
            height: '100%',
            gap: '16px'
          }}>
            <div style={{ fontSize: '48px' }}>⚠️</div>
            <h3 style={{ color: '#ef4444' }}>Processing Failed</h3>
            <p style={{ color: '#a1a1aa', fontSize: '14px', maxWidth: '400px', textAlign: 'center' }}>
              {currentDataset.error_message || 'Unknown error'}
            </p>
            <button 
              onClick={() => setSelectedDataset(null)}
              className="btn-primary"
            >
              Upload Different File
            </button>
          </div>
        ) : view === 'grid' ? (
          // Data Grid View
          <DataGrid
            datasetId={selectedDataset}
            totalRows={currentDataset?.row_count || 0}
            apiBase={API_BASE}
            authHeaders={getAuthHeaders()}
          />
        ) : (
          // Query View
          <QueryWorkspace
            datasetId={selectedDataset}
            apiBase={API_BASE}
            authHeaders={getAuthHeaders()}
          />
        )}
      </div>

      {/* Merge Modal */}
      {showMergeModal && (
        <MergeModal
          datasets={datasets.filter(d => d.status === 'ready')}
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