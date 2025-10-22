import React, { useState, useEffect, useCallback, useRef } from 'react';
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
  parquet_path?: string;
  created_at: string;
}

const App: React.FC = () => {
  // Auth state
  const [session, setSession] = useState<any>(null);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [isSignup, setIsSignup] = useState(false);

  // Dataset state
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [datasetCount, setDatasetCount] = useState<number>(0);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [selectedForMerge, setSelectedForMerge] = useState<string[]>([]);
  
  // UI state
  const [uploading, setUploading] = useState(false);
  const [showMergeModal, setShowMergeModal] = useState(false);
  const [showDatasetList, setShowDatasetList] = useState(false);
  const [view, setView] = useState<'grid' | 'query'>('grid');

  // Refs
  const hasInitiallyLoaded = useRef(false);

  // ✅ Auth setup
  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
    });
    
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session);
    });

    return () => subscription.unsubscribe();
  }, []);

  // ✅ Get auth headers
  const getAuthHeaders = useCallback(() => ({
    'Authorization': `Bearer ${session?.access_token}`
  }), [session]);

  // ✅ TASK 2 FIX: Improved fetchDatasets with better error handling
  const fetchDatasets = useCallback(async () => {
    if (!session) {
      console.log('⚠️ No session, clearing datasets');
      setDatasets([]);
      setDatasetCount(0);
      return;
    }

    try {
      console.log('🔄 Fetching datasets...');
      
      const { data } = await axios.get(`${API_BASE}/datasets`, {
        headers: getAuthHeaders()
      });
      
      const fetchedDatasets = data.datasets || [];
      
      console.log(`✅ Fetched ${fetchedDatasets.length} datasets`);
      
      // ✅ Simple, reliable count update
      setDatasets(fetchedDatasets);
      setDatasetCount(fetchedDatasets.length);
      
    } catch (error) {
      console.error('❌ Failed to fetch datasets:', error);
      
      // Don't reset count on error if we already have data
      if (datasets.length === 0) {
        setDatasetCount(0);
      }
    }
  }, [session, getAuthHeaders, datasets.length]);

  // ✅ Initial fetch on mount
  useEffect(() => {
    if (session && !hasInitiallyLoaded.current) {
      fetchDatasets();
      hasInitiallyLoaded.current = true;
    }
  }, [session, fetchDatasets]);

  // ✅ Polling every 3 seconds
  useEffect(() => {
    if (session) {
      const interval = setInterval(fetchDatasets, 3000);
      return () => clearInterval(interval);
    }
  }, [session, fetchDatasets]);

  // ✅ TASK 2 FIX: Removed optimistic update, just fetch after upload
  const onDrop = useCallback(async (files: File[]) => {
    const file = files[0];
    if (!file || !session) return;

    console.log('📁 File dropped:', file.name, `${(file.size / 1024 / 1024).toFixed(2)} MB`);

    setUploading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      console.log('📤 Uploading...');
      
      const { data } = await axios.post(`${API_BASE}/upload`, formData, {
        headers: { ...getAuthHeaders(), 'Content-Type': 'multipart/form-data' }
      });
      
      console.log('✅ Upload successful:', data.dataset_id);
      
      toast.success(`✅ Uploaded ${file.name}! Processing...`);
      
      // ✅ Just fetch - polling will pick up new count
      await fetchDatasets();
      
      setTimeout(() => {
        console.log('🎯 Auto-selecting:', data.dataset_id);
        setSelectedDataset(data.dataset_id);
      }, 1000);
      
    } catch (error: any) {
      console.error('❌ Upload failed:', error);
      toast.error(error.response?.data?.detail || 'Upload failed');
    } finally {
      setUploading(false);
    }
  }, [session, getAuthHeaders, fetchDatasets]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls']
    },
    maxFiles: 1,
    disabled: uploading
  });

  // ✅ Auth handlers
  const handleAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    
    try {
      if (isSignup) {
        const { error } = await supabase.auth.signUp({ email, password });
        if (error) throw error;
        toast.success('✅ Check your email to confirm!');
      } else {
        const { error } = await supabase.auth.signInWithPassword({ email, password });
        if (error) throw error;
        toast.success('✅ Welcome back!');
      }
    } catch (error: any) {
      toast.error(error.message || 'Authentication failed');
    }
  };

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    setDatasets([]);
    setDatasetCount(0);
    setSelectedDataset(null);
    toast.success('Signed out');
  };

  // ✅ Merge handlers
  const handleMergeClick = () => {
    if (selectedForMerge.length < 2) {
      toast.error('Select at least 2 datasets to merge');
      return;
    }
    setShowMergeModal(true);
  };

  const handleMergeComplete = async (mergedDataset: Dataset) => {
    toast.success(`✅ Merged dataset created!`);
    setShowMergeModal(false);
    setSelectedForMerge([]);
    await fetchDatasets();
    setSelectedDataset(mergedDataset.id);
  };

  const toggleDatasetSelection = (datasetId: string) => {
    setSelectedForMerge(prev => 
      prev.includes(datasetId) 
        ? prev.filter(id => id !== datasetId)
        : [...prev, datasetId]
    );
  };

  const currentDataset = datasets.find(d => d.id === selectedDataset);
  const readyDatasets = datasets.filter(d => d.status === 'ready');
  const selectedDatasetsForMerge = datasets.filter(d => selectedForMerge.includes(d.id));

  // ✅ Not logged in - show auth
  if (!session) {
    return (
      <div className="auth-container">
        <Toaster position="top-right" />
        
        <div className="auth-card">
          <div className="auth-header">
            <h1>⚡ JetDB</h1>
            <p>The first billion row spreadsheet</p>
          </div>

          <form onSubmit={handleAuth} className="auth-form">
            <input
              type="email"
              placeholder="Email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
            <input
              type="password"
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
            
            <button type="submit" className="btn-primary">
              {isSignup ? 'Sign Up' : 'Sign In'}
            </button>
            
            <button 
              type="button" 
              onClick={() => setIsSignup(!isSignup)}
              className="btn-link"
            >
              {isSignup ? 'Already have an account? Sign in' : "Don't have an account? Sign up"}
            </button>
          </form>

          <div className="auth-footer">
            <p className="text-sm">
              🚀 Handle 10GB CSVs • ⚡ Merge in seconds • 🤖 AI queries
            </p>
          </div>
        </div>
      </div>
    );
  }

  // ✅ Logged in - show main app
  return (
    <div className="app">
      <Toaster position="top-right" />

      {/* Header */}
      <header className="app-header">
        <div className="header-left">
          <h1 className="logo">⚡ JetDB</h1>
          <span className="version">v8.0</span>
        </div>

        <div className="header-center">
          {currentDataset && (
            <div className="dataset-info">
              <span className="dataset-name">{currentDataset.filename}</span>
              <span className="dataset-stats">
                {currentDataset.row_count.toLocaleString()} rows • {currentDataset.column_count} columns
              </span>
            </div>
          )}
        </div>

        <div className="header-right">
          <button 
            onClick={() => setShowDatasetList(!showDatasetList)} 
            className="btn-secondary"
            title={`You have ${datasetCount} total dataset${datasetCount !== 1 ? 's' : ''}`}
          >
            📊 Datasets ({datasetCount})
            {uploading && (
              <div 
                className="spinner" 
                style={{ 
                  width: '14px', 
                  height: '14px', 
                  borderWidth: '2px',
                  marginLeft: '8px'
                }}
              />
            )}
          </button>
          
          <button onClick={handleSignOut} className="btn-secondary">
            Sign Out
          </button>
        </div>
      </header>

      {/* Main Content */}
      <div className="app-content">
        {/* Sidebar */}
        {showDatasetList && (
          <aside className="sidebar">
            <div className="sidebar-header">
              <h3>Your Datasets</h3>
              <button onClick={() => setShowDatasetList(false)} className="btn-icon">×</button>
            </div>

            <div className="sidebar-content">
              {datasets.length === 0 ? (
                <div className="empty-state">
                  <p>No datasets yet</p>
                  <p className="text-sm">Upload a CSV to get started</p>
                </div>
              ) : (
                <div className="dataset-list">
                  {datasets.map(dataset => (
                    <div 
                      key={dataset.id}
                      className={`dataset-item ${selectedDataset === dataset.id ? 'active' : ''}`}
                      onClick={() => {
                        setSelectedDataset(dataset.id);
                        setShowDatasetList(false);
                      }}
                    >
                      <div className="dataset-item-header">
                        <input
                          type="checkbox"
                          checked={selectedForMerge.includes(dataset.id)}
                          onChange={() => toggleDatasetSelection(dataset.id)}
                          onClick={(e) => e.stopPropagation()}
                          disabled={dataset.status !== 'ready'}
                        />
                        <span className="dataset-filename">{dataset.filename}</span>
                        <span className={`status-badge status-${dataset.status}`}>
                          {dataset.status}
                        </span>
                      </div>
                      <div className="dataset-item-stats">
                        {dataset.row_count.toLocaleString()} rows • {dataset.column_count} cols
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {selectedForMerge.length >= 2 && (
                <div className="sidebar-footer">
                  <button onClick={handleMergeClick} className="btn-primary btn-block">
                    🔄 Merge {selectedForMerge.length} Datasets
                  </button>
                </div>
              )}
            </div>
          </aside>
        )}

        {/* Main View */}
        <main className="main-content">
          {!selectedDataset ? (
            // Upload zone
            <div className="upload-zone-container">
              <div {...getRootProps()} className={`upload-zone ${isDragActive ? 'drag-active' : ''} ${uploading ? 'uploading' : ''}`}>
                <input {...getInputProps()} />
                
                {uploading ? (
                  <>
                    <div className="spinner large" />
                    <h2>Uploading...</h2>
                    <p>Processing your file</p>
                  </>
                ) : (
                  <>
                    <div className="upload-icon">📁</div>
                    <h2>Drop your CSV here</h2>
                    <p>or click to browse</p>
                    <p className="upload-hint">
                      Supports files up to 10GB • CSV, XLSX, XLS
                    </p>
                  </>
                )}
              </div>

              {readyDatasets.length >= 2 && (
                <div className="quick-actions">
                  <h3>Quick Actions</h3>
                  <button 
                    onClick={() => {
                      setSelectedForMerge(readyDatasets.slice(0, 2).map(d => d.id));
                      handleMergeClick();
                    }}
                    className="btn-secondary"
                  >
                    🔄 Merge Your Datasets
                  </button>
                </div>
              )}
            </div>
          ) : (
            // Dataset view
            <>
              {/* View selector */}
              <div className="view-selector">
                <button 
                  onClick={() => setView('grid')}
                  className={`btn-tab ${view === 'grid' ? 'active' : ''}`}
                >
                  📊 Grid View
                </button>
                <button 
                  onClick={() => setView('query')}
                  className={`btn-tab ${view === 'query' ? 'active' : ''}`}
                >
                  🔍 Query
                </button>
              </div>

              {/* Content */}
              {currentDataset && currentDataset.status === 'ready' ? (
                view === 'grid' ? (
                  <DataGrid
                    datasetId={currentDataset.id}
                    totalRows={currentDataset.row_count}
                    apiBase={API_BASE}
                    authHeaders={getAuthHeaders()}
                  />
                ) : (
                  <QueryWorkspace
                    datasetId={currentDataset.id}
                    apiBase={API_BASE}
                    authHeaders={getAuthHeaders()}
                  />
                )
              ) : (
                <div className="loading-state">
                  <div className="spinner large" />
                  <h3>Processing dataset...</h3>
                  <p>{currentDataset?.status || 'Loading'}</p>
                </div>
              )}
            </>
          )}
        </main>
      </div>

      {/* Merge Modal */}
      {showMergeModal && (
        <MergeModal
          datasets={selectedDatasetsForMerge}
          apiBase={API_BASE}
          authHeaders={getAuthHeaders()}
          onClose={() => {
            setShowMergeModal(false);
            setSelectedForMerge([]);
          }}
          onComplete={handleMergeComplete}
        />
      )}
    </div>
  );
};

export default App;
