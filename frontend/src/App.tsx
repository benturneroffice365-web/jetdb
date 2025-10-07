import React, { useState, useEffect, useCallback } from "react";
import { useDropzone } from "react-dropzone";
import { createClient, Session } from '@supabase/supabase-js';
import axios from "axios";
import toast, { Toaster } from "react-hot-toast";
import Papa from "papaparse";
import $ from "jquery";
import "jquery-mousewheel";
import luckysheet from "luckysheet";
import "luckysheet/dist/plugins/css/pluginsCss.css";
import "luckysheet/dist/plugins/plugins.css";
import "luckysheet/dist/css/luckysheet.css";
import "luckysheet/dist/assets/iconfont/iconfont.css";
import "./App.css";

(window as any).$ = $;
(window as any).jQuery = $;

// Initialize Supabase client
const supabase = createClient(
  process.env.REACT_APP_SUPABASE_URL || '',
  process.env.REACT_APP_SUPABASE_KEY || ''
);

const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:8000";
const VERSION = "7.2.0";

interface Dataset {
  id: string;
  filename: string;
  row_count: number | null;
  estimated_rows: number;
  column_count: number;
  columns: string[];
  uploaded_at: string;
  status: "analyzing" | "ready" | "error";
  file_size_mb: number;
}

const App: React.FC = () => {
  // Auth state
  const [session, setSession] = useState<Session | null>(null);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isSignup, setIsSignup] = useState(false);
  const [authLoading, setAuthLoading] = useState(true);
  
  // App state
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [uploading, setUploading] = useState<boolean>(false);
  const [uploadProgress, setUploadProgress] = useState<number>(0);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [spreadsheetInitialized, setSpreadsheetInitialized] = useState<boolean>(false);
  const [showDatasetPicker, setShowDatasetPicker] = useState<boolean>(false);
  const [showSQLModal, setShowSQLModal] = useState<boolean>(false);
  const [showAIModal, setShowAIModal] = useState<boolean>(false);
  const [sqlQuery, setSqlQuery] = useState<string>("SELECT * FROM data LIMIT 100");
  const [aiQuestion, setAiQuestion] = useState<string>("");
  const [queryLoading, setQueryLoading] = useState<boolean>(false);

  const MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024;
  const SPREADSHEET_PAGE_SIZE = 10000;

  // Check auth session on mount
  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setAuthLoading(false);
    });

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session);
    });

    return () => subscription.unsubscribe();
  }, []);

  // Get auth headers for API calls
  const getAuthHeaders = useCallback(() => {
    if (!session?.access_token) {
      throw new Error("Not authenticated");
    }
    return {
      'Authorization': `Bearer ${session.access_token}`
    };
  }, [session]);

  // Mobile warning
  useEffect(() => {
    if (window.innerWidth < 768) {
      toast("⚠️ JetDB works best on desktop", { duration: 8000 });
    }
  }, []);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyPress = (e: KeyboardEvent) => {
      if (e.ctrlKey || e.metaKey) {
        if (e.key === 'k') { 
          e.preventDefault(); 
          if (selectedDataset) setShowSQLModal(true);
        }
        if (e.key === 'j') { 
          e.preventDefault(); 
          if (selectedDataset) setShowAIModal(true);
        }
      }
    };
    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [selectedDataset]);

  // Auth functions
  const handleAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    setAuthLoading(true);

    try {
      if (isSignup) {
        const { error } = await supabase.auth.signUp({ email, password });
        if (error) throw error;
        toast.success("Account created! Check your email to verify.");
      } else {
        const { error } = await supabase.auth.signInWithPassword({ email, password });
        if (error) throw error;
        toast.success("Welcome back!");
      }
    } catch (error: any) {
      toast.error(error.message || "Authentication failed");
    } finally {
      setAuthLoading(false);
    }
  };

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    setDatasets([]);
    setSelectedDataset(null);
    toast.success("Signed out");
  };

  // Fetch datasets
  const fetchDatasets = useCallback(async () => {
    if (!session) return;
    
    try {
      const { data } = await axios.get(`${API_BASE}/datasets`, {
        headers: getAuthHeaders()
      });
      setDatasets(data.datasets || []);
    } catch (error: any) {
      console.error("Failed to fetch datasets:", error);
      if (error.response?.status === 401) {
        toast.error("Session expired. Please log in again.");
        handleSignOut();
      }
    }
  }, [session, getAuthHeaders]);

  useEffect(() => {
    if (session) {
      fetchDatasets();
    }
  }, [session, fetchDatasets]);

  // File upload
  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    if (!session) {
      toast.error("Please log in first");
      return;
    }

    const file = acceptedFiles[0];
    if (!file) return;

    if (file.size > MAX_FILE_SIZE) {
      toast.error("File too large (max 10GB)");
      return;
    }

    if (!file.name.endsWith('.csv')) {
      toast.error("Only CSV files supported");
      return;
    }

    setUploading(true);
    setUploadProgress(0);

    const formData = new FormData();
    formData.append("file", file);

    try {
      const { data } = await axios.post(`${API_BASE}/upload`, formData, {
        headers: {
          ...getAuthHeaders(),
          "Content-Type": "multipart/form-data",
        },
        onUploadProgress: (progressEvent) => {
          const progress = progressEvent.total
            ? Math.round((progressEvent.loaded * 100) / progressEvent.total)
            : 0;
          setUploadProgress(progress);
        },
      });

      toast.success(`Uploaded ${file.name}! Analyzing...`);
      await fetchDatasets();
      setSelectedDataset(data.dataset_id);
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Upload failed");
    } finally {
      setUploading(false);
      setUploadProgress(0);
    }
  }, [session, getAuthHeaders, fetchDatasets]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'text/csv': ['.csv'] },
    multiple: false,
    disabled: uploading || !session,
  });

  // Load data into spreadsheet
  const loadDataIntoSpreadsheet = useCallback(async (datasetId: string) => {
    if (!session) return;

    try {
      const { data } = await axios.get(
        `${API_BASE}/datasets/${datasetId}/rows?limit=${SPREADSHEET_PAGE_SIZE}`,
        { headers: getAuthHeaders() }
      );

      const rows = data.data;
      if (!rows || rows.length === 0) {
        toast.error("No data to display");
        return;
      }

      const columns = Object.keys(rows[0]);
      const luckysheetData = [
        columns.map((col: string) => ({
          v: col,
          m: col,
          ct: { fa: "General", t: "s" },
          bg: "#f0f0f0",
          bl: 1,
        })),
        ...rows.map((row: any) =>
          columns.map((col: string) => ({
            v: row[col],
            m: String(row[col] ?? ""),
            ct: { fa: "General", t: "g" },
          }))
        ),
      ];

      const container = document.getElementById("luckysheet-container");
      if (container) {
        container.innerHTML = "";
      }

      luckysheet.create({
        container: "luckysheet-container",
        showinfobar: false,
        showsheetbar: false,
        showstatisticBar: false,
        enableAddRow: false,
        enableAddCol: false,
        userInfo: false,
        showConfigWindowResize: false,
        data: [
          {
            name: "Sheet1",
            color: "",
            status: 1,
            order: 0,
            data: luckysheetData,
            config: {},
            index: 0,
          },
        ],
      });

      setSpreadsheetInitialized(true);
      
      if (data.total_rows > SPREADSHEET_PAGE_SIZE) {
        toast(`Showing first ${SPREADSHEET_PAGE_SIZE:,} of ${data.total_rows:,} rows`, {
          duration: 5000,
          icon: "ℹ️",
        });
      }
    } catch (error: any) {
      console.error("Load failed:", error);
      toast.error(error.response?.data?.detail || "Failed to load data");
    }
  }, [session, getAuthHeaders]);

  useEffect(() => {
    if (selectedDataset) {
      loadDataIntoSpreadsheet(selectedDataset);
    }
  }, [selectedDataset, loadDataIntoSpreadsheet]);

  // SQL Query
  const executeSQLQuery = async () => {
    if (!selectedDataset || !session) return;

    setQueryLoading(true);
    try {
      const { data } = await axios.post(
        `${API_BASE}/query/sql`,
        {
          sql: sqlQuery,
          dataset_id: selectedDataset,
        },
        { headers: getAuthHeaders() }
      );

      const rows = data.data;
      if (!rows || rows.length === 0) {
        toast.error("Query returned no results");
        setQueryLoading(false);
        return;
      }

      const columns = Object.keys(rows[0]);
      const luckysheetData = [
        columns.map((col: string) => ({
          v: col,
          m: col,
          ct: { fa: "General", t: "s" },
          bg: "#e3f2fd",
          bl: 1,
        })),
        ...rows.map((row: any) =>
          columns.map((col: string) => ({
            v: row[col],
            m: String(row[col] ?? ""),
            ct: { fa: "General", t: "g" },
          }))
        ),
      ];

      const container = document.getElementById("luckysheet-container");
      if (container) {
        container.innerHTML = "";
      }

      luckysheet.create({
        container: "luckysheet-container",
        showinfobar: false,
        showsheetbar: false,
        showstatisticBar: false,
        enableAddRow: false,
        enableAddCol: false,
        userInfo: false,
        showConfigWindowResize: false,
        data: [
          {
            name: "Query Results",
            color: "",
            status: 1,
            order: 0,
            data: luckysheetData,
            config: {},
            index: 0,
          },
        ],
      });

      toast.success(`Query returned ${data.rows} rows in ${data.query_time_seconds}s`);
      setShowSQLModal(false);
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Query failed");
    } finally {
      setQueryLoading(false);
    }
  };

  // AI Query
  const executeAIQuery = async () => {
    if (!selectedDataset || !aiQuestion.trim() || !session) return;

    setQueryLoading(true);
    try {
      const { data } = await axios.post(
        `${API_BASE}/query/natural`,
        {
          question: aiQuestion,
          dataset_id: selectedDataset,
        },
        { headers: getAuthHeaders() }
      );

      const rows = data.data;
      if (!rows || rows.length === 0) {
        toast.error("Query returned no results");
        setQueryLoading(false);
        return;
      }

      const columns = Object.keys(rows[0]);
      const luckysheetData = [
        columns.map((col: string) => ({
          v: col,
          m: col,
          ct: { fa: "General", t: "s" },
          bg: "#f3e5f5",
          bl: 1,
        })),
        ...rows.map((row: any) =>
          columns.map((col: string) => ({
            v: row[col],
            m: String(row[col] ?? ""),
            ct: { fa: "General", t: "g" },
          }))
        ),
      ];

      const container = document.getElementById("luckysheet-container");
      if (container) {
        container.innerHTML = "";
      }

      luckysheet.create({
        container: "luckysheet-container",
        showinfobar: false,
        showsheetbar: false,
        showstatisticBar: false,
        enableAddRow: false,
        enableAddCol: false,
        userInfo: false,
        showConfigWindowResize: false,
        data: [
          {
            name: "AI Results",
            color: "",
            status: 1,
            order: 0,
            data: luckysheetData,
            config: {},
            index: 0,
          },
        ],
      });

      toast.success(
        <div>
          <div>✨ {data.rows} rows in {data.query_time_seconds}s</div>
          <div style={{ fontSize: "11px", opacity: 0.8, marginTop: "4px" }}>
            SQL: {data.generated_sql}
          </div>
        </div>,
        { duration: 6000 }
      );
      setShowAIModal(false);
      setAiQuestion("");
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "AI query failed");
    } finally {
      setQueryLoading(false);
    }
  };

  // Delete dataset
  const deleteDataset = async (datasetId: string) => {
    if (!session) return;
    if (!window.confirm("Delete this dataset? This cannot be undone.")) return;

    try {
      await axios.delete(`${API_BASE}/datasets/${datasetId}`, {
        headers: getAuthHeaders()
      });
      toast.success("Dataset deleted");
      setDatasets(datasets.filter((d) => d.id !== datasetId));
      if (selectedDataset === datasetId) {
        setSelectedDataset(null);
      }
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Delete failed");
    }
  };

  // Get current dataset info
  const currentDataset = datasets.find((d) => d.id === selectedDataset);
  const filteredDatasets = datasets.filter((d) =>
    d.filename.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Auth screen
  if (authLoading) {
    return (
      <div className="auth-container">
        <div className="auth-box">
          <div className="auth-loader">Loading...</div>
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <div className="auth-container">
        <Toaster position="top-right" />
        <div className="auth-box">
          <div className="auth-header">
            <h1>⚡ JetDB</h1>
            <p>Excel for Massive Datasets</p>
            <div className="version-badge">v{VERSION} • Powered by GPT-4o-mini</div>
          </div>

          <form onSubmit={handleAuth} className="auth-form">
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
              minLength={6}
            />
            <button type="submit" className="auth-button" disabled={authLoading}>
              {authLoading ? "Loading..." : isSignup ? "Sign Up" : "Sign In"}
            </button>
          </form>

          <div className="auth-toggle">
            {isSignup ? "Already have an account?" : "Don't have an account?"}
            <button onClick={() => setIsSignup(!isSignup)} className="auth-link">
              {isSignup ? "Sign In" : "Sign Up"}
            </button>
          </div>

          <div className="auth-features">
            <div className="feature-item">📊 Upload massive CSVs (up to 10GB)</div>
            <div className="feature-item">⚡ Query millions of rows instantly</div>
            <div className="feature-item">🤖 Ask questions in plain English</div>
          </div>
        </div>
      </div>
    );
  }

  // Main app
  return (
    <div className="app">
      <Toaster position="top-right" />

      {/* Header */}
      <header className="header">
        <div className="header-left">
          <h1 className="logo">⚡ JetDB</h1>
          <span className="version">v{VERSION}</span>
          {currentDataset && (
            <span className="current-file">
              {currentDataset.filename}
              {currentDataset.row_count && (
                <span className="row-count">
                  {currentDataset.row_count.toLocaleString()} rows
                </span>
              )}
            </span>
          )}
        </div>
        
        <div className="header-right">
          <button onClick={() => setShowDatasetPicker(true)} className="btn-secondary">
            📁 Datasets ({datasets.length})
          </button>
          {selectedDataset && (
            <>
              <button onClick={() => setShowSQLModal(true)} className="btn-secondary" title="Ctrl+K">
                💻 SQL
              </button>
              <button onClick={() => setShowAIModal(true)} className="btn-primary" title="Ctrl+J">
                🤖 AI Chat
              </button>
            </>
          )}
          <button onClick={handleSignOut} className="btn-secondary">
            Sign Out
          </button>
        </div>
      </header>

      {/* Main Content */}
      <div className="main-content">
        {!selectedDataset ? (
          <div
            {...getRootProps()}
            className={`dropzone ${isDragActive ? "dropzone-active" : ""} ${
              uploading ? "dropzone-uploading" : ""
            }`}
          >
            <input {...getInputProps()} />
            {uploading ? (
              <div className="upload-progress">
                <div className="spinner"></div>
                <p>Uploading... {uploadProgress}%</p>
                <div className="progress-bar">
                  <div
                    className="progress-fill"
                    style={{ width: `${uploadProgress}%` }}
                  ></div>
                </div>
              </div>
            ) : (
              <>
                <div className="dropzone-icon">📊</div>
                <h2>Drop a CSV file here</h2>
                <p>or click to browse • Up to 10GB</p>
                <div className="dropzone-features">
                  <span>⚡ Lightning fast queries</span>
                  <span>🤖 AI-powered with GPT-4o-mini</span>
                  <span>📈 Handle millions of rows</span>
                </div>
              </>
            )}
          </div>
        ) : (
          <div id="luckysheet-container" className="spreadsheet-container"></div>
        )}
      </div>

      {/* Dataset Picker Modal */}
      {showDatasetPicker && (
        <div className="modal-overlay" onClick={() => setShowDatasetPicker(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>📁 Your Datasets</h2>
              <button onClick={() => setShowDatasetPicker(false)} className="modal-close">
                ✕
              </button>
            </div>

            <input
              type="text"
              placeholder="Search datasets..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="search-input"
            />

            <div className="dataset-list">
              {filteredDatasets.length === 0 ? (
                <div className="empty-state">
                  <p>No datasets found</p>
                  <button onClick={() => setShowDatasetPicker(false)} className="btn-primary">
                    Upload Your First CSV
                  </button>
                </div>
              ) : (
                filteredDatasets.map((dataset) => (
                  <div
                    key={dataset.id}
                    className={`dataset-item ${
                      selectedDataset === dataset.id ? "dataset-item-selected" : ""
                    }`}
                  >
                    <div className="dataset-info" onClick={() => {
                      setSelectedDataset(dataset.id);
                      setShowDatasetPicker(false);
                    }}>
                      <div className="dataset-name">{dataset.filename}</div>
                      <div className="dataset-meta">
                        {dataset.status === "ready" && dataset.row_count ? (
                          <>
                            {dataset.row_count.toLocaleString()} rows •{" "}
                            {dataset.column_count} columns
                          </>
                        ) : dataset.status === "analyzing" ? (
                          <span className="status-analyzing">⏳ Analyzing...</span>
                        ) : dataset.status === "error" ? (
                          <span className="status-error">❌ Error</span>
                        ) : (
                          <>~{dataset.estimated_rows.toLocaleString()} rows (estimated)</>
                        )}
                      </div>
                      <div className="dataset-date">
                        {new Date(dataset.uploaded_at).toLocaleString()}
                      </div>
                    </div>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        deleteDataset(dataset.id);
                      }}
                      className="btn-delete"
                      title="Delete"
                    >
                      🗑️
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}

      {/* SQL Modal */}
      {showSQLModal && (
        <div className="modal-overlay" onClick={() => setShowSQLModal(false)}>
          <div className="modal modal-large" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>💻 SQL Query</h2>
              <button onClick={() => setShowSQLModal(false)} className="modal-close">
                ✕
              </button>
            </div>

            <textarea
              value={sqlQuery}
              onChange={(e) => setSqlQuery(e.target.value)}
              className="sql-editor"
              placeholder="SELECT * FROM data LIMIT 100"
              rows={10}
            />

            <div className="modal-footer">
              <button onClick={() => setShowSQLModal(false)} className="btn-secondary">
                Cancel
              </button>
              <button
                onClick={executeSQLQuery}
                className="btn-primary"
                disabled={queryLoading}
              >
                {queryLoading ? "Running..." : "Run Query"}
              </button>
            </div>

            <div className="modal-hint">
              💡 Use 'data' as the table name. Only SELECT queries allowed for security.
            </div>
          </div>
        </div>
      )}

      {/* AI Modal */}
      {showAIModal && (
        <div className="modal-overlay" onClick={() => setShowAIModal(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>🤖 AI Chat</h2>
              <button onClick={() => setShowAIModal(false)} className="modal-close">
                ✕
              </button>
            </div>

            <div className="ai-info">
              <span className="ai-badge">Powered by GPT-4o-mini</span>
              <p>Ask questions about your data in plain English</p>
            </div>

            <textarea
              value={aiQuestion}
              onChange={(e) => setAiQuestion(e.target.value)}
              className="ai-input"
              placeholder="What are the top 10 rows sorted by price?"
              rows={4}
            />

            <div className="modal-footer">
              <button onClick={() => setShowAIModal(false)} className="btn-secondary">
                Cancel
              </button>
              <button
                onClick={executeAIQuery}
                className="btn-primary"
                disabled={queryLoading || !aiQuestion.trim()}
              >
                {queryLoading ? "Thinking..." : "Ask AI"}
              </button>
            </div>

            <div className="modal-examples">
              <div className="examples-title">Example questions:</div>
              <button
                onClick={() => setAiQuestion("Show me the top 10 rows by revenue")}
                className="example-btn"
              >
                Show me the top 10 rows by revenue
              </button>
              <button
                onClick={() => setAiQuestion("What's the average value in the price column?")}
                className="example-btn"
              >
                What's the average value in the price column?
              </button>
              <button
                onClick={() => setAiQuestion("Group by category and count rows")}
                className="example-btn"
              >
                Group by category and count rows
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default App;