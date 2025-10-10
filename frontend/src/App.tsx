/**
 * JetDB Frontend v7.3.1 - Production Ready
 * ==========================================
 * FIXES IMPLEMENTED:
 * ✅ 8. Auto-Refresh Dataset List (after upload)
 * ✅ 9. Loading States (comprehensive for all operations)
 * ✅ 10. Luckysheet Timing Fix (getContext error resolved)
 */

import ErrorBoundary from './components/ErrorBoundary';
import React, { useState, useEffect, useCallback } from "react";
import { useDropzone } from "react-dropzone";
import { createClient, Session } from '@supabase/supabase-js';
import axios from "axios";
import toast, { Toaster } from "react-hot-toast";
import $ from "jquery";
import "jquery-mousewheel";
import luckysheet from "luckysheet";
import "luckysheet/dist/plugins/css/pluginsCss.css";
import "luckysheet/dist/plugins/plugins.css";
import "luckysheet/dist/css/luckysheet.css";
import "luckysheet/dist/assets/iconfont/iconfont.css";
import "./App.css";

// Ensure jQuery and mousewheel are globally available
(window as any).$ = $;
(window as any).jQuery = $;

// Force load mousewheel plugin
if ($.fn && !$.fn.mousewheel) {
  console.warn("⚠️ jQuery mousewheel not loaded, attempting to load...");
  require("jquery-mousewheel");
}

// Initialize Supabase client
const supabase = createClient(
  process.env.REACT_APP_SUPABASE_URL || '',
  process.env.REACT_APP_SUPABASE_KEY || ''
);

const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:8000";
const VERSION = "7.3.1";

interface Dataset {
  id: string;
  filename: string;
  row_count: number | null;
  estimated_rows: number;
  column_count: number;
  columns: string[];
  uploaded_at: string;
  status: "analyzing" | "ready" | "error";
  size_bytes: number;
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
  
  // Loading states for better UX
  const [datasetsLoading, setDatasetsLoading] = useState<boolean>(false);
  const [datasetLoading, setDatasetLoading] = useState<boolean>(false);
  const [deletingDatasetId, setDeletingDatasetId] = useState<string | null>(null);

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
          if (selectedDataset && !queryLoading) setShowSQLModal(true);
        }
        if (e.key === 'j') { 
          e.preventDefault(); 
          if (selectedDataset && !queryLoading) setShowAIModal(true);
        }
      }
    };
    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [selectedDataset, queryLoading]);

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

  // Fetch datasets (used by auto-refresh)
  const fetchDatasets = useCallback(async (showLoading: boolean = true) => {
    if (!session) return;
    
    if (showLoading) setDatasetsLoading(true);
    
    try {
      const { data } = await axios.get(`${API_BASE}/datasets`, {
        headers: getAuthHeaders()
      });
      setDatasets(data.datasets || []);
      
      // If we're showing a dataset that's analyzing, check again in 3 seconds
      const currentDataset = data.datasets?.find((d: Dataset) => d.id === selectedDataset);
      if (currentDataset?.status === "analyzing") {
        setTimeout(() => fetchDatasets(false), 3000);
      }
    } catch (error: any) {
      console.error("Failed to fetch datasets:", error);
      if (error.response?.status === 401) {
        toast.error("Session expired. Please log in again.");
        handleSignOut();
      }
    } finally {
      if (showLoading) setDatasetsLoading(false);
    }
  }, [session, getAuthHeaders, selectedDataset]);

  useEffect(() => {
    if (session) {
      fetchDatasets();
    }
  }, [session, fetchDatasets]);

  // File upload (auto-refresh after upload)
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

      toast.success(`✅ Uploaded ${file.name}! Analyzing...`);
      
      // Auto-refresh dataset list after upload
      await fetchDatasets(false);
      setSelectedDataset(data.dataset_id);
      
      // Poll for analysis completion
      const pollInterval = setInterval(async () => {
        await fetchDatasets(false);
        const updatedDataset = datasets.find(d => d.id === data.dataset_id);
        if (updatedDataset?.status === "ready" || updatedDataset?.status === "error") {
          clearInterval(pollInterval);
          if (updatedDataset.status === "ready") {
            toast.success(`✅ Analysis complete! ${updatedDataset.row_count?.toLocaleString()} rows`);
          }
        }
      }, 2000);
      
      // Stop polling after 2 minutes
      setTimeout(() => clearInterval(pollInterval), 120000);
      
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Upload failed");
    } finally {
      setUploading(false);
      setUploadProgress(0);
    }
  }, [session, getAuthHeaders, fetchDatasets, datasets]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'text/csv': ['.csv'] },
    multiple: false,
    disabled: uploading || !session,
  });

  // Load data into spreadsheet - FIXED TIMING ISSUE WITH DEBUGGING
  const loadDataIntoSpreadsheet = useCallback(async (datasetId: string) => {
    if (!session) return;

    setDatasetLoading(true);
    
    try {
      console.log("📊 Fetching dataset rows...");
      const { data } = await axios.get(
        `${API_BASE}/datasets/${datasetId}/rows?limit=${SPREADSHEET_PAGE_SIZE}`,
        { headers: getAuthHeaders() }
      );

      const rows = data.data;
      console.log(`✅ Received ${rows?.length || 0} rows`);
      
      if (!rows || rows.length === 0) {
        toast.error("No data to display");
        setDatasetLoading(false);
        return;
      }

      const columns = Object.keys(rows[0]);
      console.log(`📋 Columns: ${columns.join(", ")}`);
      
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

      console.log("⏳ Waiting for DOM...");

      // CRITICAL FIX: Wait for DOM to be ready before initializing Luckysheet
      setTimeout(() => {
        console.log("🔍 Checking for container...");
        const container = document.getElementById("luckysheet-container");
        
        if (!container) {
          console.error("❌ Container not found!");
          toast.error("Spreadsheet container not found - try refreshing");
          setDatasetLoading(false);
          return;
        }

        console.log("✅ Container found:", container);
        console.log("📦 Container dimensions:", container.offsetWidth, "x", container.offsetHeight);

        // Check if Luckysheet is available
        if (typeof luckysheet === 'undefined' || !luckysheet.create) {
          console.error("❌ Luckysheet library not loaded!");
          toast.error("Spreadsheet library not loaded - try refreshing");
          setDatasetLoading(false);
          return;
        }

        console.log("✅ Luckysheet library loaded");

        // Check if jQuery mousewheel is loaded
        const $ = (window as any).$;
        if (!$ || !$.fn || !$.fn.mousewheel) {
          console.error("❌ jQuery mousewheel not loaded!");
          toast.error("Spreadsheet dependencies missing - try refreshing");
          setDatasetLoading(false);
          return;
        }

        console.log("✅ jQuery mousewheel loaded");

        // Clear any existing Luckysheet instance
        container.innerHTML = "";
        console.log("🧹 Container cleared");

        try {
          console.log("🚀 Initializing Luckysheet...");
          
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

          console.log("✅ Luckysheet initialized successfully!");
          setSpreadsheetInitialized(true);
          setDatasetLoading(false);
          
          if (data.total_rows && data.total_rows > SPREADSHEET_PAGE_SIZE) {
            toast(`Showing first ${SPREADSHEET_PAGE_SIZE.toLocaleString()} of ${data.total_rows.toLocaleString()} rows`, {
              duration: 5000,
              icon: "ℹ️",
            });
          }
        } catch (err: any) {
          console.error("❌ Luckysheet initialization error:", err);
          console.error("Error message:", err.message);
          console.error("Error stack:", err.stack);
          toast.error(`Spreadsheet error: ${err.message || "Unknown error"}`);
          setDatasetLoading(false);
        }
      }, 250); // Increased to 250ms for safer timing

    } catch (error: any) {
      console.error("❌ Load failed:", error);
      toast.error(error.response?.data?.detail || "Failed to load data");
      setDatasetLoading(false);
    }
  }, [session, getAuthHeaders]);

  useEffect(() => {
    if (selectedDataset) {
      console.log("🎯 Selected dataset changed:", selectedDataset);
      
      // Wait for container to be rendered in DOM
      const waitForContainer = (attempts = 0) => {
        const container = document.getElementById("luckysheet-container");
        
        if (container) {
          console.log("✅ Container found, loading data...");
          loadDataIntoSpreadsheet(selectedDataset);
        } else if (attempts < 10) {
          console.log(`⏳ Container not ready, attempt ${attempts + 1}/10`);
          setTimeout(() => waitForContainer(attempts + 1), 100);
        } else {
          console.error("❌ Container never appeared after 10 attempts");
          toast.error("Failed to initialize spreadsheet - please try again");
          setDatasetLoading(false);
        }
      };
      
      waitForContainer();
    }
  }, [selectedDataset, loadDataIntoSpreadsheet]);

  // SQL Query (better loading feedback)
  const executeSQLQuery = async () => {
    if (!selectedDataset || !session) return;

    setQueryLoading(true);
    const loadingToast = toast.loading("🔍 Running SQL query...");
    
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
        toast.error("Query returned no results", { id: loadingToast });
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

      // Wait for DOM before re-initializing
      setTimeout(() => {
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

        toast.success(
          `✅ Query returned ${data.rows_returned} rows in ${data.execution_time_seconds}s`,
          { id: loadingToast }
        );
        setShowSQLModal(false);
        setQueryLoading(false);
      }, 100);
      
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Query failed", { id: loadingToast });
      setQueryLoading(false);
    }
  };

  // AI Query (better loading feedback)
  const executeAIQuery = async () => {
    if (!selectedDataset || !aiQuestion.trim() || !session) return;

    setQueryLoading(true);
    const loadingToast = toast.loading("🤖 AI is thinking...");
    
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
        toast.error("Query returned no results", { id: loadingToast });
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

      // Wait for DOM before re-initializing
      setTimeout(() => {
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
            <div>✨ {data.rows_returned} rows in {data.execution_time_seconds}s</div>
            <div style={{ fontSize: "11px", opacity: 0.8, marginTop: "4px" }}>
              SQL: {data.sql_query}
            </div>
          </div>,
          { duration: 6000, id: loadingToast }
        );
        setShowAIModal(false);
        setAiQuestion("");
        setQueryLoading(false);
      }, 100);
      
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "AI query failed", { id: loadingToast });
      setQueryLoading(false);
    }
  };

  // Delete dataset (auto-refresh + loading state)
  const deleteDataset = async (datasetId: string) => {
    if (!session) return;
    if (!window.confirm("Delete this dataset? This cannot be undone.")) return;

    setDeletingDatasetId(datasetId);
    const loadingToast = toast.loading("🗑️ Deleting dataset...");

    try {
      await axios.delete(`${API_BASE}/datasets/${datasetId}`, {
        headers: getAuthHeaders()
      });
      
      toast.success("✅ Dataset deleted", { id: loadingToast });
      
      // Auto-refresh dataset list after delete
      await fetchDatasets(false);
      
      if (selectedDataset === datasetId) {
        setSelectedDataset(null);
      }
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Delete failed", { id: loadingToast });
    } finally {
      setDeletingDatasetId(null);
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
      <ErrorBoundary>
        <div className="auth-container">
          <div className="auth-box">
            <div className="auth-loader">
              <div className="spinner" style={{ margin: "0 auto" }}></div>
              <p style={{ marginTop: "16px" }}>Loading...</p>
            </div>
          </div>
        </div>
      </ErrorBoundary>
    );
  }

  if (!session) {
    return (
      <ErrorBoundary>
        <div className="auth-container">
          <Toaster position="top-right" />
          <div className="auth-box">
            <div className="auth-header">
              <h1>JetDB</h1>
              <p>Big data for the rest of us</p>
              <div className="version-badge">v{VERSION} • Production Ready</div>
            </div>

            <form onSubmit={handleAuth} className="auth-form">
              <input
                type="email"
                placeholder="Email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
                className="auth-input"
                disabled={authLoading}
              />
              <input
                type="password"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                className="auth-input"
                minLength={6}
                disabled={authLoading}
              />
              <button type="submit" className="auth-button" disabled={authLoading}>
                {authLoading ? "Loading..." : isSignup ? "Sign Up" : "Sign In"}
              </button>
            </form>

            <div className="auth-toggle">
              {isSignup ? "Already have an account?" : "Don't have an account?"}
              <button 
                onClick={() => setIsSignup(!isSignup)} 
                className="auth-link"
                disabled={authLoading}
              >
                {isSignup ? "Sign In" : "Sign Up"}
              </button>
            </div>

            <div className="auth-features">
              <div className="feature-item">⚡ Upload massive CSVs (up to 10GB)</div>
              <div className="feature-item">💻 Query millions of rows with SQL</div>
              <div className="feature-item">🤖 Ask questions in plain English (GPT-4o-mini)</div>
              <div className="feature-item">☁️ Secure cloud storage with Azure</div>
            </div>
          </div>
        </div>
      </ErrorBoundary>
    );
  }

  // Main app
  return (
    <ErrorBoundary>
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
                {currentDataset.status === "analyzing" && (
                  <span className="row-count" style={{ background: "#ffa726" }}>
                    ⏳ Analyzing...
                  </span>
                )}
                {currentDataset.status === "ready" && (currentDataset.row_count || currentDataset.estimated_rows) && (
                  <span className="row-count">
                    {(currentDataset.row_count || currentDataset.estimated_rows || 0).toLocaleString()} rows
                  </span>
                )}
                {currentDataset.status === "error" && (
                  <span className="row-count" style={{ background: "#d32f2f" }}>
                    ❌ Error
                  </span>
                )}
              </span>
            )}
          </div>
          
          <div className="header-right">
            <button 
              onClick={() => setShowDatasetPicker(true)} 
              className="btn-secondary"
              disabled={datasetsLoading}
            >
              📁 Datasets ({datasets.length})
            </button>
            {selectedDataset && (
              <>
                <button 
                  onClick={() => setShowSQLModal(true)} 
                  className="btn-secondary" 
                  title="Ctrl+K"
                  disabled={queryLoading || datasetLoading}
                >
                  💻 SQL
                </button>
                <button 
                  onClick={() => setShowAIModal(true)} 
                  className="btn-primary" 
                  title="Ctrl+J"
                  disabled={queryLoading || datasetLoading}
                >
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
                    <span>🤖 AI-powered analysis</span>
                    <span>☁️ Secure cloud storage</span>
                  </div>
                </>
              )}
            </div>
          ) : (
            <>
              {datasetLoading ? (
                <div style={{ 
                  display: "flex", 
                  flexDirection: "column", 
                  alignItems: "center", 
                  justifyContent: "center", 
                  height: "100%",
                  gap: "20px"
                }}>
                  <div className="spinner"></div>
                  <p style={{ fontSize: "18px", color: "#aaa" }}>Loading dataset...</p>
                </div>
              ) : null}
              <div 
                id="luckysheet-container" 
                className="spreadsheet-container"
                style={{ 
                  display: datasetLoading ? 'none' : 'block',
                  width: '100%',
                  height: '100%'
                }}
              ></div>
            </>
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
                {datasetsLoading ? (
                  <div style={{ textAlign: "center", padding: "40px" }}>
                    <div className="spinner" style={{ margin: "0 auto" }}></div>
                    <p style={{ marginTop: "16px", color: "#888" }}>Loading datasets...</p>
                  </div>
                ) : filteredDatasets.length === 0 ? (
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
                              {(dataset.row_count || 0).toLocaleString()} rows •{" "}
                              {dataset.column_count} columns •{" "}
                              {(dataset.size_bytes / (1024 * 1024)).toFixed(1)} MB
                            </>
                          ) : dataset.status === "analyzing" ? (
                            <span className="status-analyzing">⏳ Analyzing...</span>
                          ) : dataset.status === "error" ? (
                            <span className="status-error">❌ Error</span>
                          ) : (
                            <>~{(dataset.estimated_rows || 0).toLocaleString()} rows (estimated)</>
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
                        disabled={deletingDatasetId === dataset.id}
                      >
                        {deletingDatasetId === dataset.id ? "..." : "🗑️"}
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
          <div className="modal-overlay" onClick={() => !queryLoading && setShowSQLModal(false)}>
            <div className="modal modal-large" onClick={(e) => e.stopPropagation()}>
              <div className="modal-header">
                <h2>💻 SQL Query</h2>
                <button 
                  onClick={() => setShowSQLModal(false)} 
                  className="modal-close"
                  disabled={queryLoading}
                >
                  ✕
                </button>
              </div>

              <textarea
                value={sqlQuery}
                onChange={(e) => setSqlQuery(e.target.value)}
                className="sql-editor"
                placeholder="SELECT * FROM data LIMIT 100"
                rows={10}
                disabled={queryLoading}
              />

              <div className="modal-footer">
                <button 
                  onClick={() => setShowSQLModal(false)} 
                  className="btn-secondary"
                  disabled={queryLoading}
                >
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
          <div className="modal-overlay" onClick={() => !queryLoading && setShowAIModal(false)}>
            <div className="modal" onClick={(e) => e.stopPropagation()}>
              <div className="modal-header">
                <h2>🤖 AI Chat</h2>
                <button 
                  onClick={() => setShowAIModal(false)} 
                  className="modal-close"
                  disabled={queryLoading}
                >
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
                placeholder="What are the top 10 rows sorted by revenue?"
                rows={4}
                disabled={queryLoading}
              />

              <div className="modal-footer">
                <button 
                  onClick={() => setShowAIModal(false)} 
                  className="btn-secondary"
                  disabled={queryLoading}
                >
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
                  disabled={queryLoading}
                >
                  Show me the top 10 rows by revenue
                </button>
                <button
                  onClick={() => setAiQuestion("What's the average value in the price column?")}
                  className="example-btn"
                  disabled={queryLoading}
                >
                  What's the average value in the price column?
                </button>
                <button
                  onClick={() => setAiQuestion("Group by category and count rows")}
                  className="example-btn"
                  disabled={queryLoading}
                >
                  Group by category and count rows
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </ErrorBoundary>
  );
};

export default App;