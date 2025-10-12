/**
 * JetDB Frontend v7.5.0 - Enhanced Infinite Scrolling & Polish
 * ===========================================================
 * NEW IN v7.5.0:
 * ✅ Better infinite scrolling (100k empty rows for true Excel feel)
 * ✅ Smooth transitions between pages
 * ✅ Keyboard shortcuts for navigation (PgUp/PgDown)
 * ✅ Loading overlay during page transitions
 * ✅ Better error handling for spreadsheet
 * ✅ Upload speed display
 * ✅ Parquet format indicator
 * ✅ Improved loading states
 */

import ErrorBoundary from './components/ErrorBoundary';
import React, { useState, useEffect, useCallback, useRef } from "react";
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

(window as any).$ = $;
(window as any).jQuery = $;

const supabase = createClient(
  process.env.REACT_APP_SUPABASE_URL || '',
  process.env.REACT_APP_SUPABASE_KEY || ''
);

const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:8000";
const VERSION = "7.5.0";

const MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024;
const SPREADSHEET_PAGE_SIZE = 10000;

// Enhanced padding for true Excel-like feel
const SPREADSHEET_PADDING = {
  EXTRA_COLUMNS: 100,    // Columns through CV (100 columns)
  EXTRA_ROWS: 100000,    // 100k empty rows for infinite feel
};

interface Dataset {
  id: string;
  filename: string;
  row_count: number | null;
  estimated_rows: number;
  column_count: number;
  columns: string[];
  uploaded_at: string;
  created_at?: string;
  status: "analyzing" | "ready" | "error";
  size_bytes: number;
  storage_format?: string;
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
  const [uploadSpeed, setUploadSpeed] = useState<string>("");
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [spreadsheetInitialized, setSpreadsheetInitialized] = useState<boolean>(false);
  const [showDatasetPicker, setShowDatasetPicker] = useState<boolean>(false);
  const [showSQLModal, setShowSQLModal] = useState<boolean>(false);
  const [showAIModal, setShowAIModal] = useState<boolean>(false);
  const [sqlQuery, setSqlQuery] = useState<string>("SELECT * FROM data LIMIT 100");
  const [aiQuestion, setAiQuestion] = useState<string>("");
  const [queryLoading, setQueryLoading] = useState<boolean>(false);
  
  // Loading states
  const [datasetsLoading, setDatasetsLoading] = useState<boolean>(false);
  const [datasetLoading, setDatasetLoading] = useState<boolean>(false);
  const [pageTransitioning, setPageTransitioning] = useState<boolean>(false);
  const [deletingDatasetId, setDeletingDatasetId] = useState<string | null>(null);

  // Pagination state
  const [currentPage, setCurrentPage] = useState<number>(0);
  const [totalRows, setTotalRows] = useState<number>(0);

  // Refs to prevent duplicate operations
  const loadingRef = useRef<boolean>(false);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const luckysheetInstanceRef = useRef<boolean>(false);

  // Check auth session on mount
  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setAuthLoading(false);
    });

    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
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
      toast("⚠️ JetDB works best on desktop for full spreadsheet experience", { duration: 8000 });
    }
  }, []);

  // Enhanced keyboard shortcuts
  useEffect(() => {
    const handleKeyPress = (e: KeyboardEvent) => {
      // Command/Ctrl shortcuts
      if (e.ctrlKey || e.metaKey) {
        if (e.key === 'k') { 
          e.preventDefault(); 
          if (selectedDataset && !queryLoading) setShowSQLModal(true);
        }
        if (e.key === 'j') { 
          e.preventDefault(); 
          if (selectedDataset && !queryLoading) setShowAIModal(true);
        }
        if (e.key === 'o') {
          e.preventDefault();
          setShowDatasetPicker(true);
        }
      }
      
      // Navigation shortcuts when spreadsheet is open
      if (selectedDataset && !datasetLoading && !pageTransitioning) {
        if (e.key === 'PageDown' && hasNext) {
          e.preventDefault();
          handleNextPage();
        }
        if (e.key === 'PageUp' && hasPrevious) {
          e.preventDefault();
          handlePreviousPage();
        }
      }
    };
    
    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [selectedDataset, queryLoading, datasetLoading]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current);
      }
      if (luckysheetInstanceRef.current) {
        try {
          luckysheet.destroy();
        } catch (e) {
          console.log("Luckysheet cleanup error:", e);
        }
      }
    };
  }, []);

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
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
    }
    
    await supabase.auth.signOut();
    setDatasets([]);
    setSelectedDataset(null);
    setSpreadsheetInitialized(false);
    toast.success("Signed out");
  };

  // Fetch datasets with proper polling management
  const fetchDatasets = useCallback(async (showLoading: boolean = true) => {
    if (!session) return;
    
    if (showLoading) setDatasetsLoading(true);
    
    try {
      const { data } = await axios.get(`${API_BASE}/datasets`, {
        headers: getAuthHeaders()
      });
      
      const newDatasets = data.datasets || [];
      setDatasets(newDatasets);
      
      // Check if we need to poll for analyzing datasets
      const analyzingDatasets = newDatasets.filter((d: Dataset) => d.status === "analyzing");
      
      if (analyzingDatasets.length > 0 && !pollingIntervalRef.current) {
        pollingIntervalRef.current = setInterval(async () => {
          try {
            const { data: pollData } = await axios.get(`${API_BASE}/datasets`, {
              headers: getAuthHeaders()
            });
            
            const updatedDatasets = pollData.datasets || [];
            setDatasets(updatedDatasets);
            
            const stillAnalyzing = updatedDatasets.filter((d: Dataset) => d.status === "analyzing");
            if (stillAnalyzing.length === 0 && pollingIntervalRef.current) {
              clearInterval(pollingIntervalRef.current);
              pollingIntervalRef.current = null;
            }
          } catch (error) {
            console.error("Polling error:", error);
          }
        }, 3000);
      } else if (analyzingDatasets.length === 0 && pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current);
        pollingIntervalRef.current = null;
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
  }, [session, getAuthHeaders]);

  // Initial fetch on session change
  useEffect(() => {
    if (session) {
      fetchDatasets();
    }
  }, [session, fetchDatasets]);

  // Enhanced file upload with speed tracking
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
    setUploadSpeed("");

    const formData = new FormData();
    formData.append("file", file);

    const startTime = Date.now();
    let lastTime = startTime;
    let lastLoaded = 0;

    try {
      const { data } = await axios.post(`${API_BASE}/upload`, formData, {
        headers: {
          ...getAuthHeaders(),
          "Content-Type": "multipart/form-data",
        },
        onUploadProgress: (progressEvent) => {
          const loaded = progressEvent.loaded || 0;
          const total = progressEvent.total || file.size;
          const progress = Math.round((loaded * 100) / total);
          setUploadProgress(progress);
          
          // Calculate instantaneous upload speed
          const currentTime = Date.now();
          const timeDiff = (currentTime - lastTime) / 1000; // seconds
          if (timeDiff > 0.5) { // Update speed every 0.5 seconds
            const bytesDiff = loaded - lastLoaded;
            const bytesPerSecond = bytesDiff / timeDiff;
            const mbPerSecond = bytesPerSecond / (1024 * 1024);
            setUploadSpeed(`${mbPerSecond.toFixed(1)} MB/s`);
            
            lastTime = currentTime;
            lastLoaded = loaded;
          }
        },
      });

      const uploadTime = (Date.now() - startTime) / 1000;
      const finalSpeed = (file.size / (1024 * 1024)) / uploadTime;

      toast.success(
        <div>
          <div>✅ Uploaded {file.name}!</div>
          <div style={{ fontSize: "12px", opacity: 0.8 }}>
            {data.storage_format === "parquet" ? "⚡ Converted to Parquet" : "📄 Stored as CSV"}
            {" • "}{finalSpeed.toFixed(1)} MB/s avg
            {" • "}{(file.size / (1024 * 1024)).toFixed(1)} MB
          </div>
        </div>,
        { duration: 5000 }
      );
      
      await fetchDatasets(false);
      setSelectedDataset(data.dataset_id);
      
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Upload failed");
    } finally {
      setUploading(false);
      setUploadProgress(0);
      setUploadSpeed("");
    }
  }, [session, getAuthHeaders, fetchDatasets]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'text/csv': ['.csv'] },
    multiple: false,
    disabled: uploading || !session,
  });

  // Enhanced spreadsheet loading with smooth transitions
  const loadDataIntoSpreadsheet = useCallback(async (datasetId: string, page: number = 0, isTransition: boolean = false) => {
    if (loadingRef.current || !session) {
      return;
    }

    loadingRef.current = true;
    
    if (isTransition) {
      setPageTransitioning(true);
    } else {
      setDatasetLoading(true);
    }
    
    const offset = page * SPREADSHEET_PAGE_SIZE;
    
    try {
      console.log(`📊 Loading dataset ${datasetId}, page ${page + 1}`);
      
      const { data } = await axios.get(
        `${API_BASE}/datasets/${datasetId}/rows?limit=${SPREADSHEET_PAGE_SIZE}&offset=${offset}`,
        { headers: getAuthHeaders() }
      );

      const rows = data.data;
      
      if (!rows || rows.length === 0) {
        if (page === 0) {
          toast.error("No data to display");
        } else {
          toast.error("No more data available");
        }
        return;
      }

      const dataset = datasets.find(d => d.id === datasetId);
      const total = dataset?.row_count || dataset?.estimated_rows || 0;
      setTotalRows(total);

      const columns = Object.keys(rows[0]);
      
      // Build Luckysheet data with enhanced padding
      const luckysheetData = [
        // Header row
        [
          ...columns.map((col: string) => ({
            v: col,
            m: col,
            ct: { fa: "General", t: "s" },
            bg: "#f0f0f0",
            bl: 1,
          })),
          ...Array(SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "s" },
            bg: "#f0f0f0",
          }))
        ],
        // Data rows
        ...rows.map((row: any) => [
          ...columns.map((col: string) => ({
            v: row[col],
            m: String(row[col] ?? ""),
            ct: { fa: "General", t: "g" },
          })),
          ...Array(SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "g" },
          }))
        ]),
        // Massive padding for Excel-like feel
        ...Array(SPREADSHEET_PADDING.EXTRA_ROWS).fill(null).map(() =>
          Array(columns.length + SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "g" },
          }))
        )
      ];

      // Smooth transition for page changes
      if (isTransition && luckysheetInstanceRef.current) {
        // Just update the data without destroying
        try {
          const sheetIndex = luckysheet.getSheet().index;
          luckysheet.setSheetData(luckysheetData, sheetIndex);
          luckysheet.refresh();
        } catch (e) {
          console.log("Sheet update error, doing full reload:", e);
          isTransition = false; // Fall back to full reload
        }
      }

      if (!isTransition) {
        // Clean up existing Luckysheet instance
        const container = document.getElementById("luckysheet-container");
        if (!container) {
          console.error("Luckysheet container not found");
          return;
        }

        // Destroy existing instance if any
        if (luckysheetInstanceRef.current) {
          try {
            luckysheet.destroy();
            luckysheetInstanceRef.current = false;
          } catch (e) {
            console.log("Luckysheet destroy error (expected):", e);
          }
        }

        // Clear container
        container.innerHTML = "";

        // Small delay to ensure DOM is ready
        await new Promise(resolve => setTimeout(resolve, 100));

        // Create new Luckysheet instance with enhanced options
        luckysheet.create({
          container: "luckysheet-container",
          showinfobar: false,
          showsheetbar: false,
          showstatisticBar: true,
          showstatisticBarConfig: {
            count: true,
            view: true,
            zoom: true
          },
          enableAddRow: false,
          enableAddCol: false,
          userInfo: false,
          showConfigWindowResize: false,
          forceCalculation: false,
          rowHeaderWidth: 50,
          columnHeaderHeight: 20,
          defaultColWidth: 100,
          defaultRowHeight: 20,
          data: [{
            name: `${dataset?.filename} - Page ${page + 1}`,
            color: "",
            status: 1,
            order: 0,
            data: luckysheetData,
            config: {
              merge: {},
              rowlen: {},
              columnlen: {},
              rowhidden: {},
              colhidden: {},
              borderInfo: []
            },
            index: 0,
          }],
          myFolderUrl: null,
          updateUrl: null,
          loadUrl: null,
          loadSheetUrl: null,
          allowUpdate: false,
          functionButton: false,
        });

        luckysheetInstanceRef.current = true;
        setSpreadsheetInitialized(true);
      }
      
      const startRow = offset + 1;
      const endRow = Math.min(offset + rows.length, total);
      
      // Show info toast with performance metrics
      if (data.query_time_seconds) {
        const rowsPerSec = (data.rows_per_second / 1000).toFixed(0);
        toast.success(
          <div>
            <div>Loaded rows {startRow.toLocaleString()}-{endRow.toLocaleString()} of {total.toLocaleString()}</div>
            <div style={{ fontSize: "12px", opacity: 0.8 }}>
              ⚡ {data.query_time_seconds}s • {rowsPerSec}k rows/s • {data.storage_format?.toUpperCase() || 'CSV'}
            </div>
          </div>,
          { duration: 3000 }
        );
      }

    } catch (error: any) {
      console.error("Load failed:", error);
      toast.error(error.response?.data?.detail || "Failed to load data");
    } finally {
      setDatasetLoading(false);
      setPageTransitioning(false);
      loadingRef.current = false;
    }
  }, [session, getAuthHeaders, datasets]);

  // Handle dataset selection change
  useEffect(() => {
    if (selectedDataset && !loadingRef.current) {
      setCurrentPage(0);
      loadDataIntoSpreadsheet(selectedDataset, 0);
    }
  }, [selectedDataset]);

  // Enhanced pagination handlers with smooth transitions
  const handlePreviousPage = async () => {
    if (currentPage > 0 && selectedDataset && !loadingRef.current && !pageTransitioning) {
      const newPage = currentPage - 1;
      setCurrentPage(newPage);
      await loadDataIntoSpreadsheet(selectedDataset, newPage, true);
    }
  };

  const handleNextPage = async () => {
    if (selectedDataset && (currentPage + 1) * SPREADSHEET_PAGE_SIZE < totalRows && !loadingRef.current && !pageTransitioning) {
      const newPage = currentPage + 1;
      setCurrentPage(newPage);
      await loadDataIntoSpreadsheet(selectedDataset, newPage, true);
    }
  };

  const handleGoToPage = async (page: number) => {
    if (selectedDataset && page >= 0 && !loadingRef.current && !pageTransitioning) {
      const maxPage = Math.ceil(totalRows / SPREADSHEET_PAGE_SIZE) - 1;
      const targetPage = Math.min(page, maxPage);
      
      if (targetPage !== currentPage) {
        setCurrentPage(targetPage);
        await loadDataIntoSpreadsheet(selectedDataset, targetPage, Math.abs(targetPage - currentPage) === 1);
      }
    }
  };

  // SQL Query execution
  const executeSQLQuery = async () => {
    if (!selectedDataset || !session || queryLoading) return;

    setQueryLoading(true);
    const loadingToast = toast.loading("🔍 Running SQL query...");
    
    try {
      const { data } = await axios.post(
        `${API_BASE}/query/sql`,
        { sql: sqlQuery, dataset_id: selectedDataset },
        { headers: getAuthHeaders() }
      );

      const rows = data.data;
      if (!rows || rows.length === 0) {
        toast.error("Query returned no results", { id: loadingToast });
        return;
      }

      const columns = Object.keys(rows[0]);
      const luckysheetData = [
        [
          ...columns.map((col: string) => ({
            v: col,
            m: col,
            ct: { fa: "General", t: "s" },
            bg: "#e3f2fd",
            bl: 1,
          })),
          ...Array(SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "s" },
            bg: "#e3f2fd",
          }))
        ],
        ...rows.map((row: any) => [
          ...columns.map((col: string) => ({
            v: row[col],
            m: String(row[col] ?? ""),
            ct: { fa: "General", t: "g" },
          })),
          ...Array(SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "g" },
          }))
        ]),
        ...Array(SPREADSHEET_PADDING.EXTRA_ROWS).fill(null).map(() =>
          Array(columns.length + SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "g" },
          }))
        )
      ];

      // Clean up and recreate spreadsheet
      const container = document.getElementById("luckysheet-container");
      if (container) {
        if (luckysheetInstanceRef.current) {
          try {
            luckysheet.destroy();
          } catch (e) {}
        }
        container.innerHTML = "";
        
        await new Promise(resolve => setTimeout(resolve, 100));

        luckysheet.create({
          container: "luckysheet-container",
          showinfobar: false,
          showsheetbar: false,
          showstatisticBar: true,
          enableAddRow: false,
          enableAddCol: false,
          userInfo: false,
          showConfigWindowResize: false,
          data: [{
            name: "SQL Query Results",
            color: "",
            status: 1,
            order: 0,
            data: luckysheetData,
            config: {},
            index: 0,
          }],
        });
      }

      const perfInfo = data.rows_per_second 
        ? ` • ${(data.rows_per_second / 1000000).toFixed(1)}M rows/s`
        : '';
      
      const storageInfo = data.storage_format 
        ? ` • ${data.storage_format.toUpperCase()}`
        : '';

      toast.success(
        <div>
          <div>✅ Query returned {data.rows_returned} rows</div>
          <div style={{ fontSize: "12px", opacity: 0.8 }}>
            ⚡ {data.execution_time_seconds}s{perfInfo}{storageInfo}
          </div>
        </div>,
        { id: loadingToast, duration: 5000 }
      );
      setShowSQLModal(false);
      
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Query failed", { id: loadingToast });
    } finally {
      setQueryLoading(false);
    }
  };

  // AI Query execution
  const executeAIQuery = async () => {
    if (!selectedDataset || !aiQuestion.trim() || !session || queryLoading) return;

    setQueryLoading(true);
    const loadingToast = toast.loading("🤖 AI is thinking...");
    
    try {
      const { data } = await axios.post(
        `${API_BASE}/query/natural`,
        { question: aiQuestion, dataset_id: selectedDataset },
        { headers: getAuthHeaders() }
      );

      const rows = data.data;
      if (!rows || rows.length === 0) {
        toast.error("Query returned no results", { id: loadingToast });
        return;
      }

      const columns = Object.keys(rows[0]);
      const luckysheetData = [
        [
          ...columns.map((col: string) => ({
            v: col,
            m: col,
            ct: { fa: "General", t: "s" },
            bg: "#f3e5f5",
            bl: 1,
          })),
          ...Array(SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "s" },
            bg: "#f3e5f5",
          }))
        ],
        ...rows.map((row: any) => [
          ...columns.map((col: string) => ({
            v: row[col],
            m: String(row[col] ?? ""),
            ct: { fa: "General", t: "g" },
          })),
          ...Array(SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "g" },
          }))
        ]),
        ...Array(SPREADSHEET_PADDING.EXTRA_ROWS).fill(null).map(() =>
          Array(columns.length + SPREADSHEET_PADDING.EXTRA_COLUMNS).fill(null).map(() => ({
            v: "",
            m: "",
            ct: { fa: "General", t: "g" },
          }))
        )
      ];

      // Clean up and recreate spreadsheet
      const container = document.getElementById("luckysheet-container");
      if (container) {
        if (luckysheetInstanceRef.current) {
          try {
            luckysheet.destroy();
          } catch (e) {}
        }
        container.innerHTML = "";
        
        await new Promise(resolve => setTimeout(resolve, 100));

        luckysheet.create({
          container: "luckysheet-container",
          showinfobar: false,
          showsheetbar: false,
          showstatisticBar: true,
          enableAddRow: false,
          enableAddCol: false,
          userInfo: false,
          showConfigWindowResize: false,
          data: [{
            name: "AI Query Results",
            color: "",
            status: 1,
            order: 0,
            data: luckysheetData,
            config: {},
            index: 0,
          }],
        });
      }

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
      
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "AI query failed", { id: loadingToast });
    } finally {
      setQueryLoading(false);
    }
  };

  // Delete dataset
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
      await fetchDatasets(false);
      
      if (selectedDataset === datasetId) {
        setSelectedDataset(null);
        setSpreadsheetInitialized(false);
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

  // Pagination calculations
  const totalPages = Math.ceil(totalRows / SPREADSHEET_PAGE_SIZE);
  const startRow = currentPage * SPREADSHEET_PAGE_SIZE + 1;
  const endRow = Math.min((currentPage + 1) * SPREADSHEET_PAGE_SIZE, totalRows);
  const hasPrevious = currentPage > 0;
  const hasNext = (currentPage + 1) * SPREADSHEET_PAGE_SIZE < totalRows;

  // Auth loading screen
  if (authLoading) {
    return (
      <ErrorBoundary>
        <div className="auth-container">
          <div className="auth-box">
            <div className="auth-loader">
              <div className="spinner" style={{ margin: "0 auto" }}></div>
              <p style={{ marginTop: "16px" }}>Loading JetDB...</p>
            </div>
          </div>
        </div>
      </ErrorBoundary>
    );
  }

  // Auth screen
  if (!session) {
    return (
      <ErrorBoundary>
        <div className="auth-container">
          <Toaster position="top-right" />
          <div className="auth-box">
            <div className="auth-header">
              <h1>JetDB</h1>
              <p>Big data for the rest of us</p>
              <div className="version-badge">v{VERSION} • Enhanced</div>
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
              <div className="feature-item">🤖 Ask questions in plain English</div>
              <div className="feature-item">🚀 5-10x faster with Parquet format</div>
              <div className="feature-item">☁️ Secure cloud storage</div>
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

        <header className="header">
          <div className="header-left">
            <h1 className="logo">⚡ JetDB</h1>
            <span className="version">v{VERSION}</span>
            {currentDataset && (
              <span className="current-file">
                {currentDataset.filename}
                {currentDataset.storage_format === "parquet" && (
                  <span className="row-count" style={{ background: "#10b981" }}>
                    ⚡ Parquet
                  </span>
                )}
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
              </span>
            )}
          </div>
          
          <div className="header-right">
            <button 
              onClick={() => setShowDatasetPicker(true)} 
              className="btn-secondary"
              disabled={datasetsLoading}
              title="Ctrl+O"
            >
              📁 Datasets ({datasets.length})
            </button>
            {selectedDataset && (
              <>
                <button 
                  onClick={() => setShowSQLModal(true)} 
                  className="btn-secondary" 
                  title="Ctrl+K"
                  disabled={queryLoading || datasetLoading || pageTransitioning}
                >
                  💻 SQL
                </button>
                <button 
                  onClick={() => setShowAIModal(true)} 
                  className="btn-primary" 
                  title="Ctrl+J"
                  disabled={queryLoading || datasetLoading || pageTransitioning}
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
                  {uploadSpeed && <p style={{ fontSize: "14px", opacity: 0.8 }}>{uploadSpeed}</p>}
                  <div className="progress-bar">
                    <div className="progress-fill" style={{ width: `${uploadProgress}%` }}></div>
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
                  <div style={{ marginTop: "20px", fontSize: "12px", opacity: 0.7 }}>
                    Files over 100MB are automatically converted to Parquet for 5-10x faster queries
                  </div>
                </>
              )}
            </div>
          ) : (
            <>
              {(datasetLoading || pageTransitioning) && (
                <div style={{ 
                  position: "absolute",
                  top: 0,
                  left: 0,
                  right: 0,
                  bottom: 0,
                  display: "flex", 
                  flexDirection: "column", 
                  alignItems: "center", 
                  justifyContent: "center", 
                  background: pageTransitioning ? "rgba(26, 26, 36, 0.5)" : "rgba(26, 26, 36, 0.9)",
                  backdropFilter: "blur(4px)",
                  zIndex: 1000,
                  gap: "20px"
                }}>
                  <div className="spinner"></div>
                  <p style={{ fontSize: "18px", color: "#fff" }}>
                    {pageTransitioning ? "Loading page..." : "Loading dataset..."}
                  </p>
                </div>
              )}
              
              <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
                <div 
                  id="luckysheet-container" 
                  className="spreadsheet-container"
                  style={{ 
                    flex: 1,
                    width: '100%',
                    opacity: (datasetLoading || pageTransitioning) ? 0.5 : 1,
                    transition: "opacity 0.2s ease"
                  }}
                ></div>

                {!datasetLoading && totalRows > SPREADSHEET_PAGE_SIZE && (
                  <div className="pagination-container">
                    <div className="pagination-info">
                      Showing rows {startRow.toLocaleString()}-{endRow.toLocaleString()} of {totalRows.toLocaleString()}
                      <span style={{ marginLeft: '12px', opacity: 0.7 }}>
                        (Page {currentPage + 1} of {totalPages})
                      </span>
                    </div>
                    
                    <div className="pagination-controls">
                      <button
                        onClick={handlePreviousPage}
                        disabled={!hasPrevious || datasetLoading || pageTransitioning || loadingRef.current}
                        className="btn-pagination"
                        title="PageUp"
                      >
                        ← Previous
                      </button>
                      
                      <div className="pagination-jump">
                        <input
                          type="number"
                          min="1"
                          max={totalPages}
                          value={currentPage + 1}
                          onChange={(e) => {
                            const page = parseInt(e.target.value) - 1;
                            if (!isNaN(page)) {
                              handleGoToPage(page);
                            }
                          }}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') {
                              e.currentTarget.blur();
                            }
                          }}
                          className="page-input"
                          disabled={datasetLoading || pageTransitioning || loadingRef.current}
                        />
                        <span>of {totalPages}</span>
                      </div>
                      
                      <button
                        onClick={handleNextPage}
                        disabled={!hasNext || datasetLoading || pageTransitioning || loadingRef.current}
                        className="btn-pagination"
                        title="PageDown"
                      >
                        Next →
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </>
          )}
        </div>

        {/* Dataset Picker Modal */}
        {showDatasetPicker && (
          <div className="modal-overlay" onClick={() => setShowDatasetPicker(false)}>
            <div className="modal" onClick={(e) => e.stopPropagation()}>
              <div className="modal-header">
                <h2>📁 Your Datasets</h2>
                <button onClick={() => setShowDatasetPicker(false)} className="modal-close">✕</button>
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
                      className={`dataset-item ${selectedDataset === dataset.id ? "dataset-item-selected" : ""}`}
                    >
                      <div className="dataset-info" onClick={() => {
                        setSelectedDataset(dataset.id);
                        setShowDatasetPicker(false);
                        setCurrentPage(0); // Reset to first page
                      }}>
                        <div className="dataset-name">
                          {dataset.filename}
                          {dataset.storage_format === "parquet" && (
                            <span style={{ 
                              marginLeft: "8px", 
                              fontSize: "11px", 
                              background: "#10b981", 
                              color: "white", 
                              padding: "2px 6px", 
                              borderRadius: "4px" 
                            }}>
                              Parquet
                            </span>
                          )}
                        </div>
                        <div className="dataset-meta">
                          {dataset.status === "ready" && dataset.row_count ? (
                            <>
                              {(dataset.row_count || 0).toLocaleString()} rows • {" "}
                              {dataset.column_count} columns • {" "}
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
                          {new Date(dataset.uploaded_at || dataset.created_at || '').toLocaleString()}
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
                <button onClick={() => setShowSQLModal(false)} className="modal-close" disabled={queryLoading}>✕</button>
              </div>

              <textarea
                value={sqlQuery}
                onChange={(e) => setSqlQuery(e.target.value)}
                className="sql-editor"
                placeholder="SELECT * FROM data LIMIT 100"
                rows={10}
                disabled={queryLoading}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
                    e.preventDefault();
                    executeSQLQuery();
                  }
                }}
              />

              <div className="modal-footer">
                <button onClick={() => setShowSQLModal(false)} className="btn-secondary" disabled={queryLoading}>
                  Cancel
                </button>
                <button onClick={executeSQLQuery} className="btn-primary" disabled={queryLoading || !sqlQuery.trim()}>
                  {queryLoading ? "Running..." : "Run Query (Ctrl+Enter)"}
                </button>
              </div>

              <div className="modal-hint">
                💡 Use 'data' as the table name. Only SELECT queries allowed. Press Ctrl+Enter to run.
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
                <button onClick={() => setShowAIModal(false)} className="modal-close" disabled={queryLoading}>✕</button>
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
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
                    e.preventDefault();
                    executeAIQuery();
                  }
                }}
              />

              <div className="modal-footer">
                <button onClick={() => setShowAIModal(false)} className="btn-secondary" disabled={queryLoading}>
                  Cancel
                </button>
                <button onClick={executeAIQuery} className="btn-primary" disabled={queryLoading || !aiQuestion.trim()}>
                  {queryLoading ? "Thinking..." : "Ask AI (Ctrl+Enter)"}
                </button>
              </div>

              <div className="modal-examples">
                <div className="examples-title">Example questions:</div>
                <button onClick={() => setAiQuestion("Show me the top 10 rows by revenue")} className="example-btn" disabled={queryLoading}>
                  Show me the top 10 rows by revenue
                </button>
                <button onClick={() => setAiQuestion("What's the average value in the price column?")} className="example-btn" disabled={queryLoading}>
                  What's the average value in the price column?
                </button>
                <button onClick={() => setAiQuestion("Group by category and count rows")} className="example-btn" disabled={queryLoading}>
                  Group by category and count rows
                </button>
                <button onClick={() => setAiQuestion("Find all records where amount > 1000 and status = 'completed'")} className="example-btn" disabled={queryLoading}>
                  Find high-value completed transactions
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