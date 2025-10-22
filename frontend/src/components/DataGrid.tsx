import React, { useState, useEffect, useRef, useCallback } from 'react';
import { HotTable } from '@handsontable/react';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/dist/handsontable.full.min.css';
import axios from 'axios';

registerAllModules();

interface DataGridProps {
  datasetId: string;
  totalRows: number;
  apiBase: string;
  authHeaders: Record<string, string>;
}

interface DataChunk {
  startRow: number;
  endRow: number;
  data: any[];
  timestamp: number;
}

// ✅ TASK 3: Performance configuration
const CHUNK_SIZE = 10000;
const MAX_CACHED_CHUNKS = 20;
const PREFETCH_THRESHOLD = 0.7;
const EMPTY_ROW_PADDING = 100;

export const DataGrid: React.FC<DataGridProps> = ({
  datasetId,
  totalRows,
  apiBase,
  authHeaders
}) => {
  const [columns, setColumns] = useState<string[]>([]);
  const [displayData, setDisplayData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [loadedChunks, setLoadedChunks] = useState<Map<number, DataChunk>>(new Map());
  const [currentViewport, setCurrentViewport] = useState({ start: 0, end: CHUNK_SIZE });
  
  const hotRef = useRef<any>(null);
  const isLoadingChunk = useRef(false);

  // ✅ Load initial chunk
  useEffect(() => {
    loadChunk(0);
  }, [datasetId]);

  // ✅ FIXED: Prefetch when scrolling with proper null checks
  const handleAfterScrollVertically = useCallback(() => {
    // Guard against undefined ref
    if (!hotRef.current) return;
    
    const hot = hotRef.current.hotInstance;
    if (!hot) return;
    
    // Guard against undefined view/wtTable
    if (!hot.view || !hot.view.wt || !hot.view.wt.wtTable) {
      console.warn('⚠️ Handsontable not fully initialized yet');
      return;
    }

    try {
      const viewportStart = hot.view.wt.wtTable.getFirstVisibleRow();
      const viewportEnd = hot.view.wt.wtTable.getLastVisibleRow();
      
      // Additional guard - make sure we got valid numbers
      if (typeof viewportStart !== 'number' || typeof viewportEnd !== 'number') {
        return;
      }
      
      setCurrentViewport({ start: viewportStart, end: viewportEnd });

      // Prefetch next chunk if close to end
      const scrollProgress = viewportEnd / totalRows;
      if (scrollProgress > PREFETCH_THRESHOLD) {
        const nextChunkIndex = Math.floor(viewportEnd / CHUNK_SIZE) + 1;
        const nextChunkStart = nextChunkIndex * CHUNK_SIZE;
        
        if (nextChunkStart < totalRows && !loadedChunks.has(nextChunkIndex)) {
          console.log('🔮 Prefetching chunk:', nextChunkIndex);
          loadChunk(nextChunkStart);
        }
      }
    } catch (err) {
      console.warn('⚠️ Error in scroll handler:', err);
      // Don't throw - just log and continue
    }
  }, [totalRows, loadedChunks]);

  // ✅ Load chunk from backend
  const loadChunk = async (startRow: number) => {
    if (isLoadingChunk.current) return;
    
    const chunkIndex = Math.floor(startRow / CHUNK_SIZE);
    
    // Already loaded?
    if (loadedChunks.has(chunkIndex)) {
      console.log('✓ Chunk', chunkIndex, 'already loaded');
      return;
    }

    isLoadingChunk.current = true;
    setLoading(true);

    try {
      console.log(`📦 Loading chunk ${chunkIndex} (rows ${startRow}-${startRow + CHUNK_SIZE})`);
      
      const response = await axios.get(
        `${apiBase}/datasets/${datasetId}/data`,
        { 
          headers: authHeaders,
          params: { 
            limit: CHUNK_SIZE, 
            offset: startRow 
          }
        }
      );
      
      const chunkData = response.data.data || response.data;
      
      if (!columns.length && chunkData.length > 0) {
        setColumns(Object.keys(chunkData[0]));
      }

      // Create chunk
      const chunk: DataChunk = {
        startRow,
        endRow: startRow + chunkData.length,
        data: chunkData,
        timestamp: Date.now()
      };

      // Update cache with LRU eviction
      const newChunks = new Map(loadedChunks);
      
      // Evict oldest if cache full
      if (newChunks.size >= MAX_CACHED_CHUNKS) {
        const oldestChunk = Array.from(newChunks.entries())
          .sort((a, b) => a[1].timestamp - b[1].timestamp)[0];
        newChunks.delete(oldestChunk[0]);
        console.log('🗑️ Evicted chunk', oldestChunk[0]);
      }
      
      newChunks.set(chunkIndex, chunk);
      setLoadedChunks(newChunks);

      // Rebuild display data
      rebuildDisplayData(newChunks);
      
      console.log(`✅ Loaded chunk ${chunkIndex}: ${chunkData.length} rows`);
      
    } catch (err: any) {
      console.error('❌ Failed to load chunk:', err);
      setError(err.response?.data?.detail || 'Failed to load data');
    } finally {
      isLoadingChunk.current = false;
      setLoading(false);
    }
  };

  // ✅ Rebuild display data from chunks
  const rebuildDisplayData = (chunks: Map<number, DataChunk>) => {
    if (chunks.size === 0) return;

    // Sort chunks by start row
    const sortedChunks = Array.from(chunks.values())
      .sort((a, b) => a.startRow - b.startRow);

    // Merge all chunk data
    let mergedData: any[] = [];
    
    for (const chunk of sortedChunks) {
      mergedData = mergedData.concat(chunk.data);
    }

    // Add empty rows for smooth scrolling
    if (mergedData.length > 0) {
      const emptyRow = Object.keys(mergedData[0]).reduce((acc, key) => {
        acc[key] = null;
        return acc;
      }, {} as any);

      const paddedData = [
        ...mergedData,
        ...Array(EMPTY_ROW_PADDING).fill(null).map(() => ({ ...emptyRow }))
      ];

      setDisplayData(paddedData);
    }
  };

  // Loading state
  if (loading && displayData.length === 0) {
    return (
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center', 
        height: '100%',
        flexDirection: 'column',
        gap: '16px',
        background: '#1a1a24'
      }}>
        <div className="spinner"></div>
        <div style={{ color: '#a1a1aa' }}>
          Loading first {CHUNK_SIZE.toLocaleString()} rows...
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div style={{ 
        padding: '40px', 
        textAlign: 'center',
        color: '#ef4444',
        background: '#1a1a24',
        height: '100%'
      }}>
        <h3>Error loading data</h3>
        <p style={{ marginTop: '8px', color: '#a1a1aa' }}>{error}</p>
        <button 
          onClick={() => {
            setError(null);
            loadChunk(0);
          }}
          style={{
            marginTop: '16px',
            padding: '10px 20px',
            background: '#6366f1',
            border: 'none',
            borderRadius: '8px',
            color: 'white',
            cursor: 'pointer'
          }}
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div style={{ 
      height: '100%', 
      width: '100%', 
      display: 'flex', 
      flexDirection: 'column',
      background: '#1a1a24'
    }}>
      {/* Toolbar */}
      <div style={{
        padding: '8px 16px',
        background: '#24243a',
        borderBottom: '1px solid #2d2d44',
        display: 'flex',
        gap: '8px',
        alignItems: 'center'
      }}>
        <button style={{
          padding: '6px 12px',
          background: '#3d3d54',
          border: '1px solid #4d4d64',
          borderRadius: '4px',
          color: '#e4e4e7',
          fontSize: '12px',
          cursor: 'pointer'
        }}>
          ✨ Format
        </button>
        <button style={{
          padding: '6px 12px',
          background: '#3d3d54',
          border: '1px solid #4d4d64',
          borderRadius: '4px',
          color: '#e4e4e7',
          fontSize: '12px',
          cursor: 'pointer'
        }}>
          ➕ Insert
        </button>
        <div style={{ flex: 1 }}></div>
        
        {/* ✅ TASK 4: Subtle loading indicator */}
        <div style={{ fontSize: '12px', color: '#a1a1aa', display: 'flex', alignItems: 'center', gap: '8px' }}>
          {isLoadingChunk.current && (
            <div className="spinner" style={{ width: '12px', height: '12px', borderWidth: '2px' }}></div>
          )}
          {loadedChunks.size * CHUNK_SIZE >= totalRows ? (
            <span>{totalRows.toLocaleString()} rows loaded</span>
          ) : (
            <span>{(loadedChunks.size * CHUNK_SIZE).toLocaleString()} / {totalRows.toLocaleString()} rows</span>
          )}
        </div>
      </div>

      {/* Spreadsheet */}
      <div style={{ flex: 1, overflow: 'hidden' }}>
        <HotTable
          ref={hotRef}
          data={displayData}
          colHeaders={columns}
          rowHeaders={true}
          width="100%"
          height="100%"
          licenseKey={process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY || 'non-commercial-and-evaluation'}
          
          // Features
          columnSorting={true}
          filters={true}
          dropdownMenu={true}
          contextMenu={true}
          manualColumnResize={true}
          manualRowResize={true}
          
          // Performance
          renderAllRows={false}
          viewportRowRenderingOffset={50}
          
          // Callbacks - FIXED with proper error handling
          afterScrollVertically={handleAfterScrollVertically}
          
          // Excel-like
          fillHandle={true}
          enterMoves={{ row: 1, col: 0 }}
          tabMoves={{ row: 0, col: 1 }}
          
          // Styling
          className="jetdb-spreadsheet"
          stretchH="all"
          
          columns={columns.map(col => ({
            data: col,
            type: 'text',
            allowEmpty: true
          }))}
        />
      </div>
    </div>
  );
};
