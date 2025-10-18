// frontend/src/components/DataGrid.tsx
import React, { useState, useEffect } from 'react';
import { HotTable } from '@handsontable/react';
import { registerAllModules } from 'handsontable/registry';
import 'handsontable/dist/handsontable.full.min.css';
import axios from 'axios';

// Register all Handsontable modules
registerAllModules();

interface DataGridProps {
  datasetId: string;
  totalRows: number;
  apiBase: string;
  authHeaders: Record<string, string>;
}

export const DataGrid: React.FC<DataGridProps> = ({
  datasetId,
  totalRows,
  apiBase,
  authHeaders
}) => {
  const [data, setData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [strategy, setStrategy] = useState<'all' | 'paginated'>('all');

  useEffect(() => {
    loadData();
  }, [datasetId]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      console.log('Loading data for dataset:', datasetId);
      
      // Determine strategy
      const useStrategy = totalRows < 100000 ? 'all' : 'paginated';
      setStrategy(useStrategy);
      
      const limit = useStrategy === 'all' ? totalRows : 10000;
      
      const response = await axios.get(
        `${apiBase}/datasets/${datasetId}/data`,
        { 
          headers: authHeaders,
          params: { limit }
        }
      );
      
      console.log('Response:', response.data);
      
      let rowData = response.data.data || response.data;
      
      if (Array.isArray(rowData) && rowData.length > 0) {
        // Add extra empty rows to make it feel like a spreadsheet
        const extraRows = 50;
        const emptyRow = Object.keys(rowData[0]).reduce((acc, key) => {
          acc[key] = null;
          return acc;
        }, {} as any);
        
        const paddedData = [
          ...rowData,
          ...Array(extraRows).fill(null).map(() => ({ ...emptyRow }))
        ];
        
        setData(paddedData);
        setColumns(Object.keys(rowData[0]));
      } else {
        setError('No data returned from backend');
      }
      
    } catch (err: any) {
      console.error('Failed to load data:', err);
      setError(err.response?.data?.detail || err.message || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
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
        <div style={{ color: '#a1a1aa' }}>Loading {totalRows.toLocaleString()} rows...</div>
      </div>
    );
  }

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
          onClick={loadData}
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

  if (data.length === 0) {
    return (
      <div style={{ 
        padding: '40px', 
        textAlign: 'center', 
        color: '#a1a1aa',
        background: '#1a1a24',
        height: '100%'
      }}>
        No data available
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
      {/* Toolbar (Excel-like) */}
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
        <button style={{
          padding: '6px 12px',
          background: '#3d3d54',
          border: '1px solid #4d4d64',
          borderRadius: '4px',
          color: '#e4e4e7',
          fontSize: '12px',
          cursor: 'pointer'
        }}>
          🔢 Formula
        </button>
        <div style={{ flex: 1 }}></div>
        <div style={{ fontSize: '12px', color: '#a1a1aa' }}>
          {data.length - 50} rows loaded
        </div>
      </div>

      {/* Spreadsheet */}
      <div style={{ flex: 1, overflow: 'hidden' }}>
        <HotTable
          data={data}
          colHeaders={columns}
          rowHeaders={true}
          width="100%"
          height="100%"
          licenseKey={process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY || 'non-commercial-and-evaluation'}
          
          // Spreadsheet-like features
          columnSorting={true}
          filters={true}
          dropdownMenu={true}
          contextMenu={true}
          manualColumnResize={true}
          manualRowResize={true}
          manualColumnMove={true}
          manualRowMove={true}
          
          // Allow editing
          readOnly={false}
          
          // Excel-like settings
          fillHandle={true}
          autoWrapRow={true}
          autoWrapCol={true}
          enterMoves={{ row: 1, col: 0 }} // Enter moves down like Excel
          tabMoves={{ row: 0, col: 1 }}   // Tab moves right like Excel
          
          // Selection
          selectionMode="multiple"
          outsideClickDeselects={false}
          
          // Rendering
          renderAllRows={false}
          viewportRowRenderingOffset={50}
          
          // Styling
          className="jetdb-spreadsheet"
          stretchH="all"
          
          // Cell types
          columns={columns.map(col => ({
            data: col,
            type: 'text',
            allowEmpty: true
          }))}
        />
      </div>

      {strategy === 'paginated' && (
        <div style={{ 
          padding: '12px', 
          background: '#24243a', 
          borderTop: '1px solid #2d2d44', 
          color: '#a1a1aa', 
          fontSize: '13px',
          display: 'flex',
          alignItems: 'center',
          gap: '12px'
        }}>
          <span>📊 Showing first 10,000 rows of {totalRows.toLocaleString()}</span>
          <span style={{ color: '#6366f1' }}>• Use Query mode for full analysis</span>
        </div>
      )}
    </div>
  );
};