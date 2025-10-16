#9. frontend/src/components/DataGrid.tsx - NEW
tsximport React, { useState, useEffect } from 'react';
import { HotTable } from '@handsontable/react';
import 'handsontable/dist/handsontable.full.min.css';
import axios from 'axios';

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
  const [strategy, setStrategy] = useState<'all' | 'paginated'>('all');

  useEffect(() => {
    // Determine strategy based on size
    if (totalRows < 100000) {
      setStrategy('all');
    } else {
      setStrategy('paginated');
    }
    loadData();
  }, [datasetId, totalRows]);

  const loadData = async () => {
    setLoading(true);
    try {
      const limit = strategy === 'all' ? totalRows : 10000;
      const { data: response } = await axios.get(
        `${apiBase}/datasets/${datasetId}/rows?limit=${limit}`,
        { headers: authHeaders }
      );
      
      setData(response.data);
      if (response.data.length > 0) {
        setColumns(Object.keys(response.data[0]));
      }
    } catch (error) {
      console.error('Failed to load data:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return <div style={{ padding: '40px', textAlign: 'center' }}>Loading...</div>;
  }

  return (
    <div style={{ height: '100%', width: '100%' }}>
      <HotTable
        data={data}
        colHeaders={columns}
        rowHeaders={true}
        width="100%"
        height="100%"
        licenseKey={process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY || 'non-commercial-and-evaluation'}
        columnSorting={true}
        filters={true}
        dropdownMenu={true}
        contextMenu={true}
        manualColumnResize={true}
        autoWrapRow={true}
        autoWrapCol={true}
      />
      {strategy === 'paginated' && (
        <div style={{ padding: '12px', background: '#1a1a24', borderTop: '1px solid #2d2d44', color: '#a1a1aa', fontSize: '13px' }}>
          Showing first 10,000 rows of {totalRows.toLocaleString()} â€¢ Use SQL/AI queries for full analysis
        </div>
      )}
    </div>
  );
};
