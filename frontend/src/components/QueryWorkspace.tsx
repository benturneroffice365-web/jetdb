#10. frontend/src/components/QueryWorkspace.tsx - NEW
tsximport React, { useState } from 'react';
import axios from 'axios';
import { HotTable } from '@handsontable/react';
import toast from 'react-hot-toast';

interface Tab {
  id: string;
  title: string;
  type: 'sql' | 'ai';
  query: string;
  results: any[];
  columns: string[];
}

interface QueryWorkspaceProps {
  datasetId: string;
  apiBase: string;
  authHeaders: Record<string, string>;
}

export const QueryWorkspace: React.FC<QueryWorkspaceProps> = ({
  datasetId,
  apiBase,
  authHeaders
}) => {
  const [tabs, setTabs] = useState<Tab[]>([]);
  const [activeTabId, setActiveTabId] = useState<string | null>(null);
  const [sqlQuery, setSqlQuery] = useState('SELECT * FROM data LIMIT 100');
  const [aiQuery, setAiQuery] = useState('');
  const [loading, setLoading] = useState(false);

  const executeSQL = async () => {
    setLoading(true);
    try {
      const { data } = await axios.post(
        `${apiBase}/query/sql`,
        { sql: sqlQuery, dataset_id: datasetId },
        { headers: authHeaders }
      );

      const newTab: Tab = {
        id: `sql-${Date.now()}`,
        title: `SQL: ${sqlQuery.substring(0, 30)}...`,
        type: 'sql',
        query: sqlQuery,
        results: data.data,
        columns: data.columns
      };

      setTabs([...tabs, newTab]);
      setActiveTabId(newTab.id);
      toast.success(`${data.rows_returned} rows in ${data.execution_time_seconds}s`);
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Query failed');
    } finally {
      setLoading(false);
    }
  };

  const executeAI = async () => {
    setLoading(true);
    try {
      const { data } = await axios.post(
        `${apiBase}/query/natural`,
        { question: aiQuery, dataset_id: datasetId },
        { headers: authHeaders }
      );

      const newTab: Tab = {
        id: `ai-${Date.now()}`,
        title: `AI: ${aiQuery.substring(0, 30)}...`,
        type: 'ai',
        query: aiQuery,
        results: data.data,
        columns: data.columns
      };

      setTabs([...tabs, newTab]);
      setActiveTabId(newTab.id);
      toast.success(`Generated SQL: ${data.sql_query}`);
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'AI query failed');
    } finally {
      setLoading(false);
    }
  };

  const closeTab = (tabId: string) => {
    const newTabs = tabs.filter(t => t.id !== tabId);
    setTabs(newTabs);
    if (activeTabId === tabId) {
      setActiveTabId(newTabs.length > 0 ? newTabs[0].id : null);
    }
  };

  const activeTab = tabs.find(t => t.id === activeTabId);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ padding: '16px', background: '#1a1a24', borderBottom: '1px solid #2d2d44' }}>
        <div style={{ display: 'flex', gap: '12px', marginBottom: '12px' }}>
          <input
            type="text"
            placeholder="Ask AI: 'Show top 10 by revenue'"
            value={aiQuery}
            onChange={(e) => setAiQuery(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && executeAI()}
            style={{ flex: 1, padding: '10px', borderRadius: '6px', border: '1px solid #2d2d44', background: '#24243a', color: '#fff' }}
            disabled={loading}
          />
          <button onClick={executeAI} disabled={loading || !aiQuery} style={{ padding: '10px 20px', background: '#6366f1', color: '#fff', border: 'none', borderRadius: '6px', fontWeight: 600, cursor: 'pointer' }}>
            ðŸ¤– Ask AI
          </button>
        </div>
        <div style={{ display: 'flex', gap: '12px' }}>
          <input
            type="text"
            placeholder="SQL: SELECT * FROM data WHERE revenue > 1000"
            value={sqlQuery}
            onChange={(e) => setSqlQuery(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && executeSQL()}
            style={{ flex: 1, padding: '10px', borderRadius: '6px', border: '1px solid #2d2d44', background: '#24243a', color: '#fff', fontFamily: 'monospace' }}
            disabled={loading}
          />
          <button onClick={executeSQL} disabled={loading || !sqlQuery} style={{ padding: '10px 20px', background: '#10b981', color: '#fff', border: 'none', borderRadius: '6px', fontWeight: 600, cursor: 'pointer' }}>
            ðŸ’» Run SQL
          </button>
        </div>
      </div>

      {tabs.length > 0 && (
        <>
          <div style={{ display: 'flex', gap: '4px', padding: '8px 16px', background: '#24243a', borderBottom: '1px solid #2d2d44', overflowX: 'auto' }}>
            {tabs.map(tab => (
              <div
                key={tab.id}
                onClick={() => setActiveTabId(tab.id)}
                style={{
                  padding: '8px 16px',
                  borderRadius: '6px',
                  background: activeTabId === tab.id ? '#6366f1' : '#1a1a24',
                  color: '#fff',
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  fontSize: '13px',
                  whiteSpace: 'nowrap'
                }}
              >
                <span>{tab.type === 'sql' ? 'ðŸ’»' : 'ðŸ¤–'}</span>
                <span>{tab.title}</span>
                <button
                  onClick={(e) => { e.stopPropagation(); closeTab(tab.id); }}
                  style={{ background: 'none', border: 'none', color: '#fff', cursor: 'pointer', fontSize: '16px' }}
                >
                  Ã—
                </button>
              </div>
            ))}
          </div>

          {activeTab && (
            <div style={{ flex: 1, overflow: 'hidden' }}>
              <HotTable
                data={activeTab.results}
                colHeaders={activeTab.columns}
                rowHeaders={true}
                width="100%"
                height="100%"
                licenseKey={process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY || 'non-commercial-and-evaluation'}
                columnSorting={true}
                filters={true}
              />
            </div>
          )}
        </>
      )}
    </div>
  );
};
