import React, { useState } from 'react';
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
      toast.success(`✅ ${data.rows_returned} rows in ${data.execution_time_seconds}s`);
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
      toast.success(`✅ Generated SQL: ${data.sql_query}`);
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
      {/* Query Input Area */}
      <div style={{ padding: '16px', background: '#1a1a24', borderBottom: '1px solid #2d2d44' }}>
        {/* AI Query Row */}
        <div style={{ display: 'flex', gap: '12px', marginBottom: '12px' }}>
          <input
            type="text"
            placeholder="🤖 Ask JetAI: 'Show top 10 by revenue'"
            value={aiQuery}
            onChange={(e) => setAiQuery(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !loading && aiQuery && executeAI()}
            style={{ 
              flex: 1, 
              padding: '12px 16px', 
              borderRadius: '8px', 
              border: '1px solid #2d2d44', 
              background: '#24243a', 
              color: '#fff',
              fontSize: '14px',
              outline: 'none',
              transition: 'border-color 0.2s'
            }}
            disabled={loading}
            onFocus={(e) => e.target.style.borderColor = '#6366f1'}
            onBlur={(e) => e.target.style.borderColor = '#2d2d44'}
          />
          <button 
            onClick={executeAI} 
            disabled={loading || !aiQuery} 
            style={{ 
              padding: '12px 24px',
              background: (loading || !aiQuery) ? '#3d3d54' : 'linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%)',
              color: '#fff', 
              border: 'none', 
              borderRadius: '8px', 
              fontWeight: 600,
              fontSize: '14px',
              cursor: (loading || !aiQuery) ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              transition: 'all 0.2s',
              minWidth: '120px',
              justifyContent: 'center'
            }}
            onMouseOver={(e) => {
              if (!loading && aiQuery) {
                e.currentTarget.style.transform = 'translateY(-2px)';
                e.currentTarget.style.boxShadow = '0 8px 20px rgba(99, 102, 241, 0.4)';
              }
            }}
            onMouseOut={(e) => {
              e.currentTarget.style.transform = 'translateY(0)';
              e.currentTarget.style.boxShadow = 'none';
            }}
          >
            <span style={{ fontSize: '16px' }}>🤖</span>
            {loading ? 'Running...' : 'JetAI'}
          </button>
        </div>

        {/* SQL Query Row */}
        <div style={{ display: 'flex', gap: '12px' }}>
          <input
            type="text"
            placeholder="💻 Execute SQL: SELECT * FROM data WHERE revenue > 1000"
            value={sqlQuery}
            onChange={(e) => setSqlQuery(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !loading && sqlQuery && executeSQL()}
            style={{ 
              flex: 1, 
              padding: '12px 16px', 
              borderRadius: '8px', 
              border: '1px solid #2d2d44', 
              background: '#24243a', 
              color: '#fff', 
              fontFamily: "'Consolas', 'Monaco', 'Courier New', monospace",
              fontSize: '13px',
              outline: 'none',
              transition: 'border-color 0.2s'
            }}
            disabled={loading}
            onFocus={(e) => e.target.style.borderColor = '#10b981'}
            onBlur={(e) => e.target.style.borderColor = '#2d2d44'}
          />
          <button 
            onClick={executeSQL} 
            disabled={loading || !sqlQuery} 
            style={{ 
              padding: '12px 24px',
              background: (loading || !sqlQuery) ? '#3d3d54' : 'linear-gradient(135deg, #10b981 0%, #059669 100%)',
              color: '#fff', 
              border: 'none', 
              borderRadius: '8px', 
              fontWeight: 600,
              fontSize: '14px',
              cursor: (loading || !sqlQuery) ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              transition: 'all 0.2s',
              minWidth: '150px',
              justifyContent: 'center'
            }}
            onMouseOver={(e) => {
              if (!loading && sqlQuery) {
                e.currentTarget.style.transform = 'translateY(-2px)';
                e.currentTarget.style.boxShadow = '0 8px 20px rgba(16, 185, 129, 0.4)';
              }
            }}
            onMouseOut={(e) => {
              e.currentTarget.style.transform = 'translateY(0)';
              e.currentTarget.style.boxShadow = 'none';
            }}
          >
            <span style={{ fontSize: '16px' }}>▶️</span>
            {loading ? 'Executing...' : 'Execute SQL'}
          </button>
        </div>
      </div>

      {/* Tab Bar */}
      {tabs.length > 0 && (
        <>
          <div style={{ 
            display: 'flex', 
            gap: '4px', 
            padding: '8px 16px', 
            background: '#24243a', 
            borderBottom: '1px solid #2d2d44', 
            overflowX: 'auto',
            scrollbarWidth: 'thin',
            scrollbarColor: '#3d3d54 #24243a'
          }}>
            {tabs.map(tab => (
              <div
                key={tab.id}
                onClick={() => setActiveTabId(tab.id)}
                style={{
                  padding: '10px 16px',
                  borderRadius: '6px 6px 0 0',
                  background: activeTabId === tab.id ? '#6366f1' : '#1a1a24',
                  color: '#fff',
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  fontSize: '13px',
                  whiteSpace: 'nowrap',
                  transition: 'all 0.2s',
                  border: activeTabId === tab.id ? '1px solid #6366f1' : '1px solid #2d2d44',
                  borderBottom: 'none'
                }}
                onMouseOver={(e) => {
                  if (activeTabId !== tab.id) {
                    e.currentTarget.style.background = '#2d2d44';
                  }
                }}
                onMouseOut={(e) => {
                  if (activeTabId !== tab.id) {
                    e.currentTarget.style.background = '#1a1a24';
                  }
                }}
              >
                <span>{tab.type === 'sql' ? '💻' : '🤖'}</span>
                <span style={{ fontWeight: activeTabId === tab.id ? 600 : 400 }}>
                  {tab.title}
                </span>
                <button
                  onClick={(e) => { 
                    e.stopPropagation(); 
                    closeTab(tab.id); 
                  }}
                  style={{ 
                    background: 'none', 
                    border: 'none', 
                    color: '#fff', 
                    cursor: 'pointer', 
                    fontSize: '18px',
                    padding: '0 4px',
                    marginLeft: '4px',
                    opacity: 0.7,
                    transition: 'opacity 0.2s'
                  }}
                  onMouseOver={(e) => e.currentTarget.style.opacity = '1'}
                  onMouseOut={(e) => e.currentTarget.style.opacity = '0.7'}
                >
                  ×
                </button>
              </div>
            ))}
          </div>

          {/* Tab Content */}
          {activeTab && (
            <div style={{ flex: 1, overflow: 'hidden', background: '#1a1a24' }}>
              <HotTable
                data={activeTab.results}
                colHeaders={activeTab.columns}
                rowHeaders={true}
                width="100%"
                height="100%"
                licenseKey={process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY || 'non-commercial-and-evaluation'}
                columnSorting={true}
                filters={true}
                dropdownMenu={true}
                contextMenu={true}
                manualColumnResize={true}
                className="jetdb-spreadsheet"
                stretchH="all"
              />
            </div>
          )}
        </>
      )}

      {/* Empty State */}
      {tabs.length === 0 && (
        <div style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '16px',
          color: '#a1a1aa'
        }}>
          <div style={{ fontSize: '64px' }}>🔍</div>
          <h3 style={{ color: '#e4e4e7', margin: 0 }}>
            Start querying your data
          </h3>
          <p style={{ fontSize: '14px', textAlign: 'center', maxWidth: '400px' }}>
            Use <strong style={{ color: '#6366f1' }}>JetAI</strong> for natural language queries or <strong style={{ color: '#10b981' }}>Execute SQL</strong> for custom queries
          </p>
        </div>
      )}
    </div>
  );
};