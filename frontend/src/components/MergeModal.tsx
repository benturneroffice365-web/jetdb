// ============================================================================
// MergeModal.tsx - Complete Implementation with Enhanced Error Messaging
// JetDB v8.0 - Task 5: Improved Schema Mismatch UI
// ============================================================================

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import toast from 'react-hot-toast';

interface Dataset {
  id: string;
  filename: string;
  row_count: number;
  column_count: number;
  columns: string[];
  status: string;
}

interface MergeModalProps {
  datasets: Dataset[];
  apiBase: string;
  authHeaders: Record<string, string>;
  onClose: () => void;
  onComplete: (mergedDataset: Dataset) => void;
}

interface MismatchDetails {
  missing: string[];
  extra: string[];
}

export const MergeModal: React.FC<MergeModalProps> = ({
  datasets,
  apiBase,
  authHeaders,
  onClose,
  onComplete
}) => {
  const [mergeName, setMergeName] = useState('');
  const [merging, setMerging] = useState(false);
  const [progress, setProgress] = useState(0);
  const [schemaMismatch, setSchemaMismatch] = useState(false);
  const [mismatchDetails, setMismatchDetails] = useState<MismatchDetails>({ missing: [], extra: [] });

  // ✅ Set default merge name
  useEffect(() => {
    if (datasets.length > 0) {
      const date = new Date().toISOString().split('T')[0];
      setMergeName(`Merged_${date}_${datasets.length}datasets`);
    }
  }, [datasets]);

  // ✅ TASK 5: Check for schema mismatches
  useEffect(() => {
    if (datasets.length < 2) return;

    const firstColumns = [...datasets[0].columns].sort();
    let hasMismatch = false;
    const missing: string[] = [];
    const extra: string[] = [];

    for (let i = 1; i < datasets.length; i++) {
      const currentColumns = [...datasets[i].columns].sort();
      
      if (JSON.stringify(firstColumns) !== JSON.stringify(currentColumns)) {
        hasMismatch = true;
        
        // Find missing columns (in first dataset but not in current)
        const missingInCurrent = firstColumns.filter(col => !currentColumns.includes(col));
        missing.push(...missingInCurrent);
        
        // Find extra columns (in current dataset but not in first)
        const extraInCurrent = currentColumns.filter(col => !firstColumns.includes(col));
        extra.push(...extraInCurrent);
      }
    }

    setSchemaMismatch(hasMismatch);
    setMismatchDetails({
      missing: [...new Set(missing)], // Remove duplicates
      extra: [...new Set(extra)]
    });
  }, [datasets]);

  // ✅ Handle merge
  const handleMerge = async () => {
    if (schemaMismatch) {
      toast.error('Cannot merge: Columns don\'t match');
      return;
    }

    if (!mergeName.trim()) {
      toast.error('Please enter a name for the merged dataset');
      return;
    }

    setMerging(true);
    setProgress(0);

    try {
      console.log('🔄 Starting merge...', {
        datasetIds: datasets.map(d => d.id),
        name: mergeName
      });

      // Simulate progress updates (since backend is streaming)
      const progressInterval = setInterval(() => {
        setProgress(prev => {
          if (prev >= 90) return prev;
          return prev + Math.random() * 15;
        });
      }, 500);

      const response = await axios.post(
        `${apiBase}/datasets/merge`,
        {
          dataset_ids: datasets.map(d => d.id),
          name: mergeName.trim()
        },
        { headers: authHeaders }
      );

      clearInterval(progressInterval);
      setProgress(100);

      console.log('✅ Merge complete:', response.data);

      toast.success(`✅ Merged ${response.data.row_count.toLocaleString()} rows!`);
      
      setTimeout(() => {
        onComplete(response.data);
      }, 500);

    } catch (error: any) {
      console.error('❌ Merge failed:', error);
      toast.error(error.response?.data?.detail || 'Merge failed');
      setMerging(false);
      setProgress(0);
    }
  };

  // ✅ Calculate total rows
  const totalRows = datasets.reduce((sum, d) => sum + d.row_count, 0);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="modal-header">
          <h2>🔄 Merge Datasets</h2>
          <button onClick={onClose} className="btn-close" disabled={merging}>
            ×
          </button>
        </div>

        {/* Body */}
        <div className="modal-body">
          {/* ✅ TASK 5: Improved schema mismatch error */}
          {schemaMismatch && (
            <div style={{ 
              margin: '0 0 24px 0',
              padding: '24px', 
              background: 'rgba(239, 68, 68, 0.1)', 
              border: '3px solid #ef4444', 
              borderRadius: '16px'
            }}>
              <div style={{ 
                fontSize: '48px', 
                textAlign: 'center', 
                marginBottom: '16px' 
              }}>
                ⚠️
              </div>
              
              <h3 style={{ 
                color: '#ef4444', 
                fontSize: '24px', 
                textAlign: 'center',
                marginBottom: '12px',
                fontWeight: 700,
                margin: '0 0 12px 0'
              }}>
                Columns Don't Match
              </h3>
              
              <p style={{ 
                color: '#fca5a5', 
                textAlign: 'center',
                fontSize: '16px',
                marginBottom: '20px',
                lineHeight: 1.5
              }}>
                You can only merge datasets with identical columns.
              </p>
              
              {/* Show specific mismatches */}
              {(mismatchDetails.missing.length > 0 || mismatchDetails.extra.length > 0) && (
                <div style={{ 
                  background: 'rgba(0, 0, 0, 0.3)',
                  padding: '16px',
                  borderRadius: '12px',
                  border: '1px solid rgba(239, 68, 68, 0.3)'
                }}>
                  {mismatchDetails.missing.length > 0 && (
                    <div style={{ marginBottom: mismatchDetails.extra.length > 0 ? '16px' : 0 }}>
                      <div style={{ 
                        color: '#fca5a5', 
                        fontWeight: 700, 
                        fontSize: '14px',
                        marginBottom: '8px',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px'
                      }}>
                        <span style={{ fontSize: '16px' }}>❌</span>
                        Missing Columns:
                      </div>
                      <div style={{ color: '#e4e4e7', fontSize: '13px', paddingLeft: '24px' }}>
                        {mismatchDetails.missing.map(col => (
                          <div key={col} style={{ padding: '4px 0', fontFamily: 'monospace' }}>
                            • {col}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  
                  {mismatchDetails.extra.length > 0 && (
                    <div>
                      <div style={{ 
                        color: '#fca5a5', 
                        fontWeight: 700, 
                        fontSize: '14px',
                        marginBottom: '8px',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px'
                      }}>
                        <span style={{ fontSize: '16px' }}>➕</span>
                        Extra Columns:
                      </div>
                      <div style={{ color: '#e4e4e7', fontSize: '13px', paddingLeft: '24px' }}>
                        {mismatchDetails.extra.map(col => (
                          <div key={col} style={{ padding: '4px 0', fontFamily: 'monospace' }}>
                            • {col}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
              
              <div style={{ 
                marginTop: '20px', 
                textAlign: 'center',
                fontSize: '13px',
                color: '#fca5a5'
              }}>
                💡 Tip: Make sure all datasets have the same column names
              </div>
            </div>
          )}

          {/* Merge name input */}
          <div className="form-group">
            <label className="form-label">Merged Dataset Name</label>
            <input
              type="text"
              value={mergeName}
              onChange={(e) => setMergeName(e.target.value)}
              placeholder="e.g., Merged_2025_Sales_Data"
              className="form-input"
              disabled={merging}
            />
          </div>

          {/* Selected datasets list */}
          <div className="form-group">
            <label className="form-label">
              Selected Datasets ({datasets.length})
            </label>
            <div className="dataset-list-merge">
              {datasets.map(dataset => (
                <div key={dataset.id} className="dataset-card">
                  <div className="dataset-card-header">
                    <span className="dataset-icon">📄</span>
                    <span className="dataset-filename">{dataset.filename}</span>
                  </div>
                  <div className="dataset-card-stats">
                    {dataset.row_count.toLocaleString()} rows • {dataset.column_count} columns
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Total summary */}
          <div className="merge-summary">
            <div className="summary-row">
              <span className="summary-label">Total Rows:</span>
              <span className="summary-value">{totalRows.toLocaleString()}</span>
            </div>
            <div className="summary-row">
              <span className="summary-label">Total Datasets:</span>
              <span className="summary-value">{datasets.length}</span>
            </div>
            {!schemaMismatch && (
              <div className="summary-row success">
                <span className="summary-label">Schema Check:</span>
                <span className="summary-value">✅ All columns match</span>
              </div>
            )}
          </div>

          {/* Progress bar */}
          {merging && (
            <div className="progress-container">
              <div className="progress-bar">
                <div 
                  className="progress-fill" 
                  style={{ width: `${progress}%` }}
                />
              </div>
              <div className="progress-text">
                {progress < 100 ? (
                  <>Merging {totalRows.toLocaleString()} rows... {Math.round(progress)}%</>
                ) : (
                  <>✅ Merge complete!</>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="modal-footer">
          <button 
            onClick={onClose} 
            className="btn-secondary"
            disabled={merging}
          >
            Cancel
          </button>
          <button 
            onClick={handleMerge}
            className="btn-primary"
            disabled={merging || schemaMismatch || !mergeName.trim() || datasets.length < 2}
          >
            {merging ? (
              <>
                <div className="spinner small" style={{ marginRight: '8px' }} />
                Merging...
              </>
            ) : (
              <>🔄 Merge Datasets</>
            )}
          </button>
        </div>
      </div>
    </div>
  );
};
