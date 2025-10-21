import React, { useState } from 'react';
import axios from 'axios';
import toast from 'react-hot-toast';

interface Dataset {
  id: string;
  filename: string;
  row_count: number;
  columns: string[];
}

interface MergeModalProps {
  datasets: Dataset[];
  selectedIds: string[];
  onClose: () => void;
  onComplete: () => void;
  apiBase: string;
  authHeaders: Record<string, string>;
}

export const MergeModal: React.FC<MergeModalProps> = ({
  datasets,
  selectedIds,
  onClose,
  onComplete,
  apiBase,
  authHeaders
}) => {
  const [mergeName, setMergeName] = useState('Merged Dataset');
  const [merging, setMerging] = useState(false);
  const [selectedForMerge, setSelectedForMerge] = useState<Set<string>>(new Set(selectedIds));

  const toggleDataset = (id: string) => {
    const newSelected = new Set(selectedForMerge);
    if (newSelected.has(id)) {
      newSelected.delete(id);
    } else {
      newSelected.add(id);
    }
    setSelectedForMerge(newSelected);
  };

  const selected = datasets.filter(d => selectedForMerge.has(d.id));

  // Check schema match
  let schemaMismatch = false;
  let mismatchDetails = { missing: [] as string[], extra: [] as string[] };

  if (selected.length >= 2) {
    const firstCols = new Set(selected[0].columns.map(c => c.toLowerCase().trim()));
    
    for (let i = 1; i < selected.length; i++) {
      const currentCols = new Set(selected[i].columns.map(c => c.toLowerCase().trim()));
      
      // Check if all columns match
      if (firstCols.size !== currentCols.size || 
          ![...firstCols].every(col => currentCols.has(col))) {
        schemaMismatch = true;
        
        // Find missing columns (in first but not in current)
        mismatchDetails.missing = [...firstCols].filter(col => !currentCols.has(col));
        
        // Find extra columns (in current but not in first)
        mismatchDetails.extra = [...currentCols].filter(col => !firstCols.has(col));
        
        break;
      }
    }
  }

  const handleMerge = async () => {
    if (selected.length < 2) {
      toast.error('❌ Select at least 2 datasets to merge');
      return;
    }

    if (schemaMismatch) {
      toast.error('❌ Cannot merge datasets with different columns');
      return;
    }

    setMerging(true);
    try {
      const { data } = await axios.post(
        `${apiBase}/datasets/merge`,
        { 
          dataset_ids: Array.from(selectedForMerge), 
          merged_name: mergeName 
        },
        { headers: authHeaders }
      );

      toast.success(
        `✅ Merged ${data.row_count.toLocaleString()} rows in ${data.merge_time_seconds}s!`,
        { duration: 5000 }
      );
      onComplete();
    } catch (error: any) {
      const errorDetail = error.response?.data?.detail;
      if (typeof errorDetail === 'object' && errorDetail.error) {
        toast.error(`❌ ${errorDetail.error}`);
      } else {
        toast.error(errorDetail || 'Merge failed');
      }
    } finally {
      setMerging(false);
    }
  };

  return (
    <div 
      style={{ 
        position: 'fixed', 
        inset: 0, 
        background: 'rgba(0,0,0,0.8)', 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center', 
        zIndex: 1000,
        padding: '20px'
      }} 
      onClick={onClose}
    >
      <div 
        style={{ 
          background: '#1a1a24', 
          borderRadius: '16px', 
          width: '90%', 
          maxWidth: '700px', 
          maxHeight: '90vh', 
          overflow: 'auto',
          boxShadow: '0 20px 60px rgba(0, 0, 0, 0.6)'
        }} 
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div style={{ 
          padding: '24px', 
          borderBottom: '1px solid #2d2d44',
          background: '#24243a'
        }}>
          <h2 style={{ margin: 0, color: '#fff', fontSize: '20px', fontWeight: 700 }}>
            🔄 Merge Datasets
          </h2>
          <p style={{ margin: '8px 0 0 0', color: '#a1a1aa', fontSize: '14px' }}>
            Select datasets to merge with identical columns
          </p>
        </div>

        {/* Schema Mismatch Warning */}
        {schemaMismatch && selected.length >= 2 && (
          <div style={{ 
            margin: '20px 24px 0 24px', 
            padding: '16px', 
            background: 'rgba(239, 68, 68, 0.1)', 
            border: '2px solid #ef4444', 
            borderRadius: '12px'
          }}>
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              gap: '12px',
              marginBottom: '12px'
            }}>
              <span style={{ fontSize: '24px' }}>⚠️</span>
              <div>
                <h4 style={{ 
                  margin: 0, 
                  color: '#fca5a5', 
                  fontSize: '16px',
                  fontWeight: 600 
                }}>
                  Columns Don't Match
                </h4>
                <p style={{ 
                  margin: '4px 0 0 0', 
                  color: '#fca5a5', 
                  fontSize: '13px' 
                }}>
                  You can only merge datasets with identical columns
                </p>
              </div>
            </div>
            
            {(mismatchDetails.missing.length > 0 || mismatchDetails.extra.length > 0) && (
              <div style={{ 
                marginTop: '12px', 
                padding: '12px', 
                background: 'rgba(0, 0, 0, 0.2)',
                borderRadius: '8px',
                fontSize: '12px'
              }}>
                {mismatchDetails.missing.length > 0 && (
                  <div style={{ marginBottom: '8px' }}>
                    <strong style={{ color: '#fca5a5' }}>Missing:</strong>{' '}
                    <span style={{ color: '#e4e4e7' }}>
                      {mismatchDetails.missing.join(', ')}
                    </span>
                  </div>
                )}
                {mismatchDetails.extra.length > 0 && (
                  <div>
                    <strong style={{ color: '#fca5a5' }}>Extra:</strong>{' '}
                    <span style={{ color: '#e4e4e7' }}>
                      {mismatchDetails.extra.join(', ')}
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Content */}
        <div style={{ padding: '24px' }}>
          {/* Merged Name Input */}
          <div style={{ marginBottom: '20px' }}>
            <label style={{ 
              display: 'block', 
              marginBottom: '8px', 
              color: '#e4e4e7', 
              fontSize: '14px',
              fontWeight: 600 
            }}>
              Merged Dataset Name
            </label>
            <input
              type="text"
              value={mergeName}
              onChange={(e) => setMergeName(e.target.value)}
              placeholder="Enter name for merged dataset"
              style={{ 
                width: '100%', 
                padding: '12px 16px', 
                borderRadius: '8px', 
                border: '1px solid #2d2d44', 
                background: '#24243a', 
                color: '#fff',
                fontSize: '14px',
                outline: 'none',
                transition: 'border-color 0.2s'
              }}
              onFocus={(e) => e.target.style.borderColor = '#6366f1'}
              onBlur={(e) => e.target.style.borderColor = '#2d2d44'}
            />
          </div>

          {/* Dataset Selection */}
          <div style={{ marginBottom: '16px' }}>
            <label style={{ 
              display: 'block', 
              marginBottom: '12px', 
              color: '#e4e4e7', 
              fontSize: '14px',
              fontWeight: 600 
            }}>
              Select Datasets to Merge ({selectedForMerge.size} selected)
            </label>
            
            <div style={{ 
              maxHeight: '300px', 
              overflowY: 'auto',
              background: '#24243a',
              borderRadius: '8px',
              padding: '8px'
            }}>
              {datasets.map(d => (
                <label
                  key={d.id}
                  style={{ 
                    display: 'flex',
                    alignItems: 'center',
                    padding: '12px',
                    background: selectedForMerge.has(d.id) ? 'rgba(99, 102, 241, 0.2)' : 'transparent',
                    borderRadius: '6px',
                    marginBottom: '8px',
                    cursor: 'pointer',
                    transition: 'all 0.2s',
                    border: `1px solid ${selectedForMerge.has(d.id) ? '#6366f1' : 'transparent'}`
                  }}
                  onMouseOver={(e) => {
                    if (!selectedForMerge.has(d.id)) {
                      e.currentTarget.style.background = 'rgba(61, 61, 84, 0.5)';
                    }
                  }}
                  onMouseOut={(e) => {
                    if (!selectedForMerge.has(d.id)) {
                      e.currentTarget.style.background = 'transparent';
                    }
                  }}
                >
                  <input
                    type="checkbox"
                    checked={selectedForMerge.has(d.id)}
                    onChange={() => toggleDataset(d.id)}
                    style={{ 
                      marginRight: '12px',
                      width: '18px',
                      height: '18px',
                      cursor: 'pointer',
                      accentColor: '#6366f1'
                    }}
                  />
                  <div style={{ flex: 1 }}>
                    <div style={{ 
                      fontSize: '14px', 
                      color: '#e4e4e7',
                      fontWeight: 500,
                      marginBottom: '4px'
                    }}>
                      {d.filename}
                    </div>
                    <div style={{ fontSize: '12px', color: '#a1a1aa' }}>
                      {d.row_count.toLocaleString()} rows • {d.columns.length} columns
                    </div>
                  </div>
                </label>
              ))}
            </div>
          </div>

          {/* Summary */}
          {selected.length >= 2 && (
            <div style={{ 
              padding: '16px', 
              background: schemaMismatch ? 'rgba(239, 68, 68, 0.05)' : 'rgba(16, 185, 129, 0.1)',
              border: `1px solid ${schemaMismatch ? 'rgba(239, 68, 68, 0.3)' : 'rgba(16, 185, 129, 0.3)'}`,
              borderRadius: '8px',
              marginTop: '16px'
            }}>
              <div style={{ 
                display: 'flex', 
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: '8px'
              }}>
                <span style={{ color: '#a1a1aa', fontSize: '13px' }}>Total Rows:</span>
                <span style={{ 
                  color: schemaMismatch ? '#fca5a5' : '#6ee7b7', 
                  fontWeight: 700,
                  fontSize: '16px'
                }}>
                  {selected.reduce((sum, d) => sum + d.row_count, 0).toLocaleString()}
                </span>
              </div>
              <div style={{ 
                display: 'flex', 
                justifyContent: 'space-between',
                alignItems: 'center'
              }}>
                <span style={{ color: '#a1a1aa', fontSize: '13px' }}>Datasets:</span>
                <span style={{ 
                  color: schemaMismatch ? '#fca5a5' : '#6ee7b7', 
                  fontWeight: 600,
                  fontSize: '14px'
                }}>
                  {selected.length} selected
                </span>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={{ 
          padding: '20px 24px', 
          borderTop: '1px solid #2d2d44', 
          display: 'flex', 
          gap: '12px', 
          justifyContent: 'flex-end',
          background: '#24243a'
        }}>
          <button 
            onClick={onClose} 
            disabled={merging} 
            style={{ 
              padding: '12px 24px',
              background: '#3d3d54',
              color: '#fff', 
              border: 'none', 
              borderRadius: '8px', 
              fontWeight: 600,
              fontSize: '14px',
              cursor: merging ? 'not-allowed' : 'pointer',
              opacity: merging ? 0.5 : 1
            }}
          >
            Cancel
          </button>
          <button 
            onClick={handleMerge} 
            disabled={merging || schemaMismatch || !mergeName || selected.length < 2} 
            style={{ 
              padding: '12px 24px',
              background: (merging || schemaMismatch || !mergeName || selected.length < 2) 
                ? '#3d3d54' 
                : 'linear-gradient(135deg, #10b981 0%, #059669 100%)',
              color: '#fff', 
              border: 'none', 
              borderRadius: '8px', 
              fontWeight: 600,
              fontSize: '14px',
              cursor: (merging || schemaMismatch || !mergeName || selected.length < 2) 
                ? 'not-allowed' 
                : 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '8px'
            }}
          >
            {merging ? (
              <>
                <div className="spinner" style={{ width: '14px', height: '14px', borderWidth: '2px' }}></div>
                Merging...
              </>
            ) : (
              <>
                <span>🔄</span>
                Merge Datasets
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
};