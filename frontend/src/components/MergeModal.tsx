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

  const selected = datasets.filter(d => selectedIds.includes(d.id));

  // Check schema match
  const firstCols = new Set(selected[0]?.columns || []);
  const mismatch = selected.slice(1).some(d => {
    const cols = new Set(d.columns);
    return JSON.stringify([...firstCols].sort()) !== JSON.stringify([...cols].sort());
  });

  const handleMerge = async () => {
    setMerging(true);
    try {
      const { data } = await axios.post(
        `${apiBase}/datasets/merge`,
        { dataset_ids: selectedIds, merged_name: mergeName },
        { headers: authHeaders }
      );

      toast.success(`Merged ${data.row_count.toLocaleString()} rows in ${data.merge_time_seconds}s!`);
      onComplete();
    } catch (error: any) {
      toast.error(error.response?.data?.detail?.error || 'Merge failed');
    } finally {
      setMerging(false);
    }
  };

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }} onClick={onClose}>
      <div style={{ background: '#1a1a24', borderRadius: '12px', width: '90%', maxWidth: '600px', maxHeight: '90vh', overflow: 'auto' }} onClick={(e) => e.stopPropagation()}>
        <div style={{ padding: '24px', borderBottom: '1px solid #2d2d44' }}>
          <h2 style={{ margin: 0, color: '#fff' }}>Merge Datasets</h2>
        </div>

        {mismatch && (
          <div style={{ margin: '16px 24px', padding: '16px', background: 'rgba(239, 68, 68, 0.1)', border: '1px solid #ef4444', borderRadius: '8px', color: '#fca5a5' }}>
            âš ï¸ Schema mismatch detected! Columns don't match across datasets.
          </div>
        )}

        <div style={{ padding: '24px' }}>
          <div style={{ marginBottom: '16px' }}>
            <label style={{ display: 'block', marginBottom: '8px', color: '#a1a1aa', fontSize: '14px' }}>Merged Dataset Name</label>
            <input
              type="text"
              value={mergeName}
              onChange={(e) => setMergeName(e.target.value)}
              style={{ width: '100%', padding: '10px', borderRadius: '6px', border: '1px solid #2d2d44', background: '#24243a', color: '#fff' }}
            />
          </div>

          <div style={{ marginBottom: '16px' }}>
            <label style={{ display: 'block', marginBottom: '8px', color: '#a1a1aa', fontSize: '14px' }}>Selected Datasets ({selected.length})</label>
            {selected.map(d => (
              <div key={d.id} style={{ padding: '8px 12px', background: '#24243a', borderRadius: '6px', marginBottom: '8px', fontSize: '13px', color: '#e4e4e7' }}>
                {d.filename} - {d.row_count.toLocaleString()} rows
              </div>
            ))}
            <div style={{ marginTop: '12px', fontWeight: 600, color: '#fff' }}>
              Total: {selected.reduce((sum, d) => sum + d.row_count, 0).toLocaleString()} rows
            </div>
          </div>
        </div>

        <div style={{ padding: '20px 24px', borderTop: '1px solid #2d2d44', display: 'flex', gap: '12px', justifyContent: 'flex-end' }}>
          <button onClick={onClose} disabled={merging} style={{ padding: '10px 20px', background: '#3d3d54', color: '#fff', border: 'none', borderRadius: '6px', fontWeight: 600, cursor: 'pointer' }}>
            Cancel
          </button>
          <button onClick={handleMerge} disabled={merging || mismatch || !mergeName || selected.length < 2} style={{ padding: '10px 20px', background: '#6366f1', color: '#fff', border: 'none', borderRadius: '6px', fontWeight: 600, cursor: 'pointer' }}>
            {merging ? 'Merging...' : 'Merge Datasets'}
          </button>
        </div>
      </div>
    </div>
  );
};
