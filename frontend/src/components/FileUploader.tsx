import React, { useState, useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import axios from 'axios';
import toast from 'react-hot-toast';

interface FileUploaderProps {
  apiBase: string;
  authHeaders: Record<string, string>;
  onUploadComplete: (datasetId: string) => void;
}

export const FileUploader: React.FC<FileUploaderProps> = ({
  apiBase,
  authHeaders,
  onUploadComplete
}) => {
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [fileName, setFileName] = useState<string | null>(null);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (!file) return;

    setUploading(true);
    setFileName(file.name);
    setProgress(0);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await axios.post(`${apiBase}/upload`, formData, {
        headers: {
          ...authHeaders,
          'Content-Type': 'multipart/form-data'
        },
        onUploadProgress: (progressEvent) => {
          const percentCompleted = progressEvent.total
            ? Math.round((progressEvent.loaded * 100) / progressEvent.total)
            : 0;
          setProgress(percentCompleted);
        }
      });

      toast.success(`✅ Uploaded ${file.name}!`);
      onUploadComplete(response.data.dataset_id);
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Upload failed');
      console.error('Upload error:', error);
    } finally {
      setUploading(false);
      setProgress(0);
      setFileName(null);
    }
  }, [apiBase, authHeaders, onUploadComplete]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.ms-excel': ['.xls'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx']
    },
    maxFiles: 1,
    disabled: uploading
  });

  return (
    <div
      {...getRootProps()}
      style={{
        border: '2px dashed #6366f1',
        borderRadius: '12px',
        padding: '60px 40px',
        textAlign: 'center',
        cursor: uploading ? 'not-allowed' : 'pointer',
        background: isDragActive ? 'rgba(99, 102, 241, 0.1)' : 'transparent',
        transition: 'all 0.2s'
      }}
    >
      <input {...getInputProps()} />
      
      {uploading ? (
        <div style={{ padding: '20px' }}>
          <div style={{ fontSize: '48px', marginBottom: '16px' }}>⬆️</div>
          <h3 style={{ margin: '0 0 12px 0', color: '#fff' }}>
            Uploading {fileName}...
          </h3>
          <div style={{
            width: '100%',
            height: '8px',
            background: '#2d2d44',
            borderRadius: '4px',
            overflow: 'hidden',
            marginBottom: '12px'
          }}>
            <div style={{
              width: `${progress}%`,
              height: '100%',
              background: '#6366f1',
              transition: 'width 0.3s'
            }} />
          </div>
          <p style={{ margin: 0, color: '#a1a1aa' }}>{progress}% complete</p>
        </div>
      ) : (
        <>
          <div style={{ fontSize: '64px', marginBottom: '16px' }}>📊</div>
          <h2 style={{ margin: '0 0 12px 0', color: '#fff' }}>
            {isDragActive ? 'Drop it here!' : 'Drop CSV file here'}
          </h2>
          <p style={{ margin: 0, color: '#a1a1aa', fontSize: '14px' }}>
            or click to browse • Up to 10GB • .csv, .xls, .xlsx
          </p>
        </>
      )}
    </div>
  );
};
