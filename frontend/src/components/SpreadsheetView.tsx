import React from 'react';
import { HotTable } from '@handsontable/react';
import 'handsontable/dist/handsontable.full.min.css';
import Handsontable from 'handsontable';

// Register license
if (process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY) {
  Handsontable.setLicenseKey(process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY);
}

interface SpreadsheetViewProps {
  data: any[];
  columns: string[];
}

export const SpreadsheetView: React.FC<SpreadsheetViewProps> = ({ 
  data, 
  columns 
}) => {
  const hotSettings = {
    data: data,
    colHeaders: columns,
    rowHeaders: true,
    width: '100%',
    height: 600,
    licenseKey: process.env.REACT_APP_HANDSONTABLE_LICENSE_KEY || 'non-commercial-and-evaluation',
    
    // Performance settings for large datasets
    renderAllRows: false,
    viewportRowRenderingOffset: 50,
    viewportColumnRenderingOffset: 10,
    
    // Features
    columnSorting: true,
    filters: true,
    dropdownMenu: true,
    contextMenu: true,
    manualColumnResize: true,
    autoWrapRow: true,
    autoWrapCol: true,
    
    // Styling
    stretchH: 'all' as const,
    className: 'htMiddle htCenter',
  };

  return (
    <div className="spreadsheet-container" style={{ width: '100%', height: '100%' }}>
      <HotTable settings={hotSettings} />
    </div>
  );
};
