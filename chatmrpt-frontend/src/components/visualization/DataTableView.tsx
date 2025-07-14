import React, { useState, useMemo } from 'react';
import { 
  ChevronUpIcon, 
  ChevronDownIcon,
  MagnifyingGlassIcon,
  ArrowDownTrayIcon
} from '@heroicons/react/24/outline';

interface DataTableViewProps {
  data?: any;
}

const DataTableView: React.FC<DataTableViewProps> = ({ data }) => {
  const [sortField, setSortField] = useState<string>('');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');
  const [searchTerm, setSearchTerm] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 10;

  // Extract table data from various formats
  const tableData = useMemo(() => {
    if (data?.tableData) return data.tableData;
    if (data?.wardRankings) return data.wardRankings;
    if (data?.analysisResults) return data.analysisResults;
    
    // Mock data for demonstration
    return [
      { ward: 'Ward A', population: 15000, tpr: 45, risk_level: 'High', vulnerability_score: 0.85 },
      { ward: 'Ward B', population: 22000, tpr: 38, risk_level: 'High', vulnerability_score: 0.72 },
      { ward: 'Ward C', population: 18000, tpr: 32, risk_level: 'Medium', vulnerability_score: 0.65 },
      { ward: 'Ward D', population: 12000, tpr: 28, risk_level: 'Medium', vulnerability_score: 0.58 },
      { ward: 'Ward E', population: 25000, tpr: 22, risk_level: 'Low', vulnerability_score: 0.45 },
    ];
  }, [data]);

  // Get column headers
  const columns = useMemo(() => {
    if (tableData.length === 0) return [];
    return Object.keys(tableData[0]);
  }, [tableData]);

  // Filter and sort data
  const processedData = useMemo(() => {
    let filtered = tableData.filter((row: any) =>
      Object.values(row).some((value: any) =>
        value?.toString().toLowerCase().includes(searchTerm.toLowerCase())
      )
    );

    if (sortField) {
      filtered.sort((a: any, b: any) => {
        const aVal = a[sortField];
        const bVal = b[sortField];
        
        if (typeof aVal === 'number' && typeof bVal === 'number') {
          return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
        }
        
        const aStr = aVal?.toString() || '';
        const bStr = bVal?.toString() || '';
        return sortDirection === 'asc' 
          ? aStr.localeCompare(bStr)
          : bStr.localeCompare(aStr);
      });
    }

    return filtered;
  }, [tableData, searchTerm, sortField, sortDirection]);

  // Pagination
  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    return processedData.slice(startIndex, startIndex + itemsPerPage);
  }, [processedData, currentPage]);

  const totalPages = Math.ceil(processedData.length / itemsPerPage);

  const handleSort = (field: string) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  const formatCellValue = (value: any) => {
    if (typeof value === 'number') {
      return value.toLocaleString();
    }
    return value?.toString() || '-';
  };

  const getRiskLevelColor = (value: string) => {
    const lower = value?.toLowerCase();
    if (lower === 'high') return 'text-red-600 bg-red-50 dark:bg-red-900/20';
    if (lower === 'medium') return 'text-amber-600 bg-amber-50 dark:bg-amber-900/20';
    if (lower === 'low') return 'text-green-600 bg-green-50 dark:bg-green-900/20';
    return '';
  };

  const handleExport = () => {
    // Convert to CSV
    const headers = columns.join(',');
    const rows = processedData.map((row: any) => 
      columns.map(col => `"${row[col] || ''}"`).join(',')
    );
    const csv = [headers, ...rows].join('\n');
    
    // Download
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'malaria_risk_data.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-4">
      {/* Search and Export */}
      <div className="flex justify-between items-center">
        <div className="relative flex-1 max-w-sm">
          <MagnifyingGlassIcon className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
          <input
            type="text"
            placeholder="Search..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border rounded-lg dark:bg-gray-700 dark:border-gray-600"
          />
        </div>
        <button
          onClick={handleExport}
          className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
        >
          <ArrowDownTrayIcon className="h-4 w-4" />
          <span>Export CSV</span>
        </button>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
          <thead className="bg-gray-50 dark:bg-gray-700">
            <tr>
              {columns.map((column) => (
                <th
                  key={column}
                  onClick={() => handleSort(column)}
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600"
                >
                  <div className="flex items-center space-x-1">
                    <span>{column.replace(/_/g, ' ')}</span>
                    {sortField === column && (
                      sortDirection === 'asc' 
                        ? <ChevronUpIcon className="h-4 w-4" />
                        : <ChevronDownIcon className="h-4 w-4" />
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
            {paginatedData.map((row: any, idx: number) => (
              <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                {columns.map((column) => (
                  <td key={column} className="px-6 py-4 whitespace-nowrap text-sm">
                    {column.toLowerCase().includes('risk_level') ? (
                      <span className={`px-2 py-1 rounded text-xs font-medium ${getRiskLevelColor(row[column])}`}>
                        {formatCellValue(row[column])}
                      </span>
                    ) : (
                      <span className="text-gray-900 dark:text-gray-100">
                        {formatCellValue(row[column])}
                      </span>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex justify-between items-center">
        <div className="text-sm text-gray-600 dark:text-gray-400">
          Showing {(currentPage - 1) * itemsPerPage + 1} to{' '}
          {Math.min(currentPage * itemsPerPage, processedData.length)} of{' '}
          {processedData.length} entries
        </div>
        <div className="flex space-x-2">
          <button
            onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
            disabled={currentPage === 1}
            className="px-3 py-1 border rounded disabled:opacity-50"
          >
            Previous
          </button>
          <span className="px-3 py-1">
            Page {currentPage} of {totalPages}
          </span>
          <button
            onClick={() => setCurrentPage(Math.min(totalPages, currentPage + 1))}
            disabled={currentPage === totalPages}
            className="px-3 py-1 border rounded disabled:opacity-50"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
};

export default DataTableView;