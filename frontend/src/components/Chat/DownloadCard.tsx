import React from 'react';
import type { DownloadLink } from '@/types';

const TYPE_ICONS: Record<string, string> = {
  zip: '📦',
  csv: '📊',
  html: '📄',
  pdf: '📄',
  geojson: '🗺️',
  png: '🖼️',
};

interface DownloadCardProps {
  link: DownloadLink;
}

const DownloadCard: React.FC<DownloadCardProps> = ({ link }) => {
  const icon = TYPE_ICONS[link.type] ?? '📎';

  return (
    <a
      href={link.url}
      download={link.filename}
      className="flex items-center gap-3 px-4 py-3 rounded-lg border border-gray-200 dark:border-dark-border bg-white dark:bg-dark-bg-secondary hover:bg-gray-50 dark:hover:bg-dark-bg-tertiary transition-colors no-underline"
    >
      <span className="text-xl flex-shrink-0">{icon}</span>
      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium text-gray-900 dark:text-dark-text truncate">
          {link.filename}
        </div>
        <div className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
          {link.description}
        </div>
      </div>
      <svg
        className="w-4 h-4 text-gray-400 flex-shrink-0"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"
        />
      </svg>
    </a>
  );
};

export default DownloadCard;
