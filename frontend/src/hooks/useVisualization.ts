import { useMemo } from 'react';

export interface Visualization {
  url: string;
  type: 'html' | 'image';
  title?: string;
}

/**
 * Hook to extract visualization URLs from message content
 * Looks for patterns like:
 * - [VIZ:url]
 * - ![alt](url)
 * - <img src="url">
 * - Direct HTML file paths
 */
export default function useVisualization(content: string): Visualization[] {
  return useMemo(() => {
    if (!content) return [];

    const visualizations: Visualization[] = [];

    // Pattern 1: [VIZ:url] format
    const vizPattern = /\[VIZ:([^\]]+)\]/g;
    let match;
    while ((match = vizPattern.exec(content)) !== null) {
      const url = match[1].trim();
      visualizations.push({
        url,
        type: url.endsWith('.html') ? 'html' : 'image',
      });
    }

    // Pattern 2: Markdown image format ![alt](url)
    const markdownImagePattern = /!\[([^\]]*)\]\(([^)]+)\)/g;
    while ((match = markdownImagePattern.exec(content)) !== null) {
      const title = match[1] || undefined;
      const url = match[2].trim();
      if (!visualizations.some(v => v.url === url)) {
        visualizations.push({
          url,
          type: url.endsWith('.html') ? 'html' : 'image',
          title,
        });
      }
    }

    // Pattern 3: HTML img tag <img src="url">
    const imgTagPattern = /<img[^>]+src=["']([^"']+)["']/g;
    while ((match = imgTagPattern.exec(content)) !== null) {
      const url = match[1].trim();
      if (!visualizations.some(v => v.url === url)) {
        visualizations.push({
          url,
          type: url.endsWith('.html') ? 'html' : 'image',
        });
      }
    }

    // Pattern 4: Direct file paths (session_id/filename.html or .png, including subdirectories)
    // Matches: session_abc123/file.html, abc-123/visualizations/file.html, uuid/tpr_map.html
    const filePathPattern = /(?:[a-zA-Z0-9_-]+(?:\/[a-zA-Z0-9_.-]+)+\.(?:html|png|jpg|jpeg|svg))/g;
    while ((match = filePathPattern.exec(content)) !== null) {
      // Skip if it's already a full URL (starts with http or /)
      if (match[0].startsWith('http') || match[0].startsWith('/')) {
        continue;
      }
      const url = `/serve_viz_file/${match[0]}`;
      if (!visualizations.some(v => v.url === url)) {
        visualizations.push({
          url,
          type: match[0].endsWith('.html') ? 'html' : 'image',
        });
      }
    }

    // Pattern 5: Full /serve_viz_file/ URLs
    const serveVizPattern = /\/serve_viz_file\/[a-zA-Z0-9_\/-]+\.(?:html|png|jpg|jpeg|svg)/g;
    while ((match = serveVizPattern.exec(content)) !== null) {
      const url = match[0];
      if (!visualizations.some(v => v.url === url)) {
        visualizations.push({
          url,
          type: url.endsWith('.html') ? 'html' : 'image',
        });
      }
    }

    // Pattern 6: /static/visualizations/ URLs
    const staticVizPattern = /\/static\/visualizations\/[a-zA-Z0-9_\/-]+\.(?:html|png|jpg|jpeg|svg)/g;
    while ((match = staticVizPattern.exec(content)) !== null) {
      const url = match[0];
      if (!visualizations.some(v => v.url === url)) {
        visualizations.push({
          url,
          type: url.endsWith('.html') ? 'html' : 'image',
        });
      }
    }

    return visualizations;
  }, [content]);
}
