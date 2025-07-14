import api from './api';
import { ChatMessage } from '../utils/types';

export interface ChatResponse {
  status: string;
  message?: string;
  response?: string;
  error?: string;
  suggestions?: string[];
  visualization?: any;
  action?: string;
}

class ChatService {
  /**
   * Send a chat message (non-streaming)
   */
  async sendMessage(message: string, language: string = 'en'): Promise<ChatResponse> {
    try {
      const response = await api.post('/send_message', {
        message,
        language,
      });
      return response.data;
    } catch (error) {
      console.error('Error sending message:', error);
      throw error;
    }
  }

  /**
   * Send a chat message with streaming response
   */
  async sendMessageStreaming(
    message: string,
    language: string = 'en',
    onChunk: (chunk: any) => void,
    onComplete: (result: any) => void
  ): Promise<void> {
    try {
      const response = await fetch('/send_message_streaming', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message,
          language,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error('No response body');
      }

      const decoder = new TextDecoder();
      let buffer = '';
      let responseBuffer = '';

      const processChunk = async (): Promise<void> => {
        try {
          const { done, value } = await reader.read();

          if (done) {
            onComplete({
              content: responseBuffer,
              status: 'success',
            });
            return;
          }

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.startsWith('data: ')) {
              try {
                const data = JSON.parse(line.slice(6));
                
                if (data.content) {
                  responseBuffer += data.content;
                }
                
                onChunk(data);
                
                if (data.done) {
                  onComplete({
                    ...data,
                    content: responseBuffer,
                    status: data.status || 'success',
                  });
                  return;
                }
              } catch (e) {
                console.error('Error parsing streaming chunk:', e);
              }
            }
          }

          // Continue reading
          await processChunk();
        } catch (error) {
          console.error('Error reading streaming chunk:', error);
          onComplete({
            error: error instanceof Error ? error.message : 'Unknown error',
            status: 'error',
            content: responseBuffer || 'Error reading response',
          });
        }
      };

      await processChunk();
    } catch (error) {
      console.error('Error with streaming message:', error);
      throw error;
    }
  }

  /**
   * Get session information
   */
  async getSessionInfo() {
    try {
      const response = await api.get('/session_info');
      return response.data;
    } catch (error) {
      console.error('Error getting session info:', error);
      throw error;
    }
  }

  /**
   * Clear session
   */
  async clearSession() {
    try {
      const response = await api.post('/clear_session');
      return response.data;
    } catch (error) {
      console.error('Error clearing session:', error);
      throw error;
    }
  }
}

export default new ChatService();