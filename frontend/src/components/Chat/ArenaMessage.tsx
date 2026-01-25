import React, { useState, useRef } from 'react';
import type { ArenaMessage as ArenaMessageType } from '@/types';
import { MODEL_DISPLAY_NAMES } from '@/types';
import { useChatStore } from '@/stores/chatStore';
import DualResponsePanel from '../Arena/DualResponsePanel';
import VotingButtons from '../Arena/VotingButtons';
import api from '@/services/api';
import toast from 'react-hot-toast';
import storage from '@/utils/storage';

interface ArenaMessageProps {
  message: ArenaMessageType;
}

const ArenaMessage: React.FC<ArenaMessageProps> = ({ message }) => {
  const { updateArenaAfterVote, updateMessage, completeArenaBattle, session } = useChatStore();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const voteSubmittedRef = useRef(false);  // Prevent duplicate submissions

  // Map each model to a consistent letter label that follows the winner
  const MODEL_TO_LABEL: Record<string, string> = {
    'qwen/qwen3-32b': 'A',
    'llama-3.3-70b-versatile': 'B',
    'moonshotai/kimi-k2-instruct-0905': 'C',
    'gpt-4o': 'D'
  };

  const getResponseLabels = () => {
    // ROUND 1: Always use position-based labels (A on left, B on right)
    if (message.round === 1) {
      return {
        labelA: 'Response A',
        labelB: 'Response B'
      };
    }

    // ROUND 2+: Use model-based labels so winner keeps its name
    const modelA = message.currentMatchup.modelA;
    const modelB = message.currentMatchup.modelB;

    const labelA = MODEL_TO_LABEL[modelA] || 'A';
    const labelB = MODEL_TO_LABEL[modelB] || 'B';

    return {
      labelA: `Response ${labelA}`,
      labelB: `Response ${labelB}`
    };
  };

  const handleVote = async (vote: 'a' | 'b' | 'tie' | 'bad') => {
    // Prevent duplicate submissions
    if (isSubmitting || voteSubmittedRef.current || message.currentVote) {
      console.log('Vote already submitted, ignoring duplicate');
      return;
    }

    voteSubmittedRef.current = true;
    setIsSubmitting(true);

    try {
      console.log('Arena vote submitted', {
        battleId: message.battleId,
        messageId: message.id,
        vote,
        round: message.round
      });

      // Submit vote to backend
      const response = await api.arena.vote(message.battleId, vote, session.sessionId);
      const data = response.data;

      if (data.continue) {
        // Next round - responses are included in the vote response
        const winnerModel = vote === 'a' ? message.currentMatchup.modelA : message.currentMatchup.modelB;

        const updatedMatchup = {
          modelA: data.model_a,
          modelB: data.model_b,
          responseA: data.response_a || '',
          responseB: data.response_b || '',
        };

        updateMessage(message.id, {
          type: 'arena',
          round: data.current_round,
          currentMatchup: updatedMatchup,
          eliminatedModels: data.eliminated_models || [],
          winnerChain: data.winner_chain || [],
          remainingModels: data.remaining_models || [],
          currentVote: undefined,
          modelsRevealed: false,
        } as any);

        voteSubmittedRef.current = false;
        setIsSubmitting(false);

        toast.success(`${MODEL_DISPLAY_NAMES[winnerModel] || winnerModel} wins! Next round starting...`);
      } else {
        // Battle complete
        completeArenaBattle();
        const ranking = (data.final_ranking || []).map((m: string) => MODEL_DISPLAY_NAMES[m as keyof typeof MODEL_DISPLAY_NAMES] || m);
        toast.success(`Tournament complete! Ranking: ${ranking.join(' > ')}`);
      }
    } catch (error: any) {
      console.error('Error submitting vote:', error);
      voteSubmittedRef.current = false;
      setIsSubmitting(false);

      if (error.response) {
        toast.error(`Vote failed: ${error.response.data?.error || error.response.status}`);
      } else if (error.request) {
        toast.error('Failed to submit vote - no response from server');
      } else {
        toast.error(`Failed to submit vote: ${error.message}`);
      }
    }
  };

  return (
    <div className="bg-white dark:bg-dark-bg-secondary rounded-lg shadow-sm border border-gray-200 dark:border-dark-border p-6">
      {/* Arena Header */}
      <div className="mb-4 pb-4 border-b border-gray-200 dark:border-dark-border">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-gradient-to-r from-purple-600 to-pink-600 text-white">
              Arena Mode - Tournament
            </span>
            <span className="text-sm text-gray-600 dark:text-dark-text-secondary">
              Round {message.round} of 3
            </span>
          </div>
          {(message.eliminatedModels || []).length > 0 && (
            <span className="text-sm text-gray-500 dark:text-dark-text-secondary">
              Eliminated: {(message.eliminatedModels || []).map(m => MODEL_DISPLAY_NAMES[m] || m).join(', ')}
            </span>
          )}
        </div>

        {/* User's Question */}
        <div className="mt-3">
          <p className="text-sm text-gray-500 dark:text-dark-text-secondary">Your question:</p>
          <p className="text-gray-900 dark:text-dark-text font-medium">{message.userMessage}</p>
        </div>
      </div>

      {/* Dual Response Panel */}
      <DualResponsePanel
        responseA={message.currentMatchup.responseA}
        responseB={message.currentMatchup.responseB}
        modelA={message.modelsRevealed ? (MODEL_DISPLAY_NAMES[message.currentMatchup.modelA] || message.currentMatchup.modelA) : null}
        modelB={message.modelsRevealed ? (MODEL_DISPLAY_NAMES[message.currentMatchup.modelB] || message.currentMatchup.modelB) : null}
        isLoading={false}
        labelA={getResponseLabels().labelA}
        labelB={getResponseLabels().labelB}
      />

      {/* Voting Section */}
      {!message.currentVote && !message.isComplete ? (
        <VotingButtons onVote={handleVote} disabled={isSubmitting} />
      ) : message.isComplete ? (
        <div className="border-t border-gray-200 dark:border-dark-border pt-6">
          <div className="text-center">
            <p className="text-green-600 dark:text-green-400 font-medium mb-2">
              ✓ Tournament Complete!
            </p>
            <p className="text-gray-600 dark:text-dark-text-secondary">
              Final ranking has been determined
            </p>
          </div>
        </div>
      ) : (
        <div className="border-t border-gray-200 dark:border-dark-border pt-6">
          <div className="text-center">
            <p className="text-gray-600 dark:text-dark-text-secondary">Processing next round...</p>
          </div>
        </div>
      )}

      {/* Progress Indicator */}
      <div className="mt-6 flex justify-center">
        <div className="flex items-center space-x-4">
          <div className={`flex items-center ${message.round >= 1 ? 'text-green-600 dark:text-green-400' : 'text-gray-400 dark:text-dark-text-secondary'}`}>
            <span className="w-8 h-8 rounded-full border-2 flex items-center justify-center border-current">
              {message.round >= 1 ? '✓' : '1'}
            </span>
            <span className="ml-2 text-sm">Round 1</span>
          </div>
          <div className="w-16 h-0.5 bg-gray-300 dark:bg-dark-border" />
          <div className={`flex items-center ${message.round >= 2 ? 'text-green-600 dark:text-green-400' : 'text-gray-400 dark:text-dark-text-secondary'}`}>
            <span className="w-8 h-8 rounded-full border-2 flex items-center justify-center border-current">
              {message.round >= 2 ? '✓' : '2'}
            </span>
            <span className="ml-2 text-sm">Round 2</span>
          </div>
          {message.isComplete && (
            <>
              <div className="w-16 h-0.5 bg-gray-300 dark:bg-dark-border" />
              <div className="flex items-center text-green-600 dark:text-green-400">
                <span className="w-8 h-8 rounded-full border-2 flex items-center justify-center border-current">
                  🏆
                </span>
                <span className="ml-2 text-sm">Complete</span>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default ArenaMessage;
