import React from 'react';

interface VotingButtonsProps {
  onVote: (vote: 'a' | 'b' | 'tie' | 'bad') => void;
  disabled?: boolean;
}

const VotingButtons: React.FC<VotingButtonsProps> = ({ onVote, disabled }) => {
  const buttons = [
    {
      vote: 'a' as const,
      label: 'Left is Better',
      icon: '👈',
      bgColor: 'bg-blue-600 hover:bg-blue-700',
    },
    {
      vote: 'b' as const,
      label: 'Right is Better',
      icon: '👉',
      bgColor: 'bg-green-600 hover:bg-green-700',
    },
    {
      vote: 'tie' as const,
      label: "It's a Tie",
      icon: '🤝',
      bgColor: 'bg-yellow-600 hover:bg-yellow-700',
    },
    {
      vote: 'bad' as const,
      label: 'Both are Bad',
      icon: '👎',
      bgColor: 'bg-red-600 hover:bg-red-700',
    },
  ];

  return (
    <div className="border-t border-gray-200 dark:border-dark-border pt-6">
      <h3 className="text-center text-lg font-semibold mb-4 text-gray-900 dark:text-dark-text">
        Which response is better?
      </h3>
      <div className="flex justify-center gap-3 flex-wrap">
        {buttons.map((button) => (
          <button
            key={button.vote}
            onClick={() => onVote(button.vote)}
            disabled={disabled}
            className={`
              flex items-center gap-2 px-5 py-2.5 rounded-lg text-white font-medium
              transform transition-all duration-200 hover:scale-105 active:scale-95
              disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:scale-100
              ${button.bgColor}
            `}
          >
            <span className="text-lg">{button.icon}</span>
            <span>{button.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
};

export default VotingButtons;
