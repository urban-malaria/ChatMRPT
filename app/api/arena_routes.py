"""
Arena Routes for LLM Model Comparison

Simplified API endpoints for Arena battle system using Groq API.
Users compare different LLM models in a blind tournament format.
"""

import logging
import time
from flask import Blueprint, request, jsonify, session
from typing import Dict, Any

from app.config.arena import is_arena_available, ARENA_MODELS

logger = logging.getLogger(__name__)

# Create blueprint
arena_bp = Blueprint('arena', __name__, url_prefix='/api/arena')

# Arena manager instance (initialized in init_arena_system)
arena_manager = None


def init_arena_system(app):
    """Initialize arena system with app context."""
    global arena_manager
    try:
        from app.arena.manager import ArenaManager
        arena_manager = ArenaManager()
        logger.info(f"Arena system initialized with {len(arena_manager.available_models)} models (Groq)")
    except Exception as e:
        logger.error(f"Failed to initialize Arena system: {e}")
        arena_manager = None


@arena_bp.route('/status', methods=['GET'])
def arena_status():
    """
    Check if arena mode is available and get current configuration.

    Response:
    {
        "available": true,
        "models": ["llama-3.3-70b-versatile", "mixtral-8x7b-32768", "gemma2-9b-it"],
        "total_battles": 42,
        "leaderboard": [...]
    }
    """
    try:
        if not arena_manager:
            return jsonify({
                'available': False,
                'message': 'Arena system not initialized',
            }), 503

        if not is_arena_available():
            return jsonify({
                'available': False,
                'message': 'Arena is disabled or GROQ_API_KEY not set',
            }), 503

        stats = arena_manager.get_statistics()

        return jsonify({
            'available': True,
            'models': list(arena_manager.available_models.keys()),
            'active_models': stats['active_models'],
            'total_battles': stats['total_battles'],
            'completed_battles': stats['completed_battles'],
            'leaderboard': stats['leaderboard'][:5],
        })

    except Exception as e:
        logger.error(f"Error checking arena status: {e}")
        return jsonify({
            'available': False,
            'error': str(e),
        }), 500


@arena_bp.route('/start_battle', methods=['POST'])
def start_battle():
    """
    Start a new arena battle session.

    Request body:
    {
        "message": "User's query",
        "session_id": "optional-session-id"
    }

    Response:
    {
        "battle_id": "uuid",
        "status": "ready",
        "model_a": "llama-3.3-70b-versatile",
        "model_b": "mixtral-8x7b-32768",
        "response_a": "...",
        "response_b": "...",
        "current_round": 1,
        "total_models": 3
    }
    """
    try:
        if not arena_manager:
            return jsonify({'error': 'Arena system not initialized'}), 503

        if not is_arena_available():
            return jsonify({'error': 'Arena is disabled or GROQ_API_KEY not set'}), 503

        data = request.get_json() or {}
        user_message = data.get('message', '').strip()

        if not user_message:
            return jsonify({'error': 'Message is required'}), 400

        # Get session ID from request or Flask session
        session_id = data.get('session_id') or session.get('session_id')

        # Log arena user message
        logger.info(f"Arena battle starting for session {session_id}: '{user_message[:50]}...'")

        # Start battle (synchronous - Groq is fast)
        result = arena_manager.start_battle(user_message, session_id)

        if 'error' in result:
            return jsonify(result), 500

        # Store battle ID in session
        session['current_battle_id'] = result['battle_id']

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error starting battle: {e}")
        return jsonify({
            'error': 'Failed to start battle',
            'details': str(e),
        }), 500


@arena_bp.route('/vote', methods=['POST'])
def record_vote():
    """
    Submit user's vote and get next matchup or final results.

    Request body:
    {
        "battle_id": "uuid",
        "choice": "left" | "right" | "tie"
    }

    Response (if more rounds):
    {
        "continue": true,
        "current_round": 2,
        "model_a": "llama-3.3-70b-versatile",
        "model_b": "gemma2-9b-it",
        "response_a": "...",
        "response_b": "...",
        "eliminated_models": ["mixtral-8x7b-32768"],
        "winner_chain": ["llama-3.3-70b-versatile"]
    }

    Response (if complete):
    {
        "continue": false,
        "final_ranking": ["llama-3.3-70b-versatile", "gemma2-9b-it", "mixtral-8x7b-32768"],
        "winner": "llama-3.3-70b-versatile",
        "total_rounds": 2
    }
    """
    try:
        if not arena_manager:
            return jsonify({'error': 'Arena system not initialized'}), 503

        data = request.get_json() or {}
        battle_id = data.get('battle_id')
        choice = data.get('choice') or data.get('vote') or data.get('preference')

        if not battle_id:
            return jsonify({'error': 'battle_id is required'}), 400

        if not choice:
            return jsonify({'error': 'choice is required'}), 400

        # Normalize choice values
        choice_mapping = {
            'a': 'left',
            'b': 'right',
            'left': 'left',
            'right': 'right',
            'tie': 'tie',
            'both_bad': 'tie',
        }
        normalized_choice = choice_mapping.get(choice.lower(), choice)

        if normalized_choice not in ['left', 'right', 'tie']:
            return jsonify({'error': f'Invalid choice: {choice}'}), 400

        logger.info(f"Arena vote: battle={battle_id}, choice={normalized_choice}")

        # Submit vote (synchronous)
        result = arena_manager.submit_vote(battle_id, normalized_choice)

        if 'error' in result:
            return jsonify(result), 404

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error recording vote: {e}")
        return jsonify({
            'error': 'Failed to record vote',
            'details': str(e),
        }), 500


@arena_bp.route('/leaderboard', methods=['GET'])
def get_leaderboard():
    """
    Get the current model leaderboard.

    Response:
    {
        "leaderboard": [
            {
                "rank": 1,
                "model": "llama-3.3-70b-versatile",
                "display_name": "Llama 3.3 70B",
                "provider": "groq",
                "elo_rating": 1650.5,
                "battles_fought": 42,
                "win_rate": 65.3
            },
            ...
        ],
        "last_updated": 1706140800
    }
    """
    try:
        if not arena_manager:
            return jsonify({'error': 'Arena system not initialized'}), 503

        leaderboard = arena_manager.get_leaderboard()

        return jsonify({
            'leaderboard': leaderboard,
            'last_updated': int(time.time()),
        })

    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        return jsonify({
            'error': 'Failed to get leaderboard',
            'details': str(e),
        }), 500


@arena_bp.route('/statistics', methods=['GET'])
def get_statistics():
    """Get comprehensive arena statistics."""
    try:
        if not arena_manager:
            return jsonify({'error': 'Arena system not initialized'}), 503

        stats = arena_manager.get_statistics()
        return jsonify(stats)

    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return jsonify({
            'error': 'Failed to get statistics',
            'details': str(e),
        }), 500
