"""Arena voting endpoints for the analysis blueprint."""

from __future__ import annotations

import logging

from flask import jsonify, request, session

from app.auth.decorators import require_auth
from app.config.arena import is_arena_available
from ...core.decorators import handle_errors, validate_session

from . import analysis_bp

logger = logging.getLogger(__name__)


@analysis_bp.route('/api/vote_arena', methods=['POST'])
@require_auth
@validate_session
@handle_errors
def vote_arena():
    """
    Record a user vote for progressive Arena battles.

    This endpoint delegates to the ArenaManager for vote processing.
    """
    if not is_arena_available():
        return jsonify({
            'status': 'disabled',
            'message': 'Arena is disabled or GROQ_API_KEY not set.'
        }), 404

    try:
        data = request.json or {}
        battle_id = data.get('battle_id')
        vote = data.get('vote')
        session_id = session.get('session_id', 'unknown')

        if not battle_id:
            return jsonify({'success': False, 'error': 'battle_id is required'}), 400
        if not vote:
            return jsonify({'success': False, 'error': 'vote is required'}), 400

        logger.info("Arena vote received: battle_id=%s, vote=%s, session=%s", battle_id, vote, session_id)

        from app.core.arena_manager import ArenaManager
        arena_manager = ArenaManager()

        # Map vote to choice
        choice_mapping = {
            'a': 'left',
            'b': 'right',
            'left': 'left',
            'right': 'right',
            'tie': 'tie',
            'bad': 'tie',
        }
        choice = choice_mapping.get(vote.lower(), 'tie')

        # Submit vote using the ArenaManager
        result = arena_manager.submit_vote(battle_id, choice)

        if 'error' in result:
            return jsonify({'success': False, 'error': result['error']}), 404

        # Transform result to expected format
        if result.get('continue'):
            return jsonify({
                'success': True,
                'continue_battle': True,
                'round': result.get('current_round', 1),
                'model_a': result.get('model_a'),
                'model_b': result.get('model_b'),
                'response_a': result.get('response_a', ''),
                'response_b': result.get('response_b', ''),
                'latency_a': result.get('latency_a', 0),
                'latency_b': result.get('latency_b', 0),
                'eliminated_models': result.get('eliminated_models', []),
                'winner_chain': result.get('winner_chain', []),
            })
        else:
            return jsonify({
                'success': True,
                'continue_battle': False,
                'final_ranking': result.get('final_ranking', []),
                'comparison_history': result.get('comparison_history', []),
                'winner': result.get('winner'),
                'message': f"Tournament complete! Winner: {result.get('winner', 'Unknown')}",
            })

    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Error processing arena vote: %s", exc)
        return jsonify({'success': False, 'error': str(exc)}), 500
