"""
LLM Arena Manager with Redis Storage

Simplified arena manager using Groq API for fast model comparisons.
Manages battle sessions with distributed Redis storage for multi-worker environments.
"""

import json
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import redis

from app.config.arena import (
    ARENA_MODELS,
    MAX_TOKENS,
    RESPONSE_TIMEOUT,
    REDIS_TTL_HOURS,
    is_arena_available,
)
from app.core.llm_adapter import LLMAdapter
from app.core.arena_system_prompt import get_arena_system_prompt

logger = logging.getLogger(__name__)

# Interaction logger for database storage
_interaction_logger = None

def get_interaction_logger():
    """Get or create interaction logger instance."""
    global _interaction_logger
    if _interaction_logger is None:
        try:
            from app.interaction import InteractionLogger
            _interaction_logger = InteractionLogger()
            logger.info("Arena interaction logger initialized")
        except Exception as e:
            logger.warning(f"Could not initialize interaction logger: {e}")
    return _interaction_logger


@dataclass
class ProgressiveBattleSession:
    """Represents a progressive battle session with multiple models."""
    session_id: str
    user_message: str
    all_models: List[str]
    all_responses: Dict[str, str] = field(default_factory=dict)
    all_latencies: Dict[str, float] = field(default_factory=dict)
    comparison_history: List[Dict] = field(default_factory=list)
    current_round: int = 0
    current_pair: Tuple[str, str] = None
    eliminated_models: List[str] = field(default_factory=list)
    winner_chain: List[str] = field(default_factory=list)
    remaining_models: List[str] = field(default_factory=list)
    final_ranking: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    completed: bool = False

    def to_dict(self) -> Dict:
        """Convert to dictionary for Redis storage."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        if self.current_pair:
            data['current_pair'] = list(self.current_pair)
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> 'ProgressiveBattleSession':
        """Create instance from dictionary."""
        if 'timestamp' in data and isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        if 'current_pair' in data and isinstance(data['current_pair'], list):
            data['current_pair'] = tuple(data['current_pair']) if data['current_pair'] else None
        return cls(**data)

    def get_next_matchup(self) -> Optional[Tuple[str, str]]:
        """Get the next pair of models to compare following tournament structure."""
        # Tournament: Round 1: A vs B, Round 2: Winner vs C, Round 3: Winner vs D (if present)
        has_final_model = any(
            ARENA_MODELS.get(m, {}).get('is_final', False)
            for m in self.all_models
        )
        final_model = next(
            (m for m in self.all_models if ARENA_MODELS.get(m, {}).get('is_final', False)),
            None
        )

        pair = None
        if self.current_round == 0:
            # First round: Use first two non-final models
            non_final_models = [m for m in self.all_models if m != final_model]
            if len(non_final_models) >= 2:
                pair = (non_final_models[0], non_final_models[1])
            elif len(self.all_models) >= 2:
                pair = (self.all_models[0], self.all_models[1])
        else:
            # Subsequent rounds: winner vs next challenger
            if self.winner_chain:
                current_winner = self.winner_chain[-1]
                models_that_competed = set(self.winner_chain + self.eliminated_models)
                unused_models = [m for m in self.all_models if m not in models_that_competed]

                if has_final_model and final_model in unused_models:
                    unused_non_final = [m for m in unused_models if m != final_model]
                    if len(unused_non_final) == 0:
                        # Final round: champion vs final model
                        pair = (current_winner, final_model)
                    else:
                        pair = (current_winner, unused_non_final[0])
                elif unused_models:
                    pair = (current_winner, unused_models[0])

        # Randomize left/right position to avoid position bias
        if pair and random.random() > 0.5:
            pair = (pair[1], pair[0])

        return pair

    def record_choice(self, choice: str) -> bool:
        """Record user's choice and update state. Returns True if more comparisons needed."""
        if not self.current_pair:
            return False

        model_a, model_b = self.current_pair

        # Record comparison
        self.comparison_history.append({
            'round': self.current_round,
            'model_a': model_a,
            'model_b': model_b,
            'choice': choice,
            'timestamp': datetime.now().isoformat()
        })

        # Determine winner and loser
        if choice == 'left':
            winner, loser = model_a, model_b
        elif choice == 'right':
            winner, loser = model_b, model_a
        else:  # tie - randomly pick
            winner = model_a if random.random() > 0.5 else model_b
            loser = model_b if winner == model_a else model_a

        # Update state
        if winner not in self.winner_chain:
            self.winner_chain.append(winner)
        self.eliminated_models.append(loser)

        if loser in self.remaining_models:
            self.remaining_models.remove(loser)

        self.current_round += 1

        # Check if more comparisons needed
        expected_rounds = len(self.all_models) - 1
        if self.current_round < expected_rounds and len(self.remaining_models) > 1:
            self.current_pair = self.get_next_matchup()
            if self.current_pair:
                return True

        # Tournament complete
        self.completed = True
        if self.winner_chain:
            champion = self.winner_chain[-1]
            self.final_ranking = [champion] + self.eliminated_models[::-1]
        else:
            self.final_ranking = self.eliminated_models[::-1]

        return False


class RedisStorage:
    """Redis storage backend for battle sessions."""

    def __init__(self, redis_host: str = None, redis_port: int = 6379,
                 redis_password: str = None, redis_db: int = 1):
        self.redis_host = redis_host or os.environ.get(
            'REDIS_HOST',
            'chatmrpt-redis-staging.1b3pmt.0001.use2.cache.amazonaws.com'
        )
        self.redis_port = redis_port or int(os.environ.get('REDIS_PORT', 6379))
        self.redis_password = redis_password or os.environ.get('REDIS_PASSWORD')
        self.redis_db = redis_db

        self.redis_client = None
        self.connected = False
        self.fallback_storage = {}

        self._connect()

    def _connect(self) -> bool:
        """Establish Redis connection."""
        try:
            self.redis_client = redis.StrictRedis(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                db=self.redis_db,
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            self.redis_client.ping()
            self.connected = True
            logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
            return True
        except redis.ConnectionError as e:
            logger.warning(f"Could not connect to Redis: {e}. Using fallback storage.")
            self.connected = False
            return False
        except Exception as e:
            logger.error(f"Redis connection error: {e}")
            self.connected = False
            return False

    def _get_key(self, session_id: str) -> str:
        return f"arena:progressive:{session_id}"

    def store_progressive_battle(self, battle: ProgressiveBattleSession, ttl_hours: int = None) -> bool:
        """Store progressive battle session."""
        ttl_hours = ttl_hours or REDIS_TTL_HOURS
        key = self._get_key(battle.session_id)

        if self.connected:
            try:
                battle_json = json.dumps(battle.to_dict())
                self.redis_client.setex(key, timedelta(hours=ttl_hours), battle_json)
                self.redis_client.sadd("arena:progressive_battles", battle.session_id)
                return True
            except Exception as e:
                logger.error(f"Failed to store battle in Redis: {e}")
                self.fallback_storage[battle.session_id] = battle
                return True
        else:
            self.fallback_storage[battle.session_id] = battle
            return True

    def get_progressive_battle(self, session_id: str) -> Optional[ProgressiveBattleSession]:
        """Retrieve progressive battle session."""
        if self.connected:
            try:
                key = self._get_key(session_id)
                battle_json = self.redis_client.get(key)
                if battle_json:
                    return ProgressiveBattleSession.from_dict(json.loads(battle_json))
                if session_id in self.fallback_storage:
                    return self.fallback_storage[session_id]
                return None
            except Exception as e:
                logger.error(f"Failed to retrieve battle from Redis: {e}")
                return self.fallback_storage.get(session_id)
        else:
            return self.fallback_storage.get(session_id)

    def update_progressive_battle(self, battle: ProgressiveBattleSession) -> bool:
        """Update existing progressive battle session."""
        return self.store_progressive_battle(battle)


class ELORatingSystem:
    """ELO rating system for model comparison."""

    def __init__(self, k_factor: int = 32):
        self.k_factor = k_factor
        self.ratings = {}
        self.match_history = []

    def get_rating(self, model: str) -> float:
        if model not in self.ratings:
            self.ratings[model] = 1500.0
        return self.ratings[model]

    def update_ratings(self, winner: str, loser: str, is_tie: bool = False):
        rating_winner = self.get_rating(winner)
        rating_loser = self.get_rating(loser)

        expected_winner = 1 / (1 + 10 ** ((rating_loser - rating_winner) / 400))
        expected_loser = 1 / (1 + 10 ** ((rating_winner - rating_loser) / 400))

        if is_tie:
            score_winner, score_loser = 0.5, 0.5
        else:
            score_winner, score_loser = 1.0, 0.0

        self.ratings[winner] = rating_winner + self.k_factor * (score_winner - expected_winner)
        self.ratings[loser] = rating_loser + self.k_factor * (score_loser - expected_loser)

        self.match_history.append({
            'winner': winner if not is_tie else 'tie',
            'loser': loser if not is_tie else 'tie',
            'winner_rating_after': self.ratings[winner],
            'loser_rating_after': self.ratings[loser],
            'timestamp': datetime.now().isoformat()
        })


class ArenaManager:
    """
    Manages LLM battle sessions using Groq API with Redis storage.

    Tournament structure:
    - 3 Groq models (A, B, C) compete in elimination rounds
    - Optionally, winner faces GPT-4o (D) in final round
    """

    def __init__(self, models_config: Optional[Dict] = None):
        self.storage = RedisStorage()
        self.available_models = models_config or ARENA_MODELS.copy()
        self.elo_system = ELORatingSystem()
        self.model_usage_stats = {model: 0 for model in self.available_models}
        self.preference_stats = {'left': 0, 'right': 0, 'tie': 0, 'both_bad': 0}

        self._load_stats()
        logger.info(f"Arena Manager initialized with {len(self.available_models)} models (Groq backend)")
        logger.info(f"Redis storage: {'Connected' if self.storage.connected else 'Fallback mode'}")

    def _get_model_response(self, model_id: str, user_message: str) -> Tuple[str, float]:
        """Get response from a model using LLMAdapter. Returns (response, latency_ms)."""
        model_config = self.available_models.get(model_id, {})
        provider = model_config.get('provider', 'groq')

        start_time = time.time()
        system_prompt = get_arena_system_prompt()

        try:
            adapter = LLMAdapter(backend=provider, model=model_id)
            response = adapter.generate(
                prompt=user_message,
                max_tokens=MAX_TOKENS,
                temperature=0.7,
                system_message=system_prompt,
            )
            latency = (time.time() - start_time) * 1000
            logger.info(f"{model_id} responded in {latency:.0f}ms ({len(response)} chars)")
            return response, latency
        except Exception as e:
            latency = (time.time() - start_time) * 1000
            logger.error(f"Error from {model_id}: {e}")
            return f"Error: {str(e)}", latency

    def start_battle(self, user_message: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """Start a new arena battle session."""
        if not session_id:
            session_id = str(uuid.uuid4())

        # Select models for tournament - randomize non-final models, keep final model (GPT-4o) at end
        all_models = list(self.available_models.keys())
        non_final_models = [m for m in all_models if not self.available_models.get(m, {}).get('is_final')]
        final_model = [m for m in all_models if self.available_models.get(m, {}).get('is_final')]
        random.shuffle(non_final_models)  # Randomize the 3 Groq models
        model_ids = non_final_models + final_model  # Final model (GPT-4o) always last

        battle = ProgressiveBattleSession(
            session_id=session_id,
            user_message=user_message,
            all_models=model_ids,
            remaining_models=model_ids.copy(),
        )

        # Get initial matchup
        battle.current_pair = battle.get_next_matchup()
        if not battle.current_pair:
            return {'error': 'Not enough models for battle'}

        model_a, model_b = battle.current_pair

        # Get responses from first pair
        response_a, latency_a = self._get_model_response(model_a, user_message)
        response_b, latency_b = self._get_model_response(model_b, user_message)

        battle.all_responses[model_a] = response_a
        battle.all_responses[model_b] = response_b
        battle.all_latencies[model_a] = latency_a
        battle.all_latencies[model_b] = latency_b

        # Store battle
        self.storage.store_progressive_battle(battle)

        # Update usage stats
        self.model_usage_stats[model_a] = self.model_usage_stats.get(model_a, 0) + 1
        self.model_usage_stats[model_b] = self.model_usage_stats.get(model_b, 0) + 1

        logger.info(f"Started battle {session_id}: {model_a} vs {model_b}")

        return {
            'battle_id': session_id,
            'status': 'ready',
            'model_a': model_a,
            'model_b': model_b,
            'response_a': response_a,
            'response_b': response_b,
            'latency_a': latency_a,
            'latency_b': latency_b,
            'current_round': 1,
            'total_models': len(model_ids),
        }

    def submit_vote(self, battle_id: str, choice: str) -> Dict[str, Any]:
        """Submit user's choice and get next matchup or final results."""
        battle = self.storage.get_progressive_battle(battle_id)
        if not battle:
            return {'error': 'Battle not found'}

        # Capture current matchup info before updating
        current_model_a, current_model_b = battle.current_pair if battle.current_pair else (None, None)
        elo_before_a = self.elo_system.ratings.get(current_model_a, 1500) if current_model_a else None
        elo_before_b = self.elo_system.ratings.get(current_model_b, 1500) if current_model_b else None

        # Update ELO before recording choice
        if battle.current_pair and choice in ['left', 'right']:
            model_a, model_b = battle.current_pair
            if choice == 'left':
                self.elo_system.update_ratings(model_a, model_b)
            else:
                self.elo_system.update_ratings(model_b, model_a)

        # Capture ELO after update
        elo_after_a = self.elo_system.ratings.get(current_model_a, 1500) if current_model_a else None
        elo_after_b = self.elo_system.ratings.get(current_model_b, 1500) if current_model_b else None

        # Update preference stats
        self.preference_stats[choice] = self.preference_stats.get(choice, 0) + 1

        # Log this matchup to database
        self._log_battle_to_db(
            battle_id=battle_id,
            session_id=battle.session_id,
            user_message=battle.user_message,
            model_a=current_model_a,
            model_b=current_model_b,
            response_a=battle.all_responses.get(current_model_a, ''),
            response_b=battle.all_responses.get(current_model_b, ''),
            latency_a=battle.all_latencies.get(current_model_a, 0),
            latency_b=battle.all_latencies.get(current_model_b, 0),
            user_preference=choice,
            elo_before_a=elo_before_a,
            elo_before_b=elo_before_b,
            elo_after_a=elo_after_a,
            elo_after_b=elo_after_b,
            round_number=battle.current_round
        )

        # Record choice and check if more rounds needed
        more_rounds = battle.record_choice(choice)

        if more_rounds and battle.current_pair:
            model_a, model_b = battle.current_pair

            # Get responses for next matchup (fetch if not cached)
            if model_a not in battle.all_responses:
                resp, lat = self._get_model_response(model_a, battle.user_message)
                battle.all_responses[model_a] = resp
                battle.all_latencies[model_a] = lat
                self.model_usage_stats[model_a] = self.model_usage_stats.get(model_a, 0) + 1

            if model_b not in battle.all_responses:
                resp, lat = self._get_model_response(model_b, battle.user_message)
                battle.all_responses[model_b] = resp
                battle.all_latencies[model_b] = lat
                self.model_usage_stats[model_b] = self.model_usage_stats.get(model_b, 0) + 1

            # Save updated battle
            self.storage.update_progressive_battle(battle)
            self._save_stats()

            return {
                'continue': True,
                'current_round': battle.current_round + 1,
                'model_a': model_a,
                'model_b': model_b,
                'response_a': battle.all_responses.get(model_a, ''),
                'response_b': battle.all_responses.get(model_b, ''),
                'latency_a': battle.all_latencies.get(model_a, 0),
                'latency_b': battle.all_latencies.get(model_b, 0),
                'eliminated_models': battle.eliminated_models or [],
                'winner_chain': battle.winner_chain or [],
                'remaining_models': battle.remaining_models or [],
                'final_ranking': [],  # Not complete yet
            }
        else:
            # Tournament complete
            self.storage.update_progressive_battle(battle)
            self._save_stats()

            return {
                'continue': False,
                'final_ranking': battle.final_ranking or [],
                'winner': battle.final_ranking[0] if battle.final_ranking else None,
                'comparison_history': battle.comparison_history or [],
                'eliminated_models': battle.eliminated_models or [],
                'winner_chain': battle.winner_chain or [],
                'total_rounds': battle.current_round,
            }

    def _log_battle_to_db(self, battle_id: str, session_id: str, user_message: str,
                          model_a: str, model_b: str, response_a: str, response_b: str,
                          latency_a: float, latency_b: float, user_preference: str,
                          elo_before_a: float, elo_before_b: float,
                          elo_after_a: float, elo_after_b: float,
                          round_number: int) -> bool:
        """Log a battle matchup to the interactions database for research/training data."""
        try:
            interaction_logger = get_interaction_logger()
            if interaction_logger is None:
                logger.debug("Interaction logger not available, skipping database logging")
                return False

            # Create unique ID for this specific matchup (battle_id + round)
            matchup_id = f"{battle_id}_round{round_number}"

            interaction_logger.event_logger.log_arena_battle(
                battle_id=matchup_id,
                session_id=session_id,
                user_message=user_message,
                model_a=model_a,
                model_b=model_b,
                response_a=response_a,
                response_b=response_b,
                latency_a=latency_a,
                latency_b=latency_b,
                user_preference=user_preference,
                elo_before_a=elo_before_a,
                elo_before_b=elo_before_b,
                elo_after_a=elo_after_a,
                elo_after_b=elo_after_b,
                view_index=round_number
            )
            logger.info(f"Logged arena battle to database: {matchup_id} ({model_a} vs {model_b})")
            return True
        except Exception as e:
            logger.error(f"Failed to log arena battle to database: {e}")
            return False

    def get_leaderboard(self) -> List[Dict[str, Any]]:
        """Get current model leaderboard sorted by ELO rating."""
        leaderboard = []

        for model_id, config in self.available_models.items():
            rating = self.elo_system.get_rating(model_id)
            usage = self.model_usage_stats.get(model_id, 0)

            wins = sum(1 for m in self.elo_system.match_history if m['winner'] == model_id)
            total = sum(1 for m in self.elo_system.match_history
                       if model_id in [m['winner'], m['loser']])
            win_rate = (wins / total * 100) if total > 0 else 0

            leaderboard.append({
                'rank': 0,
                'model': model_id,
                'display_name': config.get('display_name', model_id),
                'provider': config.get('provider', 'groq'),
                'elo_rating': round(rating, 1),
                'battles_fought': usage,
                'win_rate': round(win_rate, 1),
            })

        leaderboard.sort(key=lambda x: x['elo_rating'], reverse=True)
        for i, entry in enumerate(leaderboard, 1):
            entry['rank'] = i

        return leaderboard

    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive arena statistics."""
        return {
            'arena_available': is_arena_available(),
            'active_models': len(self.available_models),
            'total_battles': sum(self.model_usage_stats.values()) // 2,
            'completed_battles': sum(self.preference_stats.values()),
            'preference_distribution': self.preference_stats,
            'model_usage': self.model_usage_stats,
            'leaderboard': self.get_leaderboard(),
            'storage_status': 'Redis' if self.storage.connected else 'Fallback',
        }

    def _save_stats(self):
        """Save statistics to disk."""
        stats_file = 'instance/arena_stats.json'
        os.makedirs('instance', exist_ok=True)
        try:
            with open(stats_file, 'w') as f:
                json.dump({
                    'elo_ratings': self.elo_system.ratings,
                    'match_history': self.elo_system.match_history[-1000:],
                    'model_usage': self.model_usage_stats,
                    'preference_stats': self.preference_stats,
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save arena stats: {e}")

    def _load_stats(self):
        """Load statistics from disk if available."""
        stats_file = 'instance/arena_stats.json'
        if os.path.exists(stats_file):
            try:
                with open(stats_file, 'r') as f:
                    data = json.load(f)
                    self.elo_system.ratings = data.get('elo_ratings', {})
                    self.elo_system.match_history = data.get('match_history', [])
                    self.model_usage_stats.update(data.get('model_usage', {}))
                    self.preference_stats.update(data.get('preference_stats', {}))
                logger.info("Loaded arena stats from disk")
            except Exception as e:
                logger.error(f"Failed to load arena stats: {e}")
