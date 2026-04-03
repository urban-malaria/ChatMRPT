"""
Simple per-session memory service with fast summaries.

Goals:
- Provide compact memory summary for classifier and analyze-data agent
- Persist lightweight conversation context per session
- File-based default; can be swapped for Redis-backed implementation later
"""

import json
import os
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional

from app.core.unified_data_state import get_data_state
from app.core.redis_state_manager import get_redis_state_manager


class MemoryService:
    def __init__(self, base_dir: str = "instance/memory", max_messages: int = None):
        self.base_dir = base_dir
        # Allow env override for retention length
        try:
            env_max = int(os.getenv('CHATMRPT_MEMORY_MAX_MESSAGES', '100'))
        except Exception:
            env_max = 100
        self.max_messages = max_messages or env_max
        os.makedirs(self.base_dir, exist_ok=True)
        self._lock = threading.Lock()
        # Optional Redis backing
        self._use_redis = os.getenv('CHATMRPT_USE_REDIS_MEMORY', '0') == '1'
        self._redis_mgr = None
        if self._use_redis:
            try:
                self._redis_mgr = get_redis_state_manager()
            except Exception:
                self._use_redis = False

    def _path(self, session_id: str) -> str:
        return os.path.join(self.base_dir, f"{session_id}.json")

    def append_message(self, session_id: str, role: str, content: str, meta: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            record = self._read(session_id)
            msgs = record.get('messages', [])
            msgs.append({
                't': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
                'role': role,
                'content': content[:2000],  # keep small
                'meta': meta or {}
            })
            # Trim
            if len(msgs) > self.max_messages:
                msgs = msgs[-self.max_messages:]
            record['messages'] = msgs
            self._write(session_id, record)

    def set_fact(self, session_id: str, key: str, value: Any) -> None:
        with self._lock:
            record = self._read(session_id)
            facts = record.get('facts', {})
            facts[key] = value
            record['facts'] = facts
            self._write(session_id, record)

    def get_fact(self, session_id: str, key: str, default: Any = None) -> Any:
        with self._lock:
            record = self._read(session_id)
            facts = record.get('facts', {})
            return facts.get(key, default)

    def get_all_facts(self, session_id: str) -> Dict[str, Any]:
        with self._lock:
            record = self._read(session_id)
            return dict(record.get('facts', {}))

    def clear_session(self, session_id: str) -> None:
        with self._lock:
            path = self._path(session_id)
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

    def get_summary(self, session_id: str) -> str:
        """
        Compact summary string used by the intent classifier and agent.
        Includes:
        - Data stage (no_data/pre_analysis/post_analysis), flags
        - Basic shape and first few columns
        - Last user intent-ish snippets
        """
        try:
            state = get_data_state(session_id)
        except Exception:
            state = None

        pieces: List[str] = []
        # Data stage
        stage = 'unknown'
        data_loaded = False
        if state is not None:
            try:
                stage = state.get_stage()
                data_loaded = state.data_loaded
            except Exception:
                pass
        pieces.append(f"stage={stage}")
        pieces.append(f"data_loaded={bool(data_loaded)}")

        # Shape and columns (lightweight)
        try:
            df = state.current_data if state else None
            if df is not None:
                cols = list(df.columns)
                pieces.append(f"shape={df.shape[0]}x{df.shape[1]}")
                preview_cols = ','.join(cols[:8])
                pieces.append(f"cols={preview_cols}")
        except Exception:
            pass

        # Recent messages (very compact)
        record = self._read(session_id)
        msgs = record.get('messages', [])[-3:]  # last 3 snippets
        if msgs:
            last_bits = []
            for m in msgs:
                role = m.get('role', 'user')
                text = (m.get('content') or '').replace('\n', ' ')[:120]
                last_bits.append(f"{role}:{text}")
            pieces.append('history=' + ' | '.join(last_bits))

        return '; '.join(pieces)

    def get_messages(self, session_id: str) -> List[Dict[str, Any]]:
        """Return persisted messages (trimmed) for a session."""
        rec = self._read(session_id)
        msgs = rec.get('messages', [])
        if len(msgs) > self.max_messages:
            msgs = msgs[-self.max_messages:]
        return msgs

    # --- Internal helpers ---
    def _read(self, session_id: str) -> Dict[str, Any]:
        if self._use_redis and self._redis_mgr:
            try:
                data = self._redis_mgr.get_custom_data(session_id, 'memory')
                return data or {}
            except Exception:
                pass
        path = self._path(session_id)
        if not os.path.exists(path):
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def _write(self, session_id: str, data: Dict[str, Any]) -> None:
        if self._use_redis and self._redis_mgr:
            try:
                self._redis_mgr.set_custom_data(session_id, 'memory', data)
                return
            except Exception:
                pass
        path = self._path(session_id)
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)


_memory_service: Optional[MemoryService] = None


def get_memory_service() -> MemoryService:
    global _memory_service
    if _memory_service is None:
        _memory_service = MemoryService()
    return _memory_service
