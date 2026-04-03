#!/usr/bin/env python3
"""
Test Redis connectivity directly on AWS instance
Run this ON the EC2 instance to check Redis
"""

import sys
import os
import json

# Add parent directory to path
sys.path.insert(0, '/home/ec2-user/ChatMRPT')

def test_redis_on_aws():
    """Test Redis from within AWS VPC"""

    print("\n🔍 REDIS TEST ON AWS INSTANCE")
    print("=" * 60)

    # Check environment variables
    print("Environment variables:")
    print(f"REDIS_HOST: {os.environ.get('REDIS_HOST', 'NOT SET')}")
    print(f"REDIS_PORT: {os.environ.get('REDIS_PORT', 'NOT SET')}")
    print(f"REDIS_DB: {os.environ.get('REDIS_DB', 'NOT SET')}")

    try:
        # Import Redis manager
        from app.services.redis_state import get_redis_state_manager

        print("\n✅ Imported redis_state_manager successfully")

        # Get the manager
        redis_mgr = get_redis_state_manager()
        print("✅ Got Redis manager instance")

        # Check if client is connected
        if redis_mgr._client:
            print("✅ Redis client exists")

            # Test basic operations
            test_session = "test-session-aws"

            # Test write
            print(f"\nTesting write for session: {test_session}")
            success = redis_mgr.set_custom_data(test_session, "test_key", {"test": "data"})
            print(f"Write result: {'✅ Success' if success else '❌ Failed'}")

            # Test read
            print("Testing read...")
            data = redis_mgr.get_custom_data(test_session, "test_key")
            print(f"Read result: {data}")

            # Test conversation state
            print("\nTesting conversation state storage...")
            conv_state = {
                "history": [
                    {"user": "Test message 1", "assistant": "Test response 1"},
                    {"user": "Test message 2", "assistant": "Test response 2"}
                ],
                "last_message": "Test message 2"
            }

            success = redis_mgr.set_conversation_state(test_session, conv_state)
            print(f"Store conversation: {'✅ Success' if success else '❌ Failed'}")

            # Retrieve it
            retrieved = redis_mgr.get_conversation_state(test_session)
            print(f"Retrieved conversation: {retrieved}")

            if retrieved:
                print(f"History entries: {len(retrieved.get('history', []))}")

            print("\n✅ Redis is working on AWS instance!")
        else:
            print("❌ Redis client is None - connection failed")

            # Try manual connection
            print("\nTrying manual Redis connection...")
            import redis

            host = os.environ.get('REDIS_HOST', 'chatmrpt-redis-staging.1b3pmt.0001.use2.cache.amazonaws.com')
            port = int(os.environ.get('REDIS_PORT', '6379'))

            print(f"Connecting to {host}:{port}")

            client = redis.StrictRedis(
                host=host,
                port=port,
                db=0,
                socket_connect_timeout=5,
                socket_timeout=5,
                decode_responses=True
            )

            client.ping()
            print("✅ Manual connection successful!")

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're in the ChatMRPT directory")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_redis_on_aws()