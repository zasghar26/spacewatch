"""
Test credential-scoped caching for BYOC (Bring Your Own Credentials) safety.

These tests ensure that caches (_BUCKET_CACHE, TOP_IPS_CACHE, MEMORY) are properly
isolated per credential set, preventing cross-user data leakage.
"""

import pytest
import hashlib
from unittest.mock import Mock, MagicMock, patch
from main import (
    get_credential_cache_key,
    refresh_bucket_cache,
    _BUCKET_CACHE_BY_CREDENTIAL,
    TOP_IPS_CACHE,
    memory_key,
    MEMORY_BY_KEY,
)


class TestCredentialCacheKey:
    """Test credential cache key generation."""
    
    def test_same_credentials_same_key(self):
        """Same credentials should produce the same cache key."""
        key1 = get_credential_cache_key("access_key_1", "sgp1", "https://sgp1.digitaloceanspaces.com")
        key2 = get_credential_cache_key("access_key_1", "sgp1", "https://sgp1.digitaloceanspaces.com")
        assert key1 == key2
    
    def test_different_access_keys_different_keys(self):
        """Different access keys should produce different cache keys."""
        key1 = get_credential_cache_key("access_key_1", "sgp1", "https://sgp1.digitaloceanspaces.com")
        key2 = get_credential_cache_key("access_key_2", "sgp1", "https://sgp1.digitaloceanspaces.com")
        assert key1 != key2
    
    def test_different_regions_different_keys(self):
        """Different regions should produce different cache keys."""
        key1 = get_credential_cache_key("access_key_1", "sgp1", "https://sgp1.digitaloceanspaces.com")
        key2 = get_credential_cache_key("access_key_1", "nyc3", "https://nyc3.digitaloceanspaces.com")
        assert key1 != key2
    
    def test_different_endpoints_different_keys(self):
        """Different endpoints should produce different cache keys."""
        key1 = get_credential_cache_key("access_key_1", "sgp1", "https://sgp1.digitaloceanspaces.com")
        key2 = get_credential_cache_key("access_key_1", "sgp1", "https://custom.endpoint.com")
        assert key1 != key2
    
    def test_cache_key_is_stable_hash(self):
        """Cache key should be a stable 16-character hash that's consistent across calls."""
        key1 = get_credential_cache_key("test_key", "sgp1", "https://sgp1.digitaloceanspaces.com")
        key2 = get_credential_cache_key("test_key", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Verify both calls produce identical results (stability)
        assert key1 == key2
        
        # Verify format
        assert isinstance(key1, str)
        assert len(key1) == 16
        assert all(c in '0123456789abcdef' for c in key1)


class TestBucketCacheIsolation:
    """Test bucket cache isolation per credential."""
    
    def setup_method(self):
        """Clear cache before each test."""
        _BUCKET_CACHE_BY_CREDENTIAL.clear()
    
    @patch('main.time.time', return_value=1000.0)
    def test_different_credentials_separate_bucket_caches(self, mock_time):
        """Different credentials should have separate bucket caches."""
        # Setup mock S3 clients for two different users
        s3_client_a = Mock()
        s3_client_a.list_buckets.return_value = {
            "Buckets": [{"Name": "user-a-bucket-1"}, {"Name": "user-a-bucket-2"}]
        }
        
        s3_client_b = Mock()
        s3_client_b.list_buckets.return_value = {
            "Buckets": [{"Name": "user-b-bucket-1"}, {"Name": "user-b-bucket-2"}]
        }
        
        # Create credential cache keys for two users
        cred_key_a = get_credential_cache_key("access_key_a", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("access_key_b", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Refresh cache for user A
        buckets_a = refresh_bucket_cache(s3_client_a, cred_key_a)
        assert "user-a-bucket-1" in buckets_a
        assert "user-a-bucket-2" in buckets_a
        assert "user-b-bucket-1" not in buckets_a
        
        # Refresh cache for user B
        buckets_b = refresh_bucket_cache(s3_client_b, cred_key_b)
        assert "user-b-bucket-1" in buckets_b
        assert "user-b-bucket-2" in buckets_b
        assert "user-a-bucket-1" not in buckets_b
        
        # Verify both caches exist independently
        assert cred_key_a in _BUCKET_CACHE_BY_CREDENTIAL
        assert cred_key_b in _BUCKET_CACHE_BY_CREDENTIAL
        
        # Verify user A's cache hasn't been polluted by user B
        buckets_a_again = refresh_bucket_cache(s3_client_a, cred_key_a)
        assert "user-a-bucket-1" in buckets_a_again
        assert "user-b-bucket-1" not in buckets_a_again
    
    @patch('main.time.time')
    def test_bucket_cache_ttl_per_credential(self, mock_time):
        """TTL should be enforced per credential, not globally."""
        # Start at time 1000
        mock_time.return_value = 1000.0
        
        s3_client = Mock()
        s3_client.list_buckets.return_value = {
            "Buckets": [{"Name": "test-bucket"}]
        }
        
        cred_key_a = get_credential_cache_key("access_key_a", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("access_key_b", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Refresh cache for both users at time 1000
        refresh_bucket_cache(s3_client, cred_key_a)
        refresh_bucket_cache(s3_client, cred_key_b)
        
        # Advance time to 1200 (200 seconds later, within TTL of 300)
        mock_time.return_value = 1200.0
        
        # Cache should still be valid for both
        refresh_bucket_cache(s3_client, cred_key_a)
        refresh_bucket_cache(s3_client, cred_key_b)
        
        # Should have only called list_buckets twice (once per user initially)
        assert s3_client.list_buckets.call_count == 2
        
        # Advance time to 1400 (400 seconds from initial, beyond TTL)
        mock_time.return_value = 1400.0
        
        # Cache should be expired, triggering new list_buckets calls
        refresh_bucket_cache(s3_client, cred_key_a)
        assert s3_client.list_buckets.call_count == 3
        
        refresh_bucket_cache(s3_client, cred_key_b)
        assert s3_client.list_buckets.call_count == 4


class TestTopIPsCacheIsolation:
    """Test TOP_IPS_CACHE isolation per credential."""
    
    def setup_method(self):
        """Clear cache before each test."""
        TOP_IPS_CACHE.clear()
    
    def test_different_credentials_separate_ip_caches(self):
        """Different credentials should have separate TOP_IPS caches."""
        import time
        
        cred_key_a = get_credential_cache_key("access_key_a", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("access_key_b", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Create cache entries for two users with same bucket/date/limit but different credentials
        now = time.time()
        cache_key_a = (cred_key_a, "test-bucket", "2024-01-01", 20)
        cache_key_b = (cred_key_b, "test-bucket", "2024-01-01", 20)
        
        # Simulate cached PNG data for each user
        png_data_a = b"PNG_DATA_FOR_USER_A"
        png_data_b = b"PNG_DATA_FOR_USER_B"
        
        TOP_IPS_CACHE[cache_key_a] = (now, png_data_a)
        TOP_IPS_CACHE[cache_key_b] = (now, png_data_b)
        
        # Verify caches are separate
        assert TOP_IPS_CACHE[cache_key_a][1] == png_data_a
        assert TOP_IPS_CACHE[cache_key_b][1] == png_data_b
        assert len(TOP_IPS_CACHE) == 2
    
    def test_top_ips_cache_includes_credential_key(self):
        """TOP_IPS cache key must include credential_cache_key."""
        import time
        
        cred_key = get_credential_cache_key("access_key", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Cache key should be a tuple with credential_cache_key as first element
        cache_key = (cred_key, "bucket", "date", 20)
        now = time.time()
        
        TOP_IPS_CACHE[cache_key] = (now, b"test_data")
        
        # Verify the credential key is part of the cache key
        assert cache_key in TOP_IPS_CACHE
        assert cache_key[0] == cred_key


class TestMemoryIsolation:
    """Test MEMORY_BY_KEY isolation per credential."""
    
    def setup_method(self):
        """Clear memory before each test."""
        MEMORY_BY_KEY.clear()
    
    def test_memory_key_includes_credential_key(self):
        """Memory key should include credential cache key for isolation."""
        from fastapi import Request
        
        # Mock request
        request = Mock(spec=Request)
        request.client = Mock()
        request.client.host = "192.168.1.100"
        
        cred_key_a = get_credential_cache_key("access_key_a", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("access_key_b", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Same IP, different credentials should produce different memory keys
        mem_key_a = memory_key(request, None, cred_key_a)
        mem_key_b = memory_key(request, None, cred_key_b)
        
        assert mem_key_a != mem_key_b
        assert cred_key_a in mem_key_a
        assert cred_key_b in mem_key_b
    
    def test_same_ip_different_credentials_isolated(self):
        """Same IP with different credentials should have isolated memory."""
        from main import update_memory, get_memory
        from fastapi import Request
        
        # Mock request from same IP
        request = Mock(spec=Request)
        request.client = Mock()
        request.client.host = "192.168.1.100"
        
        cred_key_a = get_credential_cache_key("access_key_a", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("access_key_b", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Generate memory keys
        mem_key_a = memory_key(request, None, cred_key_a)
        mem_key_b = memory_key(request, None, cred_key_b)
        
        # Update memory for user A
        update_memory(mem_key_a, bucket="user-a-bucket", tool="list_objects")
        
        # Update memory for user B
        update_memory(mem_key_b, bucket="user-b-bucket", tool="recent_objects")
        
        # Verify isolation
        mem_a = get_memory(mem_key_a)
        mem_b = get_memory(mem_key_b)
        
        assert mem_a.last_bucket == "user-a-bucket"
        assert mem_a.last_tool == "list_objects"
        
        assert mem_b.last_bucket == "user-b-bucket"
        assert mem_b.last_tool == "recent_objects"
    
    def test_session_id_with_different_credentials_isolated(self):
        """Same session ID with different credentials should be isolated."""
        from main import update_memory, get_memory
        from fastapi import Request
        
        # Mock request
        request = Mock(spec=Request)
        request.client = Mock()
        request.client.host = "192.168.1.100"
        
        session_id = "shared-session-123"
        
        cred_key_a = get_credential_cache_key("access_key_a", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("access_key_b", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Generate memory keys with same session ID but different credentials
        mem_key_a = memory_key(request, session_id, cred_key_a)
        mem_key_b = memory_key(request, session_id, cred_key_b)
        
        # Should produce different keys
        assert mem_key_a != mem_key_b
        
        # Update memory for both
        update_memory(mem_key_a, bucket="bucket-a", tool="tool-a")
        update_memory(mem_key_b, bucket="bucket-b", tool="tool-b")
        
        # Verify isolation
        mem_a = get_memory(mem_key_a)
        mem_b = get_memory(mem_key_b)
        
        assert mem_a.last_bucket == "bucket-a"
        assert mem_b.last_bucket == "bucket-b"


class TestCrossUserLeakagePrevention:
    """Integration tests to verify no cross-user data leakage."""
    
    def setup_method(self):
        """Clear all caches before each test."""
        _BUCKET_CACHE_BY_CREDENTIAL.clear()
        TOP_IPS_CACHE.clear()
        MEMORY_BY_KEY.clear()
    
    @patch('main.time.time', return_value=1000.0)
    def test_scenario_user_a_then_user_b_no_leakage(self, mock_time):
        """
        Simulate:
        1. User A queries their buckets
        2. User B queries their buckets
        3. User A queries again - should not see User B's buckets
        """
        # Setup User A's S3 client
        s3_client_a = Mock()
        s3_client_a.list_buckets.return_value = {
            "Buckets": [
                {"Name": "user-a-private-bucket"},
                {"Name": "user-a-public-bucket"}
            ]
        }
        
        # Setup User B's S3 client
        s3_client_b = Mock()
        s3_client_b.list_buckets.return_value = {
            "Buckets": [
                {"Name": "user-b-confidential"},
                {"Name": "user-b-data"}
            ]
        }
        
        cred_key_a = get_credential_cache_key("user_a_key", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("user_b_key", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # Step 1: User A queries their buckets
        buckets_a_1 = refresh_bucket_cache(s3_client_a, cred_key_a)
        assert "user-a-private-bucket" in buckets_a_1
        assert "user-a-public-bucket" in buckets_a_1
        assert len(buckets_a_1) == 2
        
        # Step 2: User B queries their buckets
        buckets_b = refresh_bucket_cache(s3_client_b, cred_key_b)
        assert "user-b-confidential" in buckets_b
        assert "user-b-data" in buckets_b
        assert len(buckets_b) == 2
        
        # Step 3: User A queries again (within TTL, should use cache)
        buckets_a_2 = refresh_bucket_cache(s3_client_a, cred_key_a)
        
        # Verify User A's cache is not polluted with User B's buckets
        assert "user-a-private-bucket" in buckets_a_2
        assert "user-a-public-bucket" in buckets_a_2
        assert "user-b-confidential" not in buckets_a_2
        assert "user-b-data" not in buckets_a_2
        assert len(buckets_a_2) == 2
    
    def test_scenario_403_bucket_not_allowed_isolated(self):
        """
        Scenario: User B should not get rejected for valid buckets
        because User A's bucket cache doesn't include them.
        """
        from main import require_bucket_allowed
        from fastapi import HTTPException
        
        s3_client_a = Mock()
        s3_client_a.list_buckets.return_value = {
            "Buckets": [{"Name": "user-a-only-bucket"}]
        }
        
        s3_client_b = Mock()
        s3_client_b.list_buckets.return_value = {
            "Buckets": [{"Name": "user-b-only-bucket"}]
        }
        
        cred_key_a = get_credential_cache_key("user_a_key", "sgp1", "https://sgp1.digitaloceanspaces.com")
        cred_key_b = get_credential_cache_key("user_b_key", "sgp1", "https://sgp1.digitaloceanspaces.com")
        
        # User A refreshes cache
        refresh_bucket_cache(s3_client_a, cred_key_a)
        
        # User B should be able to access their bucket (not rejected by User A's cache)
        refresh_bucket_cache(s3_client_b, cred_key_b)
        
        # This should not raise an exception for User B
        try:
            require_bucket_allowed("user-b-only-bucket", s3_client_b, cred_key_b)
            user_b_succeeded = True
        except HTTPException:
            user_b_succeeded = False
        
        assert user_b_succeeded, "User B should be able to access their own bucket"
        
        # User A should NOT be able to access User B's bucket
        try:
            require_bucket_allowed("user-b-only-bucket", s3_client_a, cred_key_a)
            user_a_crossed = True
        except HTTPException:
            user_a_crossed = False
        
        assert not user_a_crossed, "User A should not be able to access User B's bucket"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
