#!/usr/bin/env python3
"""
Comprehensive test suite for RedisBitmap to verify correctness and performance.
Tests include different offsets, large batches, edge cases, concurrent operations,
various bit patterns, performance under load, and corruption detection.
"""
import asyncio
import random
import time
from typing import Set
from redis import asyncio as aioredis
from snapshotter.utils.redis.redis_bitmap import RedisBitmap


class BitmapTestSuite:
    """Comprehensive test suite for RedisBitmap functionality."""
    
    def __init__(self):
        self.redis_conn = None
        self.test_keys = []
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
        
    async def setup(self):
        """Initialize Redis connection."""
        self.redis_conn = aioredis.Redis(host="localhost", port=6379, db=0)
        try:
            await self.redis_conn.ping()
            print("✅ Redis connection established")
        except Exception as e:
            print(f"❌ Failed to connect to Redis: {e}")
            raise
            
    async def cleanup(self):
        """Clean up test data and close connections."""
        if self.redis_conn:
            # Clean up all test keys
            for key in self.test_keys:
                await self.redis_conn.delete(key)
            await self.redis_conn.close()
        print(f"\n🧹 Cleanup completed - removed {len(self.test_keys)} test keys")
        
    def register_test_key(self, key: str):
        """Register a test key for cleanup."""
        if key not in self.test_keys:
            self.test_keys.append(key)
            
    async def run_test(self, test_name: str, test_func):
        """Run a single test with error handling and reporting."""
        self.total_tests += 1
        try:
            print(f"\n🧪 Running {test_name}...")
            await test_func()
            self.passed_tests += 1
            print(f"✅ {test_name} PASSED")
        except Exception as e:
            self.failed_tests += 1
            print(f"❌ {test_name} FAILED: {e}")
            import traceback
            traceback.print_exc()
            
    async def verify_integrity(self, bitmap: RedisBitmap, key: str, expected_bits: Set[int], test_name: str):
        """Verify bitmap integrity by checking all expected bits are set and no extras exist."""
        print(f"  🔍 Verifying integrity for {test_name}")
        
        # Check all expected bits are set
        for bit in expected_bits:
            result = await bitmap.get_bit(self.redis_conn, key, bit)
            if not result:
                raise AssertionError(f"Expected bit {bit} to be set but it wasn't")
                
        # Check bit count matches expected
        bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        if bit_count != len(expected_bits):
            raise AssertionError(f"Expected {len(expected_bits)} bits set, got {bit_count}")
            
        print(f"  ✅ Integrity verified: {len(expected_bits)} bits correctly set")

    async def test_basic_functionality_no_offset(self):
        """Test basic functionality with zero offset."""
        bitmap = RedisBitmap(epoch_offset=0)
        key = "test_basic_no_offset"
        self.register_test_key(key)
        
        # Test single bit operations
        await bitmap.set_bit(self.redis_conn, key, 100)
        result = await bitmap.get_bit(self.redis_conn, key, 100)
        assert result, "Single bit should be set"
        
        # Test batch operations
        bits_to_set = [200, 201, 202, 300, 500]
        await bitmap.set_bits(self.redis_conn, key, bits_to_set)
        
        results = await bitmap.get_bits(self.redis_conn, key, bits_to_set)
        for bit_id, is_set in results:
            assert is_set, f"Bit {bit_id} should be set"
            
        # Test range operations
        range_bits = [1000, 1001, 1002, 1010, 1020]
        await bitmap.set_bits_in_range(self.redis_conn, key, range_bits)
        
        range_results = await bitmap.get_bits_in_range(self.redis_conn, key, range_bits)
        for bit_id, is_set in range_results:
            assert is_set, f"Range bit {bit_id} should be set"
            
        expected_bits = {100} | set(bits_to_set) | set(range_bits)
        await self.verify_integrity(bitmap, key, expected_bits, "basic_no_offset")

    async def test_various_offsets(self):
        """Test functionality with different epoch offsets."""
        offsets = [0, 100, 1000, 10000, 100000]
        
        for offset in offsets:
            bitmap = RedisBitmap(epoch_offset=offset)
            key = f"test_offset_{offset}"
            self.register_test_key(key)
            
            # Test bits around the offset
            test_bits = [offset, offset + 1, offset + 10, offset + 100, offset + 1000]
            
            # Set bits using different methods
            await bitmap.set_bit(self.redis_conn, key, test_bits[0])
            await bitmap.set_bits(self.redis_conn, key, test_bits[1:3])
            await bitmap.set_bits_in_range(self.redis_conn, key, test_bits[3:])
            
            # Verify all bits are set
            for bit in test_bits:
                result = await bitmap.get_bit(self.redis_conn, key, bit)
                assert result, f"Bit {bit} should be set with offset {offset}"
                
            await self.verify_integrity(bitmap, key, set(test_bits), f"offset_{offset}")

    async def test_large_batch_operations(self):
        """Test large batch operations to ensure performance and correctness."""
        bitmap = RedisBitmap(epoch_offset=0, max_batch_size=50000)
        key = "test_large_batch"
        self.register_test_key(key)
        
        # Test various batch sizes
        batch_sizes = [1000, 5000, 10000, 25000, 50000]
        
        for batch_size in batch_sizes:
            print(f"  📦 Testing batch size: {batch_size}")
            
            # Generate random bits with some clustering
            base_range = random.randint(10000, 100000)
            bits = []
            
            # Add some clustered bits (good for range operations)
            for i in range(batch_size // 2):
                bits.append(base_range + i)
                
            # Add some sparse bits
            for i in range(batch_size // 2):
                bits.append(random.randint(base_range + 10000, base_range + 100000))
                
            bits = list(set(bits))  # Remove duplicates
            
            start_time = time.time()
            
            # Test set_bits
            changed = await bitmap.set_bits(self.redis_conn, key, bits)
            print(f"    set_bits: {len(bits)} bits in {time.time() - start_time:.3f}s")
            
            # Test set_bits_in_range
            start_time = time.time()
            await bitmap.set_bits_in_range(self.redis_conn, key, bits)
            print(f"    set_bits_in_range: {len(bits)} bits in {time.time() - start_time:.3f}s")
            
            # Test get_bits
            start_time = time.time()
            results = await bitmap.get_bits(self.redis_conn, key, bits)
            print(f"    get_bits: {len(bits)} bits in {time.time() - start_time:.3f}s")
            
            for bit_id, is_set in results:
                assert is_set, f"Bit {bit_id} should be set in batch {batch_size}"
                
            # Test get_bits_in_range
            start_time = time.time()
            range_results = await bitmap.get_bits_in_range(self.redis_conn, key, bits)
            print(f"    get_bits_in_range: {len(bits)} bits in {time.time() - start_time:.3f}s")
            
            for bit_id, is_set in range_results:
                assert is_set, f"Range bit {bit_id} should be set in batch {batch_size}"
                
            await bitmap.clear(self.redis_conn, key)

    async def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        bitmap = RedisBitmap(epoch_offset=1000)
        key = "test_edge_cases"
        self.register_test_key(key)
        
        # Test empty operations
        result = await bitmap.set_bits(self.redis_conn, key, [])
        assert result == 0, "Empty set_bits should return 0"
        
        result = await bitmap.get_bits(self.redis_conn, key, [])
        assert result == [], "Empty get_bits should return empty list"
        
        # Test single bit operations
        await bitmap.set_bit(self.redis_conn, key, 1000)  # At offset
        await bitmap.set_bit(self.redis_conn, key, 1001)  # Just after offset
        
        # Test byte boundaries (bits 0, 7, 8, 15, 16, etc. within a byte)
        byte_boundary_bits = [1000 + i for i in [0, 7, 8, 15, 16, 23, 24, 31, 32]]
        await bitmap.set_bits_in_range(self.redis_conn, key, byte_boundary_bits)
        
        # Verify all boundary bits
        for bit in byte_boundary_bits:
            result = await bitmap.get_bit(self.redis_conn, key, bit)
            assert result, f"Byte boundary bit {bit} should be set"
            
        # Test large gaps (sparse data)
        sparse_bits = [1000, 10000, 100000, 1000000]
        await bitmap.set_bits(self.redis_conn, key, sparse_bits)
        
        for bit in sparse_bits:
            result = await bitmap.get_bit(self.redis_conn, key, bit)
            assert result, f"Sparse bit {bit} should be set"

    async def test_concurrent_operations(self):
        """Test concurrent operations to ensure thread safety."""
        bitmap = RedisBitmap(epoch_offset=0)
        key = "test_concurrent"
        self.register_test_key(key)
        
        # Prepare different bit ranges for concurrent operations
        ranges = [
            list(range(1000, 2000)),
            list(range(5000, 6000)), 
            list(range(10000, 11000)),
            list(range(20000, 21000)),
            list(range(50000, 51000))
        ]
        
        async def set_range(bit_range):
            await bitmap.set_bits_in_range(self.redis_conn, key, bit_range)
            return bit_range
            
        # Run concurrent operations
        tasks = [set_range(r) for r in ranges]
        results = await asyncio.gather(*tasks)
        
        # Verify all ranges were set correctly
        all_expected_bits = set()
        for bit_range in results:
            all_expected_bits.update(bit_range)
            
        # Check random sample of bits
        sample_bits = random.sample(list(all_expected_bits), min(1000, len(all_expected_bits)))
        results = await bitmap.get_bits(self.redis_conn, key, sample_bits)
        
        for bit_id, is_set in results:
            assert is_set, f"Concurrent bit {bit_id} should be set"
            
        print(f"  ✅ Concurrent operations: {len(all_expected_bits)} bits set correctly")

    async def test_corruption_detection(self):
        """Test for corruption when mixing different operation types."""
        bitmap = RedisBitmap(epoch_offset=0)
        key = "test_corruption"
        self.register_test_key(key)
        
        # Set initial pattern using individual bits
        initial_bits = [100, 101, 102, 103, 104, 105, 106, 107]  # One byte
        for bit in initial_bits:
            await bitmap.set_bit(self.redis_conn, key, bit)
            
        # Verify initial pattern
        for bit in initial_bits:
            result = await bitmap.get_bit(self.redis_conn, key, bit)
            assert result, f"Initial bit {bit} should be set"
            
        # Now use range operation on overlapping bits
        overlapping_bits = [105, 106, 107, 108, 109, 110]  # Overlaps with initial
        await bitmap.set_bits_in_range(self.redis_conn, key, overlapping_bits)
        
        # Verify no corruption occurred
        all_expected = set(initial_bits) | set(overlapping_bits)
        for bit in all_expected:
            result = await bitmap.get_bit(self.redis_conn, key, bit)
            assert result, f"Bit {bit} should still be set after overlap operation"
            
        # Test with batch operations
        batch_bits = [103, 104, 111, 112, 113]
        await bitmap.set_bits(self.redis_conn, key, batch_bits)
        
        # Final verification
        final_expected = all_expected | set(batch_bits)
        await self.verify_integrity(bitmap, key, final_expected, "corruption_detection")
        
        print(f"  ✅ No corruption detected in {len(final_expected)} bits")

    async def test_performance_under_load(self):
        """Test performance under various load conditions."""
        bitmap = RedisBitmap(epoch_offset=0)
        key = "test_performance"
        self.register_test_key(key)
        
        # Test 1: Many small operations
        print("  📊 Testing many small operations...")
        small_ops_start = time.time()
        for i in range(1000):
            await bitmap.set_bit(self.redis_conn, key, i * 10)
        small_ops_time = time.time() - small_ops_start
        print(f"    1000 individual set_bit operations: {small_ops_time:.3f}s")
        
        # Test 2: Few large operations
        print("  📊 Testing few large operations...")
        large_batch = list(range(50000, 100000))
        large_ops_start = time.time()
        await bitmap.set_bits_in_range(self.redis_conn, key, large_batch)
        large_ops_time = time.time() - large_ops_start
        print(f"    1 large batch operation (50k bits): {large_ops_time:.3f}s")
        
        # Test 3: Mixed read/write operations
        print("  📊 Testing mixed read/write operations...")
        mixed_start = time.time()
        
        # Interleave reads and writes
        for i in range(100):
            # Write some bits
            write_bits = [200000 + i * 10 + j for j in range(5)]
            await bitmap.set_bits(self.redis_conn, key, write_bits)
            
            # Read some bits
            read_bits = [j for j in range(i * 10, (i + 1) * 10)]
            await bitmap.get_bits(self.redis_conn, key, read_bits)
            
        mixed_time = time.time() - mixed_start
        print(f"    100 mixed read/write cycles: {mixed_time:.3f}s")
        
        # Verify final state
        bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        print(f"    Final bit count: {bit_count}")

    async def test_error_handling(self):
        """Test error handling and validation."""
        # Test invalid parameters
        try:
            bitmap = RedisBitmap(epoch_offset="invalid")
            assert False, "Should raise TypeError for invalid offset"
        except ValueError:
            pass
            
        bitmap = RedisBitmap(epoch_offset=100)
        key = "test_errors"
        self.register_test_key(key)
        
        # Test invalid epoch_id (below offset)
        try:
            await bitmap.set_bit(self.redis_conn, key, 50)  # Below offset of 100
            assert False, "Should raise ValueError for epoch_id below offset"
        except ValueError:
            pass
            
        # Test empty key
        try:
            await bitmap.set_bit(self.redis_conn, "", 100)
            assert False, "Should raise ValueError for empty key"
        except ValueError:
            pass
            
        # Test invalid connection
        try:
            await bitmap.set_bit("invalid_conn", key, 100)
            assert False, "Should raise TypeError for invalid connection"
        except TypeError:
            pass
            
        print("  ✅ Error handling tests passed")

    async def test_memory_efficiency(self):
        """Test memory efficiency with sparse data."""
        bitmap = RedisBitmap(epoch_offset=0)
        key = "test_memory"
        self.register_test_key(key)
        
        # Set sparse bits across a large range
        sparse_bits = [i * 1000000 for i in range(10)]  # 10 bits across 10M range
        
        start_time = time.time()
        await bitmap.set_bits_in_range(self.redis_conn, key, sparse_bits)
        set_time = time.time() - start_time
        
        # Verify bits are set
        results = await bitmap.get_bits_in_range(self.redis_conn, key, sparse_bits)
        for bit_id, is_set in results:
            assert is_set, f"Sparse bit {bit_id} should be set"
            
        bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        assert bit_count == len(sparse_bits), f"Expected {len(sparse_bits)} bits, got {bit_count}"
        
        print(f"  📏 Sparse data test: {len(sparse_bits)} bits across 10M range in {set_time:.3f}s")

    async def test_cleanup_old_bits(self):
        """Test automatic cleanup of old bits functionality."""
        bitmap = RedisBitmap(epoch_offset=1000)
        key = "test_cleanup_old_bits"
        self.register_test_key(key)
        
        # Set up old epochs that should be cleaned up
        old_epochs = [1010, 1020, 1030, 1040, 1050, 1060, 1070]
        for epoch_id in old_epochs:
            await bitmap.set_bit(self.redis_conn, key, epoch_id)
        
        # Set up recent epochs that should be kept (within DEFAULT_MAX_EPOCHS_TO_KEEP)
        current_epoch = 1000 + bitmap.DEFAULT_MAX_EPOCHS_TO_KEEP + 100
        recent_epochs = [current_epoch - 50, current_epoch - 30, current_epoch - 10, current_epoch]
        for epoch_id in recent_epochs:
            await bitmap.set_bit(self.redis_conn, key, epoch_id)
        
        # Verify all bits are initially set
        all_epochs = old_epochs + recent_epochs
        for epoch_id in all_epochs:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert result, f"Epoch {epoch_id} should be initially set"
        
        initial_bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        initial_size = await self.redis_conn.strlen(key)
        print(f"  📊 Initial state: {initial_bit_count} bits, {initial_size} bytes")
        
        # Perform cleanup
        bytes_cleaned = await bitmap.cleanup_old_bits(self.redis_conn, key, current_epoch)
        print(f"  🧹 Cleaned up {bytes_cleaned} bytes")
        
        # Verify old epochs are cleared
        for epoch_id in old_epochs:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert not result, f"Old epoch {epoch_id} should be cleared after cleanup"
        
        # Verify recent epochs are still set
        for epoch_id in recent_epochs:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert result, f"Recent epoch {epoch_id} should still be set after cleanup"
        
        final_bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        final_size = await self.redis_conn.strlen(key)
        print(f"  📊 Final state: {final_bit_count} bits, {final_size} bytes")
        
        # Verify bit count decreased correctly
        expected_count = len(recent_epochs)
        assert final_bit_count == expected_count, f"Expected {expected_count} bits after cleanup, got {final_bit_count}"
        assert bytes_cleaned > 0, "Should have cleaned up some bytes"

    async def test_cleanup_with_custom_retention(self):
        """Test cleanup with custom max_epochs_to_keep parameter."""
        bitmap = RedisBitmap(epoch_offset=2000)
        key = "test_cleanup_custom_retention"
        self.register_test_key(key)
        
        # Set bits spanning different time ranges
        base_epoch = 2000
        epochs_to_set = [
            base_epoch + 10,   # Very old
            base_epoch + 50,   # Old
            base_epoch + 150,  # Medium
            base_epoch + 250,  # Recent
            base_epoch + 300   # Current
        ]
        
        for epoch_id in epochs_to_set:
            await bitmap.set_bit(self.redis_conn, key, epoch_id)
        
        current_epoch = base_epoch + 300
        custom_retention = 200  # Keep only last 200 epochs
        
        # Cleanup with custom retention
        bytes_cleaned = await bitmap.cleanup_old_bits(
            self.redis_conn, key, current_epoch, max_epochs_to_keep=custom_retention
        )
        
        # Calculate which epochs should be kept (current_epoch - custom_retention or newer)
        cutoff_epoch = current_epoch - custom_retention  # 2300 - 200 = 2100
        
        for epoch_id in epochs_to_set:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            if epoch_id >= cutoff_epoch:
                assert result, f"Epoch {epoch_id} should be kept (>= cutoff {cutoff_epoch})"
            else:
                assert not result, f"Epoch {epoch_id} should be cleared (< cutoff {cutoff_epoch})"
        
        print(f"  🎯 Custom retention test: cutoff at {cutoff_epoch}, cleaned {bytes_cleaned} bytes")

    async def test_cleanup_edge_cases(self):
        """Test cleanup edge cases and boundary conditions."""
        bitmap = RedisBitmap(epoch_offset=5000)
        key = "test_cleanup_edge_cases"
        self.register_test_key(key)
        
        # Test cleanup when no cleanup is needed (current epoch is too low)
        current_epoch = 5100  # Only 100 epochs after offset
        await bitmap.set_bit(self.redis_conn, key, 5050)
        
        bytes_cleaned = await bitmap.cleanup_old_bits(self.redis_conn, key, current_epoch)
        assert bytes_cleaned == 0, "Should not clean anything when cutoff is before epoch_offset"
        
        # Verify bit is still set
        result = await bitmap.get_bit(self.redis_conn, key, 5050)
        assert result, "Bit should still be set when no cleanup occurred"
        
        # Test cleanup on empty bitmap
        empty_key = "test_cleanup_empty"
        self.register_test_key(empty_key)
        
        bytes_cleaned = await bitmap.cleanup_old_bits(self.redis_conn, empty_key, current_epoch + 200000)
        assert bytes_cleaned == 0, "Should not clean anything from empty bitmap"
        
        # Test cleanup when all bits should be cleared
        await bitmap.set_bit(self.redis_conn, key, 5010)
        very_future_epoch = 5000 + bitmap.DEFAULT_MAX_EPOCHS_TO_KEEP + 1000
        
        initial_size = await self.redis_conn.strlen(key)
        bytes_cleaned = await bitmap.cleanup_old_bits(self.redis_conn, key, very_future_epoch)
        
        # All old bits should be cleared
        result = await bitmap.get_bit(self.redis_conn, key, 5010)
        assert not result, "Old bit should be cleared"
        result = await bitmap.get_bit(self.redis_conn, key, 5050)
        assert not result, "Old bit should be cleared"
        
        print(f"  🎯 Edge cases: cleaned {bytes_cleaned} bytes from {initial_size} byte bitmap")

    async def test_set_bit_with_auto_cleanup(self):
        """Test automatic cleanup when setting bits."""
        bitmap = RedisBitmap(epoch_offset=3000)
        key = "test_auto_cleanup"
        self.register_test_key(key)
        
        # Set some old bits that should be cleaned up
        old_epochs = [3010, 3020, 3030, 3040]
        for epoch_id in old_epochs:
            await bitmap.set_bit(self.redis_conn, key, epoch_id)
        
        initial_bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        assert initial_bit_count == len(old_epochs), "All old bits should be initially set"
        
        # Set a new bit with auto-cleanup (this should trigger cleanup)
        current_epoch = 3000 + bitmap.DEFAULT_MAX_EPOCHS_TO_KEEP + 100
        was_set = await bitmap.set_bit_with_auto_cleanup(
            self.redis_conn, key, current_epoch, max_epochs_to_keep=50
        )
        
        assert was_set, "New bit should have been set"
        
        # Verify the new bit is set
        result = await bitmap.get_bit(self.redis_conn, key, current_epoch)
        assert result, "New bit should be set"
        
        # Verify old bits are cleaned up
        for epoch_id in old_epochs:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert not result, f"Old epoch {epoch_id} should be cleaned up"
        
        final_bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        assert final_bit_count == 1, f"Should have only 1 bit set after auto-cleanup, got {final_bit_count}"
        
        print("  ✅ Auto-cleanup successfully removed old bits while setting new bit")

    async def test_auto_cleanup_error_resilience(self):
        """Test that set_bit_with_auto_cleanup is resilient to cleanup errors."""
        bitmap = RedisBitmap(epoch_offset=4000)
        key = "test_auto_cleanup_resilience"
        self.register_test_key(key)
        
        # Set a bit normally first
        epoch_id = 4100
        was_set = await bitmap.set_bit_with_auto_cleanup(self.redis_conn, key, epoch_id)
        assert was_set, "Bit should be set successfully"
        
        # Verify bit is set even if cleanup might fail
        result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
        assert result, "Bit should be set regardless of cleanup status"
        
        # Test setting same bit again (should return False but not fail)
        was_set_again = await bitmap.set_bit_with_auto_cleanup(self.redis_conn, key, epoch_id)
        assert not was_set_again, "Setting same bit again should return False"
        
        print("  ✅ Auto-cleanup is resilient to errors and doesn't affect bit setting")

    async def test_cleanup_validation(self):
        """Test parameter validation for cleanup methods."""
        bitmap = RedisBitmap(epoch_offset=6000)
        key = "test_cleanup_validation"
        self.register_test_key(key)
        
        # Test invalid current_epoch_id (below offset)
        try:
            await bitmap.cleanup_old_bits(self.redis_conn, key, 5999)  # Below offset
            assert False, "Should raise ValueError for current_epoch_id below offset"
        except ValueError as e:
            assert "must be >= epoch_offset" in str(e)
        
        # Test invalid max_epochs_to_keep
        try:
            await bitmap.cleanup_old_bits(self.redis_conn, key, 6100, max_epochs_to_keep=0)
            assert False, "Should raise ValueError for zero max_epochs_to_keep"
        except ValueError as e:
            assert "must be a positive integer" in str(e)
        
        try:
            await bitmap.cleanup_old_bits(self.redis_conn, key, 6100, max_epochs_to_keep=-10)
            assert False, "Should raise ValueError for negative max_epochs_to_keep"
        except ValueError as e:
            assert "must be a positive integer" in str(e)
        
        # Test invalid types
        try:
            await bitmap.cleanup_old_bits(self.redis_conn, key, "invalid")
            assert False, "Should raise TypeError for non-integer current_epoch_id"
        except TypeError as e:
            assert "must be an integer" in str(e)
        
        print("  ✅ Parameter validation working correctly")

    async def test_cleanup_performance(self):
        """Test cleanup performance with large datasets."""
        bitmap = RedisBitmap(epoch_offset=7000)
        key = "test_cleanup_performance"
        self.register_test_key(key)
        
        # Create a large dataset with both old and recent data
        print("  📦 Setting up large dataset for performance test...")
        
        # Use a smaller retention for this test to make it more predictable
        # Choose numbers that align with byte boundaries to avoid partial byte issues
        max_epochs_to_keep = 1000
        current_epoch = 7000 + max_epochs_to_keep + 500  # epoch 8500
        cutoff_epoch = current_epoch - max_epochs_to_keep  # epoch 7500
        
        # Calculate the actual byte boundary where cleanup will stop
        cutoff_bit_position = cutoff_epoch - 7000  # 500
        cutoff_byte_position = cutoff_bit_position // 8  # 62 bytes
        actual_cleanup_boundary = 7000 + (cutoff_byte_position * 8)  # 7000 + 496 = 7496
        
        print(f"    Cutoff epoch: {cutoff_epoch}, Actual cleanup boundary: {actual_cleanup_boundary}")
        
        # Set old bits that will definitely be cleaned (well before byte boundary)
        old_epochs = [7000 + i for i in range(0, actual_cleanup_boundary - 7000, 3)]  # Every 3rd bit up to cleanup boundary
        start_time = time.time()
        await bitmap.set_bits_in_range(self.redis_conn, key, old_epochs)
        setup_time = time.time() - start_time
        print(f"    Setup {len(old_epochs)} old bits in {setup_time:.3f}s")
        
        # Set recent bits that will definitely be kept (well after byte boundary)
        recent_start = actual_cleanup_boundary + 8  # Start after the byte boundary
        recent_epochs = [recent_start + i for i in range(0, 100, 5)]  # 20 recent bits
        await bitmap.set_bits_in_range(self.redis_conn, key, recent_epochs)
        print(f"    Setup {len(recent_epochs)} recent bits")
        
        initial_size = await self.redis_conn.strlen(key)
        initial_bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        
        print(f"    Current epoch: {current_epoch}, Cutoff epoch: {cutoff_epoch}")
        print(f"    Old epochs range: {min(old_epochs)}-{max(old_epochs)} (will be cleaned)")
        print(f"    Recent epochs range: {min(recent_epochs)}-{max(recent_epochs)} (will be kept)")
        print(f"    Cleanup will clear bytes covering epochs {7000}-{actual_cleanup_boundary-1}")
        
        # Perform cleanup and measure performance
        cleanup_start = time.time()
        bytes_cleaned = await bitmap.cleanup_old_bits(
            self.redis_conn, key, current_epoch, max_epochs_to_keep=max_epochs_to_keep
        )
        cleanup_time = time.time() - cleanup_start
        
        final_size = await self.redis_conn.strlen(key)
        final_bit_count = await bitmap.get_bit_count(self.redis_conn, key)
        
        print("  📊 Performance results:")
        print(f"    Initial: {initial_bit_count} bits, {initial_size} bytes")
        print(f"    Cleanup time: {cleanup_time:.3f}s")
        print(f"    Bytes cleaned: {bytes_cleaned}")
        print(f"    Final: {final_bit_count} bits, {final_size} bytes")
        if cleanup_time > 0:
            cleanup_rate = bytes_cleaned / cleanup_time
            print(f"    Cleanup rate: {cleanup_rate:.0f} bytes/sec")
        else:
            print("    Instant cleanup")
        
        # Verify correctness - only recent bits should remain
        expected_remaining = len(recent_epochs)
        print(f"    Expected remaining bits: {expected_remaining}")
        
        # Verify old bits are cleaned
        sample_old_bits = old_epochs[:10]  # Check first 10 old bits
        for epoch_id in sample_old_bits:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert not result, f"Old epoch {epoch_id} should be cleaned up"
        
        # Verify recent bits are kept
        sample_recent_bits = recent_epochs[:5]  # Check first 5 recent bits
        for epoch_id in sample_recent_bits:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert result, f"Recent epoch {epoch_id} should be kept"
        
        expected = expected_remaining
        assert final_bit_count == expected, f"Expected {expected} bits remaining, got {final_bit_count}"
        assert bytes_cleaned > 0, "Should have cleaned up significant data"

    async def test_byte_boundary_precision(self):
        """Test cleanup precision at byte boundaries to prevent data loss."""
        bitmap = RedisBitmap(epoch_offset=8000)
        key = "test_byte_boundary_precision"
        self.register_test_key(key)
        
        # Test scenario where cutoff falls in the middle of a byte
        current_epoch = 8100
        max_epochs_to_keep = 50  # cutoff at epoch 8050
        cutoff_epoch = current_epoch - max_epochs_to_keep  # 8050
        
        # Bit positions: 8050 - 8000 = 50
        # Byte position: 50 // 8 = 6 (byte 6)
        # Bit position in byte: 50 % 8 = 2 (3rd bit from left in byte 6)
        
        # Set bits that should be cleared (epochs 8000-8049, bits 0-49)
        old_epochs = [8000 + i for i in range(50)]  # bits 0-49
        await bitmap.set_bits_in_range(self.redis_conn, key, old_epochs)
        
        # Set bits that should be kept (epochs 8050+, bits 50+)
        recent_epochs = [8050 + i for i in range(20)]  # bits 50-69
        await bitmap.set_bits_in_range(self.redis_conn, key, recent_epochs)
        
        # Set additional bits in the same byte as the cutoff to test precision
        boundary_bits = [8048, 8049, 8050, 8051]  # bits 48, 49, 50, 51
        for epoch_id in boundary_bits:
            await bitmap.set_bit(self.redis_conn, key, epoch_id)
        
        initial_count = await bitmap.get_bit_count(self.redis_conn, key)
        print(f"    Initial bits: {initial_count}")
        print(f"    Cutoff epoch: {cutoff_epoch} (bit position {cutoff_epoch - 8000})")
        print("    Should clear: epochs 8000-8049 (bits 0-49)")
        print("    Should keep: epochs 8050+ (bits 50+)")
        
        # Perform cleanup
        bytes_cleaned = await bitmap.cleanup_old_bits(
            self.redis_conn, key, current_epoch, max_epochs_to_keep=max_epochs_to_keep
        )
        
        # Verify precise cleanup
        for epoch_id in range(8000, 8050):  # Should be cleared
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert not result, f"Epoch {epoch_id} should be cleared (< cutoff {cutoff_epoch})"
        
        for epoch_id in range(8050, 8070):  # Should be kept
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert result, f"Epoch {epoch_id} should be kept (>= cutoff {cutoff_epoch})"
        
        final_count = await bitmap.get_bit_count(self.redis_conn, key)
        expected_remaining = 20  # epochs 8050-8069
        assert final_count == expected_remaining, f"Expected {expected_remaining} bits remaining, got {final_count}"
        
        print(f"    ✅ Precise cleanup: {initial_count - final_count} bits cleared, {final_count} bits preserved")

    async def test_partial_byte_cleanup(self):
        """Test cleanup when cutoff falls within a byte."""
        bitmap = RedisBitmap(epoch_offset=9000)
        key = "test_partial_byte_cleanup"
        self.register_test_key(key)
        
        # Test multiple scenarios with different bit positions in bytes
        test_cases = [
            (9005, 1),  # cutoff at bit 5, clear 1 bit in first byte
            (9007, 3),  # cutoff at bit 7, clear 3 bits in first byte  
            (9010, 2),  # cutoff at bit 10, clear 2 bits in second byte
            (9015, 7),  # cutoff at bit 15, clear 7 bits in second byte
        ]
        
        for i, (cutoff_epoch, bits_to_clear_in_partial_byte) in enumerate(test_cases):
            subkey = f"{key}_{i}"
            self.register_test_key(subkey)
            
            # Set up a pattern where some bits are old and some are new
            old_epochs = list(range(9000, cutoff_epoch))
            recent_epochs = list(range(cutoff_epoch, cutoff_epoch + 10))
            
            # Set all bits
            all_epochs = old_epochs + recent_epochs
            await bitmap.set_bits_in_range(self.redis_conn, subkey, all_epochs)
            
            current_epoch = cutoff_epoch + 50
            max_epochs_to_keep = 50
            
            initial_count = await bitmap.get_bit_count(self.redis_conn, subkey)
            
            # Perform cleanup
            await bitmap.cleanup_old_bits(
                self.redis_conn, subkey, current_epoch, max_epochs_to_keep=max_epochs_to_keep
            )
            
            # Verify old bits are cleared
            for epoch_id in old_epochs:
                result = await bitmap.get_bit(self.redis_conn, subkey, epoch_id)
                assert not result, f"Case {i}: Old epoch {epoch_id} should be cleared"
            
            # Verify recent bits are kept
            for epoch_id in recent_epochs:
                result = await bitmap.get_bit(self.redis_conn, subkey, epoch_id)
                assert result, f"Case {i}: Recent epoch {epoch_id} should be kept"
            
            final_count = await bitmap.get_bit_count(self.redis_conn, subkey)
            expected_remaining = len(recent_epochs)
            assert final_count == expected_remaining, f"Case {i}: Expected {expected_remaining}, got {final_count}"
        
        print("    ✅ All partial byte cleanup cases passed")

    async def test_no_data_loss_edge_cases(self):
        """Test edge cases to ensure no unintended data loss."""
        bitmap = RedisBitmap(epoch_offset=10000)
        key = "test_no_data_loss"
        self.register_test_key(key)
        
        # Case 1: Cutoff exactly at byte boundary
        cutoff_epoch = 10000 + 64  # Exactly 8 bytes (64 bits)
        current_epoch = cutoff_epoch + 100
        
        # Set bits before and after the boundary
        pre_boundary = list(range(10000, cutoff_epoch))  # Should be cleared
        post_boundary = list(range(cutoff_epoch, cutoff_epoch + 20))  # Should be kept
        
        await bitmap.set_bits_in_range(self.redis_conn, key, pre_boundary + post_boundary)
        
        # Cleanup
        await bitmap.cleanup_old_bits(self.redis_conn, key, current_epoch, max_epochs_to_keep=100)
        
        # Verify boundary precision
        for epoch_id in pre_boundary:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert not result, f"Pre-boundary epoch {epoch_id} should be cleared"
        
        for epoch_id in post_boundary:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            assert result, f"Post-boundary epoch {epoch_id} should be kept"
        
        print("    ✅ Byte boundary case passed")
        
        # Case 2: Single bit cleanup
        await bitmap.clear(self.redis_conn, key)
        
        single_old_bit = 10001
        single_new_bit = 10002
        await bitmap.set_bit(self.redis_conn, key, single_old_bit)
        await bitmap.set_bit(self.redis_conn, key, single_new_bit)
        
        # Cleanup should clear only the first bit
        await bitmap.cleanup_old_bits(self.redis_conn, key, 10003, max_epochs_to_keep=1)
        
        result_old = await bitmap.get_bit(self.redis_conn, key, single_old_bit)
        result_new = await bitmap.get_bit(self.redis_conn, key, single_new_bit)
        
        assert not result_old, "Single old bit should be cleared"
        assert result_new, "Single new bit should be kept"
        
        print("    ✅ Single bit precision case passed")

    async def test_comprehensive_boundary_scenarios(self):
        """Test comprehensive boundary scenarios with different alignments."""
        bitmap = RedisBitmap(epoch_offset=11000)
        
        # Test different bit positions within bytes
        bit_positions = [1, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33]
        
        for bit_pos in bit_positions:
            subkey = f"test_boundary_{bit_pos}"
            self.register_test_key(subkey)
            
            cutoff_epoch = 11000 + bit_pos
            current_epoch = cutoff_epoch + 50
            
            # Create a mixed pattern
            epochs_to_set = []
            
            # Old epochs (should be cleared)
            old_epochs = list(range(11000, cutoff_epoch))
            epochs_to_set.extend(old_epochs)
            
            # Recent epochs (should be kept)
            recent_epochs = list(range(cutoff_epoch, cutoff_epoch + 10))
            epochs_to_set.extend(recent_epochs)
            
            if epochs_to_set:
                await bitmap.set_bits_in_range(self.redis_conn, subkey, epochs_to_set)
                
                # Perform cleanup
                await bitmap.cleanup_old_bits(
                    self.redis_conn, subkey, current_epoch, max_epochs_to_keep=50
                )
                
                # Verify all old epochs are cleared
                for epoch_id in old_epochs:
                    result = await bitmap.get_bit(self.redis_conn, subkey, epoch_id)
                    assert not result, f"Bit pos {bit_pos}: Old epoch {epoch_id} should be cleared"
                
                # Verify all recent epochs are kept
                for epoch_id in recent_epochs:
                    result = await bitmap.get_bit(self.redis_conn, subkey, epoch_id)
                    assert result, f"Bit pos {bit_pos}: Recent epoch {epoch_id} should be kept"
        
        print("    ✅ All boundary scenarios passed")

    async def test_stress_cleanup_scenarios(self):
        """Test cleanup under stress conditions with large datasets."""
        bitmap = RedisBitmap(epoch_offset=12000)
        
        # Test 1: Massive cleanup with sparse data
        key1 = "test_stress_massive"
        self.register_test_key(key1)
        
        # Create 100,000 epochs with sparse distribution
        old_epochs = [12000 + i * 10 for i in range(5000)]  # Every 10th epoch, 50K range
        recent_epochs = [62000 + i * 5 for i in range(1000)]  # Every 5th epoch, 5K range
        
        print("  📦 Setting up massive dataset...")
        await bitmap.set_bits_in_range(self.redis_conn, key1, old_epochs + recent_epochs)
        
        initial_count = await bitmap.get_bit_count(self.redis_conn, key1)
        print(f"    Initial bits: {initial_count}")
        
        # Cleanup should clear all old_epochs but keep recent_epochs
        current_epoch = 62000 + 10000
        start_time = time.time()
        bytes_cleaned = await bitmap.cleanup_old_bits(
            self.redis_conn, key1, current_epoch, max_epochs_to_keep=10000
        )
        cleanup_time = time.time() - start_time
        
        final_count = await bitmap.get_bit_count(self.redis_conn, key1)
        
        # Verify all old epochs are cleared
        sample_old = old_epochs[::100]  # Sample every 100th old epoch
        for epoch_id in sample_old:
            result = await bitmap.get_bit(self.redis_conn, key1, epoch_id)
            assert not result, f"Old epoch {epoch_id} should be cleared"
        
        # Verify recent epochs are kept
        sample_recent = recent_epochs[::50]  # Sample every 50th recent epoch
        for epoch_id in sample_recent:
            result = await bitmap.get_bit(self.redis_conn, key1, epoch_id)
            assert result, f"Recent epoch {epoch_id} should be kept"
        
        print(f"    ✅ Massive cleanup: {initial_count} → {final_count} bits in {cleanup_time:.3f}s")
        
        # Test 2: Repeated cleanup operations
        key2 = "test_stress_repeated"
        self.register_test_key(key2)
        
        base_epoch = 13000
        for round_num in range(10):
            # Add new epochs each round
            new_epochs = [base_epoch + round_num * 1000 + i for i in range(100)]
            await bitmap.set_bits_in_range(self.redis_conn, key2, new_epochs)
            
            # Cleanup keeping only last 200 epochs
            current_epoch = base_epoch + round_num * 1000 + 200
            await bitmap.cleanup_old_bits(
                self.redis_conn, key2, current_epoch, max_epochs_to_keep=200
            )
        
        final_count = await bitmap.get_bit_count(self.redis_conn, key2)
        assert final_count <= 200, f"Should have ≤200 bits after repeated cleanup, got {final_count}"
        
        print(f"    ✅ Repeated cleanup: Final count {final_count} bits")

    async def test_concurrent_cleanup_operations(self):
        """Test cleanup behavior with concurrent operations."""
        bitmap = RedisBitmap(epoch_offset=14000)
        key = "test_concurrent_cleanup"
        self.register_test_key(key)
        
        # Set up initial data
        epochs = [14000 + i for i in range(1000)]
        await bitmap.set_bits_in_range(self.redis_conn, key, epochs)
        
        # Define concurrent operations
        async def cleanup_operation(epoch_id, retention):
            return await bitmap.cleanup_old_bits(
                self.redis_conn, key, epoch_id, max_epochs_to_keep=retention
            )
        
        async def set_operation(epoch_ids):
            return await bitmap.set_bits_in_range(self.redis_conn, key, epoch_ids)
        
        async def get_operation(epoch_ids):
            return await bitmap.get_bits_in_range(self.redis_conn, key, epoch_ids)
        
        # Run concurrent operations
        tasks = [
            cleanup_operation(15000, 500),
            set_operation([15001, 15002, 15003]),
            get_operation([14500, 14501, 14502]),
            cleanup_operation(15100, 400),
            set_operation([15101, 15102, 15103]),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify no exceptions occurred
        for i, result in enumerate(results):
            assert not isinstance(result, Exception), f"Task {i} failed: {result}"
        
        # Verify final state is consistent
        final_count = await bitmap.get_bit_count(self.redis_conn, key)
        assert final_count > 0, "Should have some bits remaining after concurrent operations"
        
        print(f"    ✅ Concurrent operations completed successfully, final bits: {final_count}")

    async def test_cleanup_with_fragmented_data(self):
        """Test cleanup with highly fragmented bitmap data."""
        bitmap = RedisBitmap(epoch_offset=15000)
        key = "test_fragmented_cleanup"
        self.register_test_key(key)
        
        # Create highly fragmented pattern
        fragmented_epochs = []
        
        # Pattern 1: Alternating bits (15000, 15002, 15004, ...)
        fragmented_epochs.extend([15000 + i * 2 for i in range(500)])
        
        # Pattern 2: Prime number positions
        primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47]
        fragmented_epochs.extend([15000 + 1000 + p * 10 for p in primes])
        
        # Pattern 3: Exponential positions
        fragmented_epochs.extend([15000 + 2000 + 2**i for i in range(10)])
        
        # Pattern 4: Random sparse positions
        import random
        random.seed(42)  # Deterministic for testing
        fragmented_epochs.extend([15000 + 3000 + random.randint(0, 10000) for _ in range(100)])
        
        await bitmap.set_bits_in_range(self.redis_conn, key, fragmented_epochs)
        
        initial_count = await bitmap.get_bit_count(self.redis_conn, key)
        print(f"    Fragmented data: {initial_count} bits across {len(fragmented_epochs)} positions")
        
        # Cleanup with different retention policies
        cutoff_epoch = 15000 + 2500
        current_epoch = cutoff_epoch + 1000
        
        bytes_cleaned = await bitmap.cleanup_old_bits(
            self.redis_conn, key, current_epoch, max_epochs_to_keep=1000
        )
        
        final_count = await bitmap.get_bit_count(self.redis_conn, key)
        
        # Verify cleanup worked correctly with fragmented data
        for epoch_id in fragmented_epochs:
            result = await bitmap.get_bit(self.redis_conn, key, epoch_id)
            should_be_cleared = epoch_id < cutoff_epoch
            
            if should_be_cleared:
                assert not result, f"Fragmented epoch {epoch_id} should be cleared (< {cutoff_epoch})"
            else:
                assert result, f"Fragmented epoch {epoch_id} should be kept (>= {cutoff_epoch})"
        
        print(f"    ✅ Fragmented cleanup: {initial_count} → {final_count} bits, {bytes_cleaned} bytes cleaned")

    async def test_cleanup_memory_efficiency(self):
        """Test cleanup memory efficiency and optimization."""
        bitmap = RedisBitmap(epoch_offset=16000)
        key = "test_memory_efficiency"
        self.register_test_key(key)
        
        # Test 1: Dense vs Sparse cleanup efficiency
        print("  📊 Testing cleanup efficiency patterns...")
        
        # Dense pattern (consecutive bits)
        dense_epochs = list(range(16000, 16500))
        await bitmap.set_bits_in_range(self.redis_conn, key, dense_epochs)
        
        dense_size_before = await self.redis_conn.strlen(key)
        start_time = time.time()
        dense_bytes_cleaned = await bitmap.cleanup_old_bits(
            self.redis_conn, key, 17000, max_epochs_to_keep=100
        )
        dense_time = time.time() - start_time
        dense_size_after = await self.redis_conn.strlen(key)
        
        await bitmap.clear(self.redis_conn, key)
        
        # Sparse pattern (every 100th bit)
        sparse_epochs = [16000 + i * 100 for i in range(500)]
        await bitmap.set_bits_in_range(self.redis_conn, key, sparse_epochs)
        
        sparse_size_before = await self.redis_conn.strlen(key)
        start_time = time.time()
        sparse_bytes_cleaned = await bitmap.cleanup_old_bits(
            self.redis_conn, key, 17000, max_epochs_to_keep=100
        )
        sparse_time = time.time() - start_time
        sparse_size_after = await self.redis_conn.strlen(key)
        
        print(f"    Dense pattern: {dense_size_before}→{dense_size_after} bytes, {dense_time:.4f}s")
        print(f"    Sparse pattern: {sparse_size_before}→{sparse_size_after} bytes, {sparse_time:.4f}s")
        
        # Test 2: Memory reclamation verification
        await bitmap.clear(self.redis_conn, key)
        
        # Fill bitmap completely, then cleanup most of it
        full_epochs = list(range(16000, 18000))  # 2000 epochs
        await bitmap.set_bits_in_range(self.redis_conn, key, full_epochs)
        
        size_full = await self.redis_conn.strlen(key)
        
        # Cleanup 95% of data
        await bitmap.cleanup_old_bits(self.redis_conn, key, 18000, max_epochs_to_keep=100)
        
        size_after_cleanup = await self.redis_conn.strlen(key)
        remaining_bits = await bitmap.get_bit_count(self.redis_conn, key)
        
        print(f"    Memory reclamation: {size_full} → {size_after_cleanup} bytes ({remaining_bits} bits)")
        
        assert remaining_bits <= 100, f"Should have ≤100 bits remaining, got {remaining_bits}"
        print("    ✅ Memory efficiency tests passed")

    async def test_production_usage_patterns(self):
        """Test realistic production usage patterns."""
        bitmap = RedisBitmap(epoch_offset=17000)
        
        # Simulate real-world blank epochs tracking
        patterns = [
            {
                "name": "High Activity Project",
                "key": "test_prod_high_activity",
                "blank_rate": 0.1,  # 10% blank epochs
                "epoch_range": 1000,
            },
            {
                "name": "Medium Activity Project", 
                "key": "test_prod_medium_activity",
                "blank_rate": 0.3,  # 30% blank epochs
                "epoch_range": 2000,
            },
            {
                "name": "Low Activity Project",
                "key": "test_prod_low_activity", 
                "blank_rate": 0.7,  # 70% blank epochs
                "epoch_range": 5000,
            },
        ]
        
        for pattern in patterns:
            key = pattern["key"]
            self.register_test_key(key)
            
            # Simulate blank epochs pattern
            blank_epochs = []
            total_epochs = pattern["epoch_range"]
            blank_rate = pattern["blank_rate"]
            
            import random
            random.seed(42)  # Deterministic
            
            for i in range(total_epochs):
                if random.random() < blank_rate:
                    blank_epochs.append(17000 + i)
            
            await bitmap.set_bits_in_range(self.redis_conn, key, blank_epochs)
            
            initial_count = await bitmap.get_bit_count(self.redis_conn, key)
            initial_size = await self.redis_conn.strlen(key)
            
            # Simulate production cleanup (keep last 30 days worth)
            current_epoch = 17000 + total_epochs + 1000
            retention_epochs = min(30 * 24 * 60 // 12, bitmap.DEFAULT_MAX_EPOCHS_TO_KEEP)  # ~30 days of 12-min epochs
            
            start_time = time.time()
            bytes_cleaned = await bitmap.cleanup_old_bits(
                self.redis_conn, key, current_epoch, max_epochs_to_keep=retention_epochs
            )
            cleanup_time = time.time() - start_time
            
            final_count = await bitmap.get_bit_count(self.redis_conn, key)
            final_size = await self.redis_conn.strlen(key)
            
            print(f"    {pattern['name']}:")
            print(f"      Initial: {initial_count} bits ({initial_size} bytes)")
            print(f"      Cleanup: {bytes_cleaned} bytes in {cleanup_time:.3f}s")
            print(f"      Final: {final_count} bits ({final_size} bytes)")
            print(f"      Reduction: {((initial_count - final_count) / initial_count * 100):.1f}%")
        
        print("    ✅ Production patterns tested successfully")

    async def test_auto_cleanup_integration(self):
        """Test comprehensive auto-cleanup integration scenarios."""
        bitmap = RedisBitmap(epoch_offset=18000)
        key = "test_auto_cleanup_integration"
        self.register_test_key(key)
        
        # Test 1: Progressive auto-cleanup
        print("  🔄 Testing progressive auto-cleanup...")
        
        cleanup_stats = []
        
        for epoch_batch in range(20):
            base_epoch = 18000 + epoch_batch * 100
            
            # Add 50 new epochs with auto-cleanup
            for i in range(50):
                epoch_id = base_epoch + i
                was_set = await bitmap.set_bit_with_auto_cleanup(
                    self.redis_conn, key, epoch_id, max_epochs_to_keep=200
                )
                assert was_set, f"Epoch {epoch_id} should be newly set"
            
            # Record stats
            bit_count = await bitmap.get_bit_count(self.redis_conn, key)
            size = await self.redis_conn.strlen(key)
            cleanup_stats.append((epoch_batch, bit_count, size))
            
            # Verify we don't exceed retention limit significantly
            if epoch_batch > 4:  # After 5 batches, should start seeing cleanup
                assert bit_count <= 300, f"Batch {epoch_batch}: Too many bits {bit_count}, auto-cleanup not working"
        
        # Verify progressive cleanup maintained reasonable size
        final_stats = cleanup_stats[-1]
        print(f"    Progressive cleanup: Final stats - {final_stats[1]} bits, {final_stats[2]} bytes")
        
        # Test 2: Auto-cleanup error resilience
        print("  🛡️ Testing auto-cleanup error resilience...")
        
        # Test with edge case epoch values
        edge_cases = [
            18000,  # At epoch_offset
            18001,  # Just after epoch_offset
            bitmap.DEFAULT_MAX_EPOCHS_TO_KEEP + 18000,  # At default retention boundary
        ]
        
        for edge_epoch in edge_cases:
            try:
                was_set = await bitmap.set_bit_with_auto_cleanup(
                    self.redis_conn, key, edge_epoch
                )
                result = await bitmap.get_bit(self.redis_conn, key, edge_epoch)
                assert result, f"Edge case epoch {edge_epoch} should be set despite cleanup challenges"
            except Exception as e:
                assert False, f"Auto-cleanup failed on edge case {edge_epoch}: {e}"
        
        print("    ✅ Auto-cleanup integration tests passed")

    async def test_cleanup_correctness_validation(self):
        """Test cleanup correctness with validation checks."""
        bitmap = RedisBitmap(epoch_offset=19000)
        key = "test_cleanup_correctness"
        self.register_test_key(key)
        
        # Test 1: Exact boundary validation
        print("  ✅ Testing exact boundary correctness...")
        
        test_cases = [
            # (cutoff_bit, bits_before, bits_after)
            (1, [0], [1, 2, 3]),
            (8, [0, 1, 2, 3, 4, 5, 6, 7], [8, 9, 10]),
            (15, [0, 7, 14], [15, 16, 23]),
            (16, [0, 8, 15], [16, 17, 24]),
            (33, [0, 16, 32], [33, 34, 40]),
        ]
        
        for case_num, (cutoff_bit, bits_before, bits_after) in enumerate(test_cases):
            subkey = f"{key}_case_{case_num}"
            self.register_test_key(subkey)
            
            # Set up test data
            all_bits = bits_before + bits_after
            epochs_to_set = [19000 + bit for bit in all_bits]
            await bitmap.set_bits_in_range(self.redis_conn, subkey, epochs_to_set)
            
            # Perform cleanup
            cutoff_epoch = 19000 + cutoff_bit
            current_epoch = cutoff_epoch + 100
            await bitmap.cleanup_old_bits(
                self.redis_conn, subkey, current_epoch, max_epochs_to_keep=100
            )
            
            # Validate exact boundaries
            for bit in bits_before:
                epoch_id = 19000 + bit
                result = await bitmap.get_bit(self.redis_conn, subkey, epoch_id)
                assert not result, f"Case {case_num}: Bit {bit} (epoch {epoch_id}) should be cleared"
            
            for bit in bits_after:
                epoch_id = 19000 + bit
                result = await bitmap.get_bit(self.redis_conn, subkey, epoch_id)
                assert result, f"Case {case_num}: Bit {bit} (epoch {epoch_id}) should be kept"
        
        print("    ✅ Boundary correctness validated")
        
        # Test 2: Data integrity validation
        print("  🔍 Testing data integrity after cleanup...")
        
        # Create known pattern
        pattern_epochs = [19000 + i for i in range(0, 1000, 7)]  # Every 7th epoch
        await bitmap.set_bits_in_range(self.redis_conn, key, pattern_epochs)
        
        # Cleanup with various retention values
        retention_values = [50, 100, 200, 500]
        for retention in retention_values:
            test_key = f"{key}_retention_{retention}"
            self.register_test_key(test_key)
            
            # Copy pattern
            await bitmap.set_bits_in_range(self.redis_conn, test_key, pattern_epochs)
            
            # Cleanup
            current_epoch = 19000 + 1000 + retention
            await bitmap.cleanup_old_bits(
                self.redis_conn, test_key, current_epoch, max_epochs_to_keep=retention
            )
            
            # Verify integrity
            cutoff_epoch = current_epoch - retention
            for epoch_id in pattern_epochs:
                result = await bitmap.get_bit(self.redis_conn, test_key, epoch_id)
                expected = epoch_id >= cutoff_epoch
                
                assert result == expected, \
                    f"Retention {retention}: Epoch {epoch_id} integrity failed (expected {expected}, got {result})"
        
        print("    ✅ Data integrity validated for all retention values")

    async def run_all_tests(self):
        """Run the complete test suite."""
        print("🚀 Starting RedisBitmap Comprehensive Test Suite")
        print("=" * 60)
        
        await self.setup()
        
        try:
            # Run all tests
            await self.run_test("Basic Functionality (No Offset)", self.test_basic_functionality_no_offset)
            await self.run_test("Various Offsets", self.test_various_offsets)
            await self.run_test("Large Batch Operations", self.test_large_batch_operations)
            await self.run_test("Edge Cases", self.test_edge_cases)
            await self.run_test("Concurrent Operations", self.test_concurrent_operations)
            await self.run_test("Corruption Detection", self.test_corruption_detection)
            await self.run_test("Performance Under Load", self.test_performance_under_load)
            await self.run_test("Error Handling", self.test_error_handling)
            await self.run_test("Memory Efficiency", self.test_memory_efficiency)
            
            # Cleanup functionality tests
            await self.run_test("Cleanup Old Bits", self.test_cleanup_old_bits)
            await self.run_test("Cleanup with Custom Retention", self.test_cleanup_with_custom_retention)
            await self.run_test("Cleanup Edge Cases", self.test_cleanup_edge_cases)
            await self.run_test("Set Bit with Auto Cleanup", self.test_set_bit_with_auto_cleanup)
            await self.run_test("Auto Cleanup Error Resilience", self.test_auto_cleanup_error_resilience)
            await self.run_test("Cleanup Validation", self.test_cleanup_validation)
            await self.run_test("Cleanup Performance", self.test_cleanup_performance)
            
            # Precision and edge case tests
            await self.run_test("Byte Boundary Precision", self.test_byte_boundary_precision)
            await self.run_test("Partial Byte Cleanup", self.test_partial_byte_cleanup)
            await self.run_test("No Data Loss Edge Cases", self.test_no_data_loss_edge_cases)
            await self.run_test("Comprehensive Boundary Scenarios", self.test_comprehensive_boundary_scenarios)
            
            # Advanced comprehensive tests
            await self.run_test("Stress Cleanup Scenarios", self.test_stress_cleanup_scenarios)
            await self.run_test("Concurrent Cleanup Operations", self.test_concurrent_cleanup_operations)
            await self.run_test("Cleanup with Fragmented Data", self.test_cleanup_with_fragmented_data)
            await self.run_test("Cleanup Memory Efficiency", self.test_cleanup_memory_efficiency)
            await self.run_test("Production Usage Patterns", self.test_production_usage_patterns)
            await self.run_test("Auto Cleanup Integration", self.test_auto_cleanup_integration)
            await self.run_test("Cleanup Correctness Validation", self.test_cleanup_correctness_validation)
            
        finally:
            await self.cleanup()
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 TEST SUITE SUMMARY")
        print("=" * 60)
        print(f"Total Tests: {self.total_tests}")
        print(f"Passed: {self.passed_tests} ✅")
        print(f"Failed: {self.failed_tests} ❌")
        
        if self.failed_tests == 0:
            print("\n🎉 ALL TESTS PASSED! RedisBitmap is working correctly.")
        else:
            print(f"\n⚠️  {self.failed_tests} tests failed. Please review the issues above.")
            
        return self.failed_tests == 0


async def main():
    """Main test runner."""
    test_suite = BitmapTestSuite()
    success = await test_suite.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)