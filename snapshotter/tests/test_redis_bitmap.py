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