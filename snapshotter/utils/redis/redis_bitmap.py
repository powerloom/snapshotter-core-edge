from typing import Iterable, List, Tuple
from redis.exceptions import RedisError
from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger


class RedisBitmap:
    """
    This class provides an efficient way to store and query sets of integers using Redis bitmaps.
    It supports both individual and batch operations for setting and querying bits.
    All operations are asynchronous for better performance.
    
    The bitmap uses an epoch offset to map integer values to bit positions, allowing for
    efficient storage of large ranges of integers.
    
    Attributes:
        epoch_offset (int): Base offset for mapping integers to bit positions
        logger: Logger instance for tracking operations
    """
    
    def __init__(self, epoch_offset: int):
        """
        Initialize the RedisBitmap with an epoch offset.
        
        Args:
            epoch_offset (int): Base offset for mapping integers to bit positions
        """
        self.epoch_offset = epoch_offset
        self.logger = logger.bind(module="RedisBitmap")

    async def set_bit(self, redis_conn: aioredis.Redis, key: str, epoch_id: int) -> bool:
        """
        Set a single bit in the bitmap asynchronously.
        
        This method sets a bit at the position corresponding to (epoch_id - epoch_offset).
        If the bit was already set, returns False. If the bit was newly set, returns True.
        
        Args:
            redis_conn (aioredis.Redis): Redis connection
            key (str): Redis key for the bitmap
            epoch_id (int): Non-negative integer representing the bit position
            
        Returns:
            bool: True if the bit was newly set, False if it was already set
            
        Raises:
            ValueError: If epoch_id is negative
            RedisBitmapError: If the Redis operation fails
        """
        if epoch_id - self.epoch_offset < 0:
            self.logger.error(f"Invalid epoch_id: {epoch_id}")
            raise ValueError("epoch_id must be non-negative")

        try:
            result = await redis_conn.setbit(key, epoch_id - self.epoch_offset, 1)
            self.logger.debug(f"Set bit {epoch_id} (previous value: {result})")
            return result == 0  # True if bit was changed
        except RedisError as e:
            self.logger.error(f"Failed to set bit {epoch_id}: {e}")
            raise RedisBitmapError(f"Failed to set bit: {e}") from e

    async def set_bits(self, redis_conn: aioredis.Redis, key: str, epoch_ids: Iterable[int]) -> int:
        """
        Set multiple bits in the bitmap using a pipeline asynchronously.
        
        This method efficiently sets multiple bits using Redis pipelining to reduce
        network round trips. It returns the number of bits that were newly set.
        
        Args:
            redis_conn (aioredis.Redis): Redis connection
            key (str): Redis key for the bitmap
            epoch_ids (Iterable[int]): Non-negative integers representing bit positions
            
        Returns:
            int: Number of bits that were newly set
            
        Raises:
            ValueError: If any epoch_id is negative
            RedisBitmapError: If the Redis operation fails
        """
        epoch_ids = list(epoch_ids)  # Ensure iterable is materialized
        if not epoch_ids:
            self.logger.debug("No epoch_ids provided for set_bits")
            return 0

        if any(b - self.epoch_offset < 0 for b in epoch_ids):
            self.logger.error(f"Negative epoch_ids detected: {epoch_ids}")
            raise ValueError("All epoch_ids must be non-negative")

        try:
            async with redis_conn.pipeline() as pipe:
                for epoch_id in epoch_ids:
                    pipe.setbit(key, epoch_id - self.epoch_offset, 1)
                results = await pipe.execute()
            changed = sum(1 for r in results if r == 0)
            self.logger.debug(f"Set {len(epoch_ids)} bits, {changed} changed")
            return changed
        except RedisError as e:
            self.logger.error(f"Failed to set bits: {e}")
            raise RedisBitmapError(f"Failed to set bits: {e}") from e

    async def set_bits_in_range(self, redis_conn: aioredis.Redis, key: str, epoch_ids: Iterable[int]) -> int:
        """
        Set multiple bits in the bitmap using SETRANGE for efficiency when possible.
        
        This method optimizes setting multiple bits by grouping them into bytes and
        using SETRANGE to minimize Redis operations. It's most efficient when bits
        are clustered together.
        
        Args:
            redis_conn (aioredis.Redis): Redis connection
            key (str): Redis key for the bitmap
            epoch_ids (Iterable[int]): Non-negative integers representing bit positions
            
        Returns:
            int: Number of bytes written (approximate measure of operation scope)
            
        Raises:
            ValueError: If any epoch_id is negative
            RedisBitmapError: If the Redis operation fails
        """
        epoch_ids = list(epoch_ids)
        if not epoch_ids:
            self.logger.debug("No epoch_ids provided for set_bits_in_range")
            return 0

        if any(b - self.epoch_offset < 0 for b in epoch_ids):
            self.logger.error(f"Negative epoch_ids detected: {epoch_ids}")
            raise ValueError("All epoch_ids must be non-negative")

        try:
            # Group bits by byte position for efficient SETRANGE
            byte_map = {}
            for epoch_id in epoch_ids:
                epoch_id -= self.epoch_offset
                byte_pos = epoch_id // 8
                bit_pos = epoch_id % 8
                if byte_pos not in byte_map:
                    byte_map[byte_pos] = 0
                byte_map[byte_pos] |= 1 << (7 - bit_pos)

            # Create contiguous byte array for SETRANGE
            min_byte = min(byte_map.keys())
            max_byte = max(byte_map.keys())
            byte_data = bytearray(max_byte - min_byte + 1)
            for byte_pos, value in byte_map.items():
                byte_data[byte_pos - min_byte] = value

            # Write bytes with SETRANGE
            await redis_conn.setrange(key, min_byte, bytes(byte_data))
            self.logger.debug(f"Set bits in range, wrote {len(byte_data)} bytes")
            return len(byte_data)
        except RedisError as e:
            self.logger.error(f"Failed to set bits in range: {e}")
            raise RedisBitmapError(f"Failed to set bits in range: {e}") from e

    async def get_bit(self, redis_conn: aioredis.Redis, key: str, epoch_id: int) -> bool:
        """
        Check if a single bit is set in the bitmap asynchronously.
        
        Args:
            redis_conn (aioredis.Redis): Redis connection
            key (str): Redis key for the bitmap
            epoch_id (int): Non-negative integer representing the bit position
            
        Returns:
            bool: True if the bit is set, False otherwise
            
        Raises:
            ValueError: If epoch_id is negative
            RedisBitmapError: If the Redis operation fails
        """
        if epoch_id - self.epoch_offset < 0:
            self.logger.error(f"Invalid epoch_id: {epoch_id}")
            raise ValueError("epoch_id must be non-negative")

        try:
            result = await redis_conn.getbit(key, epoch_id - self.epoch_offset)
            self.logger.debug(f"Get bit {epoch_id}: {result}")
            return bool(result)
        except RedisError as e:
            self.logger.error(f"Failed to get bit {epoch_id}: {e}")
            raise RedisBitmapError(f"Failed to get bit: {e}") from e

    async def get_bits(self, redis_conn: aioredis.Redis, key: str, epoch_ids: Iterable[int]) -> List[Tuple[int, bool]]:
        """
        Check if multiple bits are set in the bitmap using a pipeline asynchronously.
        
        This method efficiently queries multiple bits using Redis pipelining to reduce
        network round trips.
        
        Args:
            redis_conn (aioredis.Redis): Redis connection
            key (str): Redis key for the bitmap
            epoch_ids (Iterable[int]): Non-negative integers to query
            
        Returns:
            List[Tuple[int, bool]]: List of (epoch_id, is_present) tuples
            
        Raises:
            ValueError: If any epoch_id is negative
            RedisBitmapError: If the Redis operation fails
        """
        epoch_ids = list(epoch_ids)
        if not epoch_ids:
            self.logger.debug("No query numbers provided for get_bits")
            return []

        if any(b - self.epoch_offset < 0 for b in epoch_ids):
            self.logger.error(f"Negative epoch_ids detected: {epoch_ids}")
            raise ValueError("All epoch_ids must be non-negative")

        try:
            async with redis_conn.pipeline() as pipe:
                for epoch_id in epoch_ids:
                    pipe.getbit(key, epoch_id - self.epoch_offset)
                results = await pipe.execute()
            result_pairs = [(epoch_id, bool(result)) for epoch_id, result in zip(epoch_ids, results)]
            self.logger.debug(f"Got {len(result_pairs)} bits")
            return result_pairs
        except RedisError as e:
            self.logger.error(f"Failed to get bits: {e}")
            raise RedisBitmapError(f"Failed to get bits: {e}") from e

    async def get_bits_in_range(self, redis_conn: aioredis.Redis, key: str, epoch_ids: Iterable[int]) -> List[Tuple[int, bool]]:
        """
        Efficiently check multiple bits in a Redis bitmap using GETRANGE command.

        This method optimizes performance by fetching a range of bytes containing all requested bits
        in a single Redis operation, rather than making individual GETBIT calls. It's particularly
        efficient when querying bits that are close together in the bitmap.

        Args:
            redis_conn (aioredis.Redis): Redis connection instance
            key (str): Redis key storing the bitmap
            epoch_ids (Iterable[int]): Collection of epoch IDs to check in the bitmap

        Returns:
            List[Tuple[int, bool]]: List of tuples containing (epoch_id, is_present) pairs,
                                   where is_present indicates if the bit was set

        Raises:
            ValueError: If any epoch_id is negative after applying epoch_offset
            RedisBitmapError: If Redis operation fails or connection issues occur
        """
        # Convert iterable to list for multiple passes
        epoch_ids = list(epoch_ids)
        if not epoch_ids:
            self.logger.debug("No epoch_ids provided for get_bits_in_range")
            return []

        # Validate all epoch IDs are non-negative after offset
        if any(b - self.epoch_offset < 0 for b in epoch_ids):
            self.logger.error(f"Negative epoch_ids detected: {epoch_ids}")
            raise ValueError("All epoch_ids must be non-negative")

        try:
            # Calculate byte range to fetch from Redis
            min_epoch_id = min(epoch_ids) - self.epoch_offset
            max_epoch_id = max(epoch_ids) - self.epoch_offset
            start_byte = min_epoch_id // 8  # Convert bit position to byte position
            end_byte = max_epoch_id // 8

            # Fetch the byte range containing all requested bits
            byte_data = await redis_conn.getrange(key, start_byte, end_byte)
            # If key doesn't exist, initialize with zeros
            if byte_data is None:
                byte_data = b'\x00' * (end_byte - start_byte + 1)

            # Process each requested epoch ID
            results = []
            for epoch_id in epoch_ids:
                # Calculate byte and bit positions within the fetched data
                byte_pos = (epoch_id - self.epoch_offset) // 8 - start_byte
                bit_pos = (epoch_id - self.epoch_offset) % 8
                
                # Extract bit value using bitwise operations
                if byte_pos < len(byte_data):
                    # Shift right by (7-bit_pos) to move target bit to LSB, then mask with 1
                    bit_value = (byte_data[byte_pos] >> (7 - bit_pos)) & 1
                else:
                    bit_value = 0
                results.append((epoch_id, bool(bit_value)))

            self.logger.debug(f"Got {len(results)} bits in range")
            return results
        except RedisError as e:
            self.logger.error(f"Failed to get bits in range: {e}")
            raise RedisBitmapError(f"Failed to get bits in range: {e}") from e

    async def clear(self, redis_conn: aioredis.Redis, key: str) -> bool:
        """
        Remove a bitmap from Redis storage.

        This method deletes the specified bitmap key from Redis. It's useful for
        cleaning up bitmaps that are no longer needed or for resetting state.

        Args:
            redis_conn (aioredis.Redis): Redis connection instance
            key (str): Redis key of the bitmap to delete

        Returns:
            bool: True if the key was successfully deleted, False if it didn't exist

        Raises:
            RedisBitmapError: If Redis operation fails or connection issues occur
        """
        try:
            result = await redis_conn.delete(key)
            self.logger.info(f"Cleared bitmap key '{key}': {'deleted' if result else 'not found'}")
            return bool(result)
        except RedisError as e:
            self.logger.error(f"Failed to clear bitmap: {e}")
            raise RedisBitmapError(f"Failed to clear bitmap: {e}") from e


class RedisBitmapError(Exception):
    """
    Base exception class for Redis bitmap operations.

    This exception is raised when Redis bitmap operations fail due to
    invalid operations, data corruption, or other bitmap-specific issues.
    """
    pass


class RedisConnectionError(RedisBitmapError):
    """
    Exception raised when Redis connection operations fail.

    This exception is specifically for connection-related issues such as
    connection timeouts, authentication failures, or network problems.
    """
    pass


# Example usage demonstrating RedisBitmap functionality
# async def main():
#     # Initialize Redis connection
#     redis_conn = aioredis.Redis(host="localhost", port=6379, db=0)
#     
#     # Create bitmap instance with epoch offset
#     bitmap = RedisBitmap(epoch_offset=22500000)
#     
#     # Demonstrate individual bit operations
#     await bitmap.set_bit(redis_conn, "test", 22520001)
#     
#     # Demonstrate batch bit operations
#     epoch_ids = [22520003, 22520005, 22520007, 22520100, 225201000]
#     await bitmap.set_bits(redis_conn, "test", epoch_ids)
#     
#     # Demonstrate range operations
#     range_epoch_ids = [22520010, 22520011, 22520012, 22520015]
#     await bitmap.set_bits_in_range(redis_conn, "test", range_epoch_ids)
#     
#     # Query individual and batch bits
#     query_epoch_ids = [22520001, 22520002, 22520003, 22520100, 225201000, 225201001]
#     results = await bitmap.get_bits(redis_conn, "test", query_epoch_ids)
#     for epoch_id, is_present in results:
#         print(f"Epoch {epoch_id} {'is' if is_present else 'is not'} in the set")
#     
#     # Query bits in range
#     range_results = await bitmap.get_bits_in_range(redis_conn, "test", range_epoch_ids)
#     for epoch_id, is_present in range_results:
#         print(f"Range query: Epoch {epoch_id} {'is' if is_present else 'is not'} in the set")
#     
#     # Clean up
#     await bitmap.clear(redis_conn, "test")
#     await redis_conn.close()


# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())