from typing import Iterable, List, Tuple
from redis.exceptions import RedisError, ConnectionError
from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger


class RedisBitmap:
    """
    Production-ready Redis bitmap implementation for efficient integer set operations.
    
    This class provides an efficient way to store and query sets of integers using Redis bitmaps.
    It supports both individual and batch operations for setting and querying bits with comprehensive
    error handling, input validation, and performance optimizations.
    
    The bitmap uses an epoch offset to map integer values to bit positions, allowing for
    efficient storage of large ranges of integers while maintaining memory efficiency.
    
    Attributes:
        epoch_offset (int): Base offset for mapping integers to bit positions
        max_range_size (int): Maximum allowed byte range for range operations (default: 1MB)
        max_batch_size (int): Maximum number of bits in a single batch operation (default: 10K)
    """
    
    # Production safety limits
    DEFAULT_MAX_RANGE_SIZE = 1024 * 1024 * 10  # 10MB maximum range
    DEFAULT_MAX_BATCH_SIZE = 1000000  # 1M bits maximum per batch
    DEFAULT_MAX_EPOCHS_TO_KEEP = 100000  # 100K epochs maximum to keep
    
    def __init__(
        self, 
        epoch_offset: int, 
        max_range_size: int = DEFAULT_MAX_RANGE_SIZE,
        max_batch_size: int = DEFAULT_MAX_BATCH_SIZE
    ):
        """
        Initialize the RedisBitmap with safety limits and epoch offset.
        
        Args:
            epoch_offset (int): Base offset for mapping integers to bit positions
            max_range_size (int): Maximum allowed byte range for range operations
            max_batch_size (int): Maximum number of bits in a single batch operation
            
        Raises:
            ValueError: If parameters are invalid
        """
        if not isinstance(epoch_offset, int):
            raise ValueError("epoch_offset must be an integer")
        if max_range_size <= 0:
            raise ValueError("max_range_size must be positive")
        if max_batch_size <= 0:
            raise ValueError("max_batch_size must be positive")
            
        self.epoch_offset = epoch_offset
        self.max_range_size = max_range_size
        self.max_batch_size = max_batch_size
        self.logger = logger.bind(module="RedisBitmap")

    def _validate_connection(self, redis_conn: aioredis.Redis) -> None:
        """Validate Redis connection parameter."""
        if not isinstance(redis_conn, aioredis.Redis):
            raise TypeError("redis_conn must be an aioredis.Redis instance")

    def _validate_key(self, key: str) -> None:
        """Validate Redis key parameter."""
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if not key.strip():
            raise ValueError("key cannot be empty")
        if len(key.encode('utf-8')) > 512 * 1024 * 1024:  # 512MB Redis key limit
            raise ValueError("key too long")

    def _validate_epoch_id(self, epoch_id: int) -> None:
        """Validate single epoch ID."""
        if not isinstance(epoch_id, int):
            raise TypeError("epoch_id must be an integer")
        if epoch_id - self.epoch_offset < 0:
            raise ValueError(f"epoch_id {epoch_id} must be >= epoch_offset {self.epoch_offset}")
        # Redis bitmap limit is 512MB * 8 = ~4.3B bits
        if epoch_id - self.epoch_offset >= 2**32:
            raise ValueError(f"epoch_id {epoch_id} exceeds Redis bitmap limits")

    def _validate_epoch_ids(self, epoch_ids: List[int]) -> None:
        """Validate list of epoch IDs."""
        if len(epoch_ids) > self.max_batch_size:
            raise ValueError(f"Too many epoch_ids: {len(epoch_ids)} > {self.max_batch_size}")
        
        for epoch_id in epoch_ids:
            self._validate_epoch_id(epoch_id)

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
            TypeError: If parameters have wrong types
            ValueError: If epoch_id is invalid
            RedisBitmapError: If the Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)
        self._validate_epoch_id(epoch_id)

        try:
            result = await redis_conn.setbit(key, epoch_id - self.epoch_offset, 1)
            self.logger.debug(f"Set bit {epoch_id} (previous value: {result})")
            return result == 0  # True if bit was changed
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while setting bit {epoch_id}: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
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
            TypeError: If parameters have wrong types
            ValueError: If any epoch_id is invalid or batch too large
            RedisBitmapError: If the Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)
        
        epoch_ids = list(epoch_ids)  # Ensure iterable is materialized
        if not epoch_ids:
            self.logger.debug("No epoch_ids provided for set_bits")
            return 0

        self._validate_epoch_ids(epoch_ids)

        try:
            async with redis_conn.pipeline() as pipe:
                for epoch_id in epoch_ids:
                    pipe.setbit(key, epoch_id - self.epoch_offset, 1)
                results = await pipe.execute()
            changed = sum(1 for r in results if r == 0)
            self.logger.debug(f"Set {len(epoch_ids)} bits, {changed} changed")
            return changed
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while setting bits: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
        except RedisError as e:
            self.logger.error(f"Failed to set bits: {e}")
            raise RedisBitmapError(f"Failed to set bits: {e}") from e

    async def set_bits_in_range(self, redis_conn: aioredis.Redis, key: str, epoch_ids: Iterable[int]) -> int:
        """
        Set multiple bits in the bitmap using SETRANGE for efficiency when possible.
        
        This method optimizes setting multiple bits by grouping them into bytes and
        using SETRANGE to minimize Redis operations. It's most efficient when bits
        are clustered together. Automatically falls back to individual SETBIT operations
        for sparse data or when range exceeds safety limits.
        
        Args:
            redis_conn (aioredis.Redis): Redis connection
            key (str): Redis key for the bitmap
            epoch_ids (Iterable[int]): Non-negative integers representing bit positions
            
        Returns:
            int: Number of bytes written (approximate measure of operation scope)
            
        Raises:
            TypeError: If parameters have wrong types
            ValueError: If any epoch_id is invalid or batch too large
            RedisBitmapError: If the Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)
        
        epoch_ids = list(epoch_ids)
        if not epoch_ids:
            self.logger.debug("No epoch_ids provided for set_bits_in_range")
            return 0

        self._validate_epoch_ids(epoch_ids)

        try:
            # Group bits by byte position for efficient SETRANGE
            byte_map = {}
            for epoch_id in epoch_ids:
                adjusted_id = epoch_id - self.epoch_offset
                byte_pos = adjusted_id // 8
                bit_pos = adjusted_id % 8
                if byte_pos not in byte_map:
                    byte_map[byte_pos] = 0
                byte_map[byte_pos] |= 1 << (7 - bit_pos)

            # Calculate byte range
            min_byte = min(byte_map.keys())
            max_byte = max(byte_map.keys())
            range_size = max_byte - min_byte + 1
            num_affected_bytes = len(byte_map)
            
            # Safety check: prevent excessive memory usage
            if range_size > self.max_range_size:
                self.logger.warning(f"Range too large ({range_size} > {self.max_range_size}), using SETBIT")
                return await self._fallback_to_setbit(redis_conn, key, epoch_ids)
            
            # Check if range is too sparse - fall back to individual SETBIT operations
            if range_size > num_affected_bytes * 4:  # If more than 75% sparse
                self.logger.debug(f"Range too sparse ({num_affected_bytes}/{range_size} bytes), using SETBIT")
                return await self._fallback_to_setbit(redis_conn, key, epoch_ids)

            # Read existing bytes to preserve other bits
            existing_data = await redis_conn.getrange(key, min_byte, max_byte)
            if existing_data is None:
                existing_data = b'\x00' * range_size
            elif len(existing_data) < range_size:
                # Pad with zeros if existing data is shorter
                existing_data += b'\x00' * (range_size - len(existing_data))

            # Merge existing data with new bits
            byte_data = bytearray(existing_data)
            for byte_pos, new_bits in byte_map.items():
                byte_data[byte_pos - min_byte] |= new_bits

            # Write merged bytes with SETRANGE
            await redis_conn.setrange(key, min_byte, bytes(byte_data))
            self.logger.debug(f"Set bits in range, wrote {len(byte_data)} bytes")
            return len(byte_data)
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while setting bits in range: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
        except RedisError as e:
            self.logger.error(f"Failed to set bits in range: {e}")
            raise RedisBitmapError(f"Failed to set bits in range: {e}") from e

    async def _fallback_to_setbit(self, redis_conn: aioredis.Redis, key: str, epoch_ids: List[int]) -> int:
        """Fallback method using individual SETBIT operations."""
        async with redis_conn.pipeline() as pipe:
            for epoch_id in epoch_ids:
                pipe.setbit(key, epoch_id - self.epoch_offset, 1)
            await pipe.execute()
        return len(epoch_ids)

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
            TypeError: If parameters have wrong types
            ValueError: If epoch_id is invalid
            RedisBitmapError: If the Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)
        self._validate_epoch_id(epoch_id)

        try:
            result = await redis_conn.getbit(key, epoch_id - self.epoch_offset)
            self.logger.debug(f"Get bit {epoch_id}: {result}")
            return bool(result)
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while getting bit {epoch_id}: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
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
            TypeError: If parameters have wrong types
            ValueError: If any epoch_id is invalid or batch too large
            RedisBitmapError: If the Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)
        
        epoch_ids = list(epoch_ids)
        if not epoch_ids:
            self.logger.debug("No epoch_ids provided for get_bits")
            return []

        self._validate_epoch_ids(epoch_ids)

        try:
            async with redis_conn.pipeline() as pipe:
                for epoch_id in epoch_ids:
                    pipe.getbit(key, epoch_id - self.epoch_offset)
                results = await pipe.execute()
            result_pairs = [(epoch_id, bool(result)) for epoch_id, result in zip(epoch_ids, results)]
            self.logger.debug(f"Got {len(result_pairs)} bits")
            return result_pairs
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while getting bits: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
        except RedisError as e:
            self.logger.error(f"Failed to get bits: {e}")
            raise RedisBitmapError(f"Failed to get bits: {e}") from e

    async def get_bits_in_range(
        self, 
        redis_conn: aioredis.Redis, 
        key: str, 
        epoch_ids: Iterable[int]
    ) -> List[Tuple[int, bool]]:
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
            TypeError: If parameters have wrong types
            ValueError: If any epoch_id is invalid or batch too large
            RedisBitmapError: If Redis operation fails or connection issues occur
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)
        
        # Convert iterable to list for multiple passes
        epoch_ids = list(epoch_ids)
        if not epoch_ids:
            self.logger.debug("No epoch_ids provided for get_bits_in_range")
            return []

        self._validate_epoch_ids(epoch_ids)

        try:
            # Calculate byte range to fetch from Redis
            min_epoch_id = min(epoch_ids) - self.epoch_offset
            max_epoch_id = max(epoch_ids) - self.epoch_offset
            start_byte = min_epoch_id // 8  # Convert bit position to byte position
            end_byte = max_epoch_id // 8
            range_size = end_byte - start_byte + 1

            # Safety check: prevent excessive memory usage
            if range_size > self.max_range_size:
                self.logger.warning(f"Range too large ({range_size} > {self.max_range_size}), using individual GETBIT")
                return await self.get_bits(redis_conn, key, epoch_ids)

            # Fetch the byte range containing all requested bits
            byte_data = await redis_conn.getrange(key, start_byte, end_byte)
            # If key doesn't exist, initialize with zeros
            if byte_data is None:
                byte_data = b'\x00' * range_size

            # Process each requested epoch ID
            results = []
            for epoch_id in epoch_ids:
                # Calculate byte and bit positions within the fetched data
                byte_pos = (epoch_id - self.epoch_offset) // 8 - start_byte
                bit_pos = (epoch_id - self.epoch_offset) % 8
                
                # Extract bit value using bitwise operations
                if 0 <= byte_pos < len(byte_data):
                    # Shift right by (7-bit_pos) to move target bit to LSB, then mask with 1
                    bit_value = (byte_data[byte_pos] >> (7 - bit_pos)) & 1
                else:
                    bit_value = 0
                results.append((epoch_id, bool(bit_value)))

            self.logger.debug(f"Got {len(results)} bits in range")
            return results
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while getting bits in range: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
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
            TypeError: If parameters have wrong types
            RedisBitmapError: If Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)

        try:
            result = await redis_conn.delete(key)
            self.logger.info(f"Cleared bitmap key '{key}': {'deleted' if result else 'not found'}")
            return bool(result)
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while clearing bitmap: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
        except RedisError as e:
            self.logger.error(f"Failed to clear bitmap: {e}")
            raise RedisBitmapError(f"Failed to clear bitmap: {e}") from e

    async def get_bit_count(self, redis_conn: aioredis.Redis, key: str) -> int:
        """
        Get the total number of set bits in the bitmap.

        Args:
            redis_conn (aioredis.Redis): Redis connection instance
            key (str): Redis key of the bitmap

        Returns:
            int: Number of set bits in the bitmap

        Raises:
            TypeError: If parameters have wrong types
            RedisBitmapError: If Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)

        try:
            result = await redis_conn.bitcount(key)
            self.logger.debug(f"Bit count for key '{key}': {result}")
            return result
        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while counting bits: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
        except RedisError as e:
            self.logger.error(f"Failed to count bits: {e}")
            raise RedisBitmapError(f"Failed to count bits: {e}") from e

    async def cleanup_old_bits(
        self,
        redis_conn: aioredis.Redis,
        key: str,
        current_epoch_id: int,
        max_epochs_to_keep: int = None
    ) -> int:
        """
        Automatically cleanup bits older than the specified number of epochs to keep.

        This method efficiently removes old epoch bits to prevent unbounded growth of the bitmap.
        It calculates the cutoff epoch based on current_epoch_id and max_epochs_to_keep, then
        clears all bits representing epochs older than the cutoff.

        Args:
            redis_conn (aioredis.Redis): Redis connection instance
            key (str): Redis key of the bitmap to clean up
            current_epoch_id (int): The current epoch ID for reference
            max_epochs_to_keep (int, optional): Maximum number of epochs to keep.
                                               Defaults to DEFAULT_MAX_EPOCHS_TO_KEEP

        Returns:
            int: Number of bytes cleared from the bitmap

        Raises:
            TypeError: If parameters have wrong types
            ValueError: If epoch parameters are invalid
            RedisBitmapError: If Redis operation fails
            RedisConnectionError: If connection fails
        """
        self._validate_connection(redis_conn)
        self._validate_key(key)

        if not isinstance(current_epoch_id, int):
            raise TypeError("current_epoch_id must be an integer")

        if max_epochs_to_keep is None:
            max_epochs_to_keep = self.DEFAULT_MAX_EPOCHS_TO_KEEP

        if not isinstance(max_epochs_to_keep, int) or max_epochs_to_keep <= 0:
            raise ValueError("max_epochs_to_keep must be a positive integer")

        if current_epoch_id - self.epoch_offset < 0:
            raise ValueError(f"current_epoch_id {current_epoch_id} must be >= epoch_offset {self.epoch_offset}")

        try:
            # Calculate the cutoff epoch - epochs older than this will be cleared
            cutoff_epoch_id = current_epoch_id - max_epochs_to_keep

            # If cutoff is before our epoch_offset, nothing to clean
            if cutoff_epoch_id <= self.epoch_offset:
                self.logger.debug(
                    f"No cleanup needed: cutoff_epoch_id {cutoff_epoch_id} <= epoch_offset {self.epoch_offset}"
                )
                return 0

            # Calculate bit positions for cleanup
            cutoff_bit_position = cutoff_epoch_id - self.epoch_offset
            
            if cutoff_bit_position <= 0:
                self.logger.debug("No bits to clean up")
                return 0

            # Get current bitmap size to avoid clearing beyond existing data
            current_size = await redis_conn.strlen(key)
            if current_size == 0:
                self.logger.debug(f"Bitmap key '{key}' is empty, nothing to clean")
                return 0

            # Calculate how many complete bytes we can safely clear
            complete_bytes_to_clear = cutoff_bit_position // 8
            remaining_bits_in_partial_byte = cutoff_bit_position % 8
            
            bytes_affected = 0
            
            # Clear complete bytes if any
            if complete_bytes_to_clear > 0:
                bytes_to_clear = min(complete_bytes_to_clear, current_size)
                if bytes_to_clear > 0:
                    self.logger.info(
                        f"Cleaning up bitmap '{key}': clearing {bytes_to_clear} complete bytes "
                        f"(epochs {self.epoch_offset} to {self.epoch_offset + (bytes_to_clear * 8) - 1})"
                    )
                    
                    # Clear complete bytes with zeros
                    zero_bytes = b'\x00' * bytes_to_clear
                    await redis_conn.setrange(key, 0, zero_bytes)
                    bytes_affected += bytes_to_clear
            
            # Handle partial byte if there are remaining bits to clear
            if remaining_bits_in_partial_byte > 0 and complete_bytes_to_clear < current_size:
                byte_position = complete_bytes_to_clear
                
                # Read the current byte
                current_byte_data = await redis_conn.getrange(key, byte_position, byte_position)
                if current_byte_data and len(current_byte_data) > 0:
                    current_byte = current_byte_data[0]
                    
                    # Create mask to clear only the old bits in this byte
                    # remaining_bits_in_partial_byte tells us how many bits to clear (from MSB)
                    mask = (0xFF >> remaining_bits_in_partial_byte)  # Keep the rightmost bits
                    new_byte = current_byte & mask
                    
                    # Write back the modified byte
                    await redis_conn.setrange(key, byte_position, bytes([new_byte]))
                    
                    if new_byte != current_byte:
                        bytes_affected += 1
                        self.logger.info(
                            f"Cleaned partial byte at position {byte_position}: "
                            f"cleared {remaining_bits_in_partial_byte} bits "
                            f"(epochs {self.epoch_offset + complete_bytes_to_clear * 8} to {cutoff_epoch_id - 1})"
                        )

            # If we cleared the entire bitmap, we might want to delete the key entirely
            # to free up memory, but we'll leave it as zeros to maintain the data structure

            self.logger.info(f"Successfully cleaned up {bytes_affected} bytes from bitmap '{key}'")
            return bytes_affected

        except ConnectionError as e:
            self.logger.error(f"Redis connection failed while cleaning up old bits: {e}")
            raise RedisConnectionError(f"Connection failed: {e}") from e
        except RedisError as e:
            self.logger.error(f"Failed to cleanup old bits: {e}")
            raise RedisBitmapError(f"Failed to cleanup old bits: {e}") from e

    async def set_bit_with_auto_cleanup(
        self,
        redis_conn: aioredis.Redis,
        key: str,
        epoch_id: int,
        max_epochs_to_keep: int = None
    ) -> bool:
        """
        Set a bit and automatically cleanup old bits if needed.

        This convenience method combines setting a bit with automatic cleanup of old data.
        It first sets the bit for the given epoch_id, then performs cleanup of epochs
        older than max_epochs_to_keep based on the current epoch_id.

        Args:
            redis_conn (aioredis.Redis): Redis connection
            key (str): Redis key for the bitmap
            epoch_id (int): Non-negative integer representing the bit position
            max_epochs_to_keep (int, optional): Maximum epochs to keep.
                                               Defaults to DEFAULT_MAX_EPOCHS_TO_KEEP

        Returns:
            bool: True if the bit was newly set, False if it was already set

        Raises:
            TypeError: If parameters have wrong types
            ValueError: If epoch_id is invalid
            RedisBitmapError: If the Redis operation fails
            RedisConnectionError: If connection fails
        """
        # Set the bit first
        bit_was_set = await self.set_bit(redis_conn, key, epoch_id)

        # Perform cleanup using the current epoch_id as reference
        try:
            bytes_cleaned = await self.cleanup_old_bits(
                redis_conn, key, epoch_id, max_epochs_to_keep
            )
            if bytes_cleaned > 0:
                self.logger.info(f"Auto-cleanup removed {bytes_cleaned} bytes of old data")
        except Exception as e:
            # Log cleanup errors but don't fail the set operation
            self.logger.warning(f"Auto-cleanup failed: {e}")

        return bit_was_set


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
