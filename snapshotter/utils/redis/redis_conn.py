import contextlib
from functools import wraps

import redis
import redis.exceptions as redis_exc
from redis import asyncio as aioredis
from redis.asyncio.connection import ConnectionPool

from snapshotter.settings.config import settings as settings_conf
from snapshotter.utils.default_logger import default_logger

# Setup logging
logger = default_logger.bind(module='RedisConn')

# Redis connection configuration
REDIS_CONN_CONF = {
    'host': settings_conf.redis.host,
    'port': settings_conf.redis.port,
    'password': settings_conf.redis.password,
    'db': settings_conf.redis.db,
    'retry_on_error': [redis.exceptions.ReadOnlyError],
}


def construct_redis_url():
    """
    Constructs a Redis URL based on the REDIS_CONN_CONF dictionary.

    Returns:
        str: Redis URL constructed from REDIS_CONN_CONF dictionary.
    """
    if REDIS_CONN_CONF['password']:
        return (
            f'redis://{REDIS_CONN_CONF["password"]}@{REDIS_CONN_CONF["host"]}:{REDIS_CONN_CONF["port"]}'
            f'/{REDIS_CONN_CONF["db"]}'
        )
    else:
        return f'redis://{REDIS_CONN_CONF["host"]}:{REDIS_CONN_CONF["port"]}/{REDIS_CONN_CONF["db"]}'

# Reference: https://github.com/redis/redis-py/issues/936


async def get_aioredis_pool(pool_size=200):
    """
    Returns an aioredis Redis connection pool.

    Args:
        pool_size (int): Maximum number of connections to the Redis server.

    Returns:
        aioredis.Redis: Redis connection pool.
    """
    pool = ConnectionPool.from_url(
        url=construct_redis_url(),
        retry_on_error=[redis.exceptions.ReadOnlyError],
        max_connections=pool_size,
    )

    return aioredis.Redis(connection_pool=pool)


@contextlib.contextmanager
def create_redis_conn(
    connection_pool: redis.BlockingConnectionPool,
) -> redis.Redis:
    """
    Context manager for creating a Redis connection using a connection pool.

    Args:
        connection_pool (redis.BlockingConnectionPool): The connection pool to use.

    Yields:
        redis.Redis: A Redis connection object.

    Raises:
        redis_exc.RedisError: If there is an error connecting to Redis.
    """
    try:
        redis_conn = redis.Redis(connection_pool=connection_pool)
        yield redis_conn
    except redis_exc.RedisError:
        raise
    except KeyboardInterrupt:
        pass


def provide_async_redis_conn_insta(fn):
    """
    A decorator function that provides an async Redis connection instance to the decorated function.

    Args:
        fn (callable): The function to be decorated.

    Returns:
        callable: The decorated function with an async Redis connection instance.
    """
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        arg_conn = 'redis_conn'
        if kwargs.get(arg_conn):
            return await fn(*args, **kwargs)
        else:
            # Create a single connection using the high-level aioredis interface
            connection = await aioredis.Redis(
                host=REDIS_CONN_CONF['host'],
                port=REDIS_CONN_CONF['port'],
                db=REDIS_CONN_CONF['db'],
                password=REDIS_CONN_CONF['password'],
                retry_on_error=[redis.exceptions.ReadOnlyError],
            )
            kwargs[arg_conn] = connection
            try:
                return await fn(*args, **kwargs)
            except Exception:
                raise
            finally:
                try:  # Ignore residual errors
                    await connection.close()
                except:
                    pass

    return wrapped


class RedisPoolCache:
    """
    A class that manages a Redis connection pool cache.
    """
    _aioredis_pool: aioredis.Redis
    _pool_size: int

    def __init__(self, pool_size=2000):
        """
        Initializes a Redis connection object with the specified connection pool size.

        Args:
            pool_size (int): The maximum number of connections to keep in the pool.
        """
        self._aioredis_pool = None
        self._pool_size = pool_size

    async def populate(self):
        """
        Populates the Redis connection pool with the specified number of connections.
        """
        if not self._aioredis_pool:
            self._aioredis_pool = await get_aioredis_pool(
                self._pool_size,
            )
