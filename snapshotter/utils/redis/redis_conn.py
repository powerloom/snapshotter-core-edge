import contextlib
from functools import wraps

import redis
import redis.exceptions as redis_exc
from redis import asyncio as aioredis
from redis.asyncio.connection import ConnectionPool

from snapshotter.utils.default_logger import default_logger

# Setup logging
logger = default_logger.bind(module='RedisConn')


def construct_redis_url(redis_conf: dict):
    """
    Constructs a Redis URL based on the REDIS_CONN_CONF dictionary.

    Returns:
        str: Redis URL constructed from REDIS_CONN_CONF dictionary.
    """
    if redis_conf['password']:
        return (
            f'redis://{redis_conf["password"]}@{redis_conf["host"]}:{redis_conf["port"]}'
            f'/{redis_conf["db"]}'
        )
    else:
        return f'redis://{redis_conf["host"]}:{redis_conf["port"]}/{redis_conf["db"]}'

# Reference: https://github.com/redis/redis-py/issues/936


async def get_aioredis_pool(pool_size=200, redis_conf: dict = dict()):
    """
    Returns an aioredis Redis connection pool.

    Args:
        pool_size (int): Maximum number of connections to the Redis server.

    Returns:
        aioredis.Redis: Redis connection pool.
    """
    pool = ConnectionPool.from_url(
        url=construct_redis_url(redis_conf),
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
            from snapshotter.settings.config import settings as settings_conf
            redis_conf = {
                'host': settings_conf.redis.host,
                'port': settings_conf.redis.port,
                'password': settings_conf.redis.password,
                'db': settings_conf.redis.db,
            }

            connection = await aioredis.Redis(
                host=redis_conf['host'],
                port=redis_conf['port'],
                db=redis_conf['db'],
                password=redis_conf['password'],
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

    def __init__(self, pool_size=2000, redis_conf: dict = dict()):
        """
        Initializes a Redis connection object with the specified connection pool size.

        Args:
            pool_size (int): The maximum number of connections to keep in the pool.
        """
        self._pool_size = pool_size
        if not redis_conf:
            from snapshotter.settings.config import settings as settings_conf
            redis_conf = {
                'host': settings_conf.redis.host,
                'port': settings_conf.redis.port,
                'password': settings_conf.redis.password,
                'db': settings_conf.redis.db,
            }
        self._redis_conf = redis_conf

    async def populate(self):
        """
        Populates the Redis connection pool with the specified number of connections.
        """
        if not self._aioredis_pool:
            self._aioredis_pool = await get_aioredis_pool(
                self._pool_size,
                self._redis_conf,
            )

    def get_redis_conn(self):
        """
        Returns a Redis connection object.
        """
        return self._aioredis_pool
