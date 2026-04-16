"""
API-key auth and per-user rate limits for FastAPI routes (Depends-based).

Wiring status: ``core_api.py`` does not import this module; it uses ``rate_limiter.generic_rate_limiter``
from ``public_rate_limit.py`` for coarse global limits. Optional per-route enforcement: add
``Depends(rate_limit_auth_check)`` to endpoints. The auth *service* (``server_entry.py``) does not use these helpers.

Depends on ``async_limits`` (see ``pyproject.toml``).
"""

import time
from datetime import datetime
from datetime import timedelta

from async_limits import parse_many
from fastapi import Depends
from fastapi import Request
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis

from snapshotter.auth.helpers.data_models import AppOwnerModel
from snapshotter.auth.helpers.data_models import AuthCheck
from snapshotter.auth.helpers.data_models import RateLimitAuthCheck
from snapshotter.auth.helpers.data_models import UserStatusEnum
from snapshotter.auth.helpers.rate_limiter import generic_rate_limiter
from snapshotter.auth.helpers.redis_keys import api_key_to_owner_key
from snapshotter.auth.helpers.redis_keys import user_active_api_keys_set
from snapshotter.auth.helpers.redis_keys import user_details_htable


async def incr_success_calls_count(
    auth_redis_conn: aioredis.Redis,
    rate_limit_auth_dep: RateLimitAuthCheck,
):
    """
    Increment the successful calls count for a user.

    Args:
        auth_redis_conn (aioredis.Redis): Redis connection for authentication.
        rate_limit_auth_dep (RateLimitAuthCheck): Rate limit authentication dependency.
    """
    await auth_redis_conn.hincrby(
        name=user_details_htable(rate_limit_auth_dep.owner.email),
        key='callsCount',
        amount=1,
    )


async def incr_throttled_calls_count(
    auth_redis_conn: aioredis.Redis,
    rate_limit_auth_dep: RateLimitAuthCheck,
):
    """
    Increment the throttled calls count for a user.

    Args:
        auth_redis_conn (aioredis.Redis): Redis connection for authentication.
        rate_limit_auth_dep (RateLimitAuthCheck): Rate limit authentication dependency.
    """
    await auth_redis_conn.hincrby(
        name=user_details_htable(rate_limit_auth_dep.owner.email),
        key='throttledCount',
        amount=1,
    )


def inject_rate_limit_fail_response(
    rate_limit_auth_check_dependency: RateLimitAuthCheck,
) -> JSONResponse:
    """
    Generate a JSON response for rate limit failure.

    Args:
        rate_limit_auth_check_dependency (RateLimitAuthCheck): Rate limit authentication dependency.

    Returns:
        JSONResponse: A JSON response with appropriate status code and headers.
    """
    if rate_limit_auth_check_dependency.authorized:
        # User is authorized but exceeded rate limit
        response_body = {
            'error': {
                'details': (
                    'Rate limit exceeded:'
                    f' {rate_limit_auth_check_dependency.violated_limit}. Check'
                    ' response body and headers for more details on backoff.'
                ),
                'data': {
                    'rate_violated': str(
                        rate_limit_auth_check_dependency.violated_limit,
                    ),
                    'retry_after': rate_limit_auth_check_dependency.retry_after,
                    'violating_domain': rate_limit_auth_check_dependency.current_limit,
                },
            },
        }
        response_headers = {
            'Retry-After': (
                datetime.now() + timedelta(rate_limit_auth_check_dependency.retry_after)
            ).isoformat(),
        }
        response_status = 429
    else:
        # User is not authorized
        response_headers = dict()
        response_body = {
            'error': {
                'details': rate_limit_auth_check_dependency.reason,
            },
        }
        if 'cache error' in rate_limit_auth_check_dependency.reason:
            response_status = 500
        elif (
            'no API key' in rate_limit_auth_check_dependency.reason or
            'bad API key' in rate_limit_auth_check_dependency.reason
        ):
            response_status = 401
        else:  # usual auth issues like bad API key
            response_status = 200
    return JSONResponse(
        content=response_body,
        status_code=response_status,
        headers=response_headers,
    )


# TODO: cacheize for better performance
async def check_user_details(
    api_key,
    redis_conn: aioredis.Redis,
):
    """
    Check user details based on the provided API key.

    Args:
        api_key (str): The API key to check.
        redis_conn (aioredis.Redis): Redis connection for authentication.

    Returns:
        AuthCheck: An AuthCheck object containing authorization details.
    """
    owner_email = await redis_conn.get(api_key_to_owner_key(api_key))
    if not owner_email:
        return AuthCheck(
            authorized=False,
            api_key=api_key,
            reason='bad API key',
        )
    else:
        owner_email = owner_email.decode('utf-8')
        owner_details_b = await redis_conn.hgetall(
            user_details_htable(owner_email),
        )
        owner_details_dec = {
            k.decode('utf-8'): v.decode('utf-8') for k, v in owner_details_b.items()
        }
        owner_details = AppOwnerModel(**owner_details_dec)
        return AuthCheck(
            authorized=await redis_conn.sismember(
                user_active_api_keys_set(owner_email),
                api_key,
            ),
            api_key=api_key,
            owner=owner_details,
        )


async def auth_check(
    request: Request,
) -> AuthCheck:
    """
    Perform authentication check for the incoming request.

    Args:
        request (Request): The incoming FastAPI request object.

    Returns:
        AuthCheck: An AuthCheck object containing authorization details.
    """
    core_settings = request.app.state.core_settings.core_api
    auth_redis_conn: aioredis.Redis = request.app.state.auth_aioredis_pool
    expected_header_key_for_auth = core_settings.auth.header_key
    api_key_in_header = (
        request.headers[expected_header_key_for_auth]
        if expected_header_key_for_auth in request.headers
        else None
    )
    if not api_key_in_header:
        # Handle public access based on IP address
        if 'CF-Connecting-IP' in request.headers:
            user_ip = request.headers['CF-Connecting-IP']
        elif 'X-Forwarded-For' in request.headers:
            proxy_data = request.headers['X-Forwarded-For']
            ip_list = proxy_data.split(',')
            user_ip = ip_list[0]  # first address in list is User IP
        else:
            user_ip = request.client.host  # For local development
        ip_user_dets_b = await auth_redis_conn.hgetall(
            user_details_htable(user_ip),
        )
        if not ip_user_dets_b:
            # Create new public user if not exists
            public_owner = AppOwnerModel(
                email=user_ip,
                rate_limit=core_settings.public_rate_limit,
                active=UserStatusEnum.active,
                callsCount=0,
                throttledCount=0,
                next_reset_at=int(time.time()) + 86400,
            )
            await auth_redis_conn.hset(
                user_details_htable(user_ip),
                mapping=public_owner.dict(),
            )
        else:
            # Load existing public user details
            ip_owner_details = {
                k.decode('utf-8'): v.decode('utf-8') for k, v in ip_user_dets_b.items()
            }
            public_owner = AppOwnerModel(**ip_owner_details)
        return AuthCheck(
            authorized=True,
            owner=public_owner,
            api_key='dummy',
        )
    else:
        # Check user details for API key authentication
        return await check_user_details(api_key_in_header, auth_redis_conn)


async def rate_limit_auth_check(
    request: Request,
    auth_check: AuthCheck = Depends(auth_check),
) -> RateLimitAuthCheck:
    """
    Perform rate limit and authentication check for the incoming request.

    Args:
        request (Request): The incoming FastAPI request object.
        auth_check (AuthCheck): The result of the initial authentication check.

    Returns:
        RateLimitAuthCheck: A RateLimitAuthCheck object containing rate limit and authorization details.
    """
    if auth_check.authorized:
        auth_redis_conn: aioredis.Redis = request.app.state.auth_aioredis_pool
        try:
            # Perform rate limiting check
            passed, retry_after, violated_limit = await generic_rate_limiter(
                parsed_limits=parse_many(auth_check.owner.rate_limit),
                key_bits=[
                    # using instance_id instead of chain_id because we just need a unique identifier
                    # for shared auth redis(don't really need chain id)
                    str(request.app.state.core_settings.instance_id),
                    auth_check.owner.email,
                ],
                redis_conn=auth_redis_conn,
            )
        except:
            # Handle internal cache error
            auth_check.authorized = False
            auth_check.reason = 'internal cache error'
            return RateLimitAuthCheck(
                **auth_check.dict(),
                rate_limit_passed=False,
                retry_after=1,
                violated_limit='',
                current_limit=auth_check.owner.rate_limit,
            )
        else:
            ret = RateLimitAuthCheck(
                **auth_check.dict(),
                rate_limit_passed=passed,
                retry_after=retry_after,
                violated_limit=violated_limit,
                current_limit=auth_check.owner.rate_limit,
            )
            if not passed:
                await incr_throttled_calls_count(auth_redis_conn, ret)
            return ret
        finally:
            # Reset user's call counts if the reset time has passed
            if auth_check.owner.next_reset_at <= int(time.time()):
                owner_updated_obj = auth_check.owner.copy(deep=True)
                owner_updated_obj.callsCount = 0
                owner_updated_obj.throttledCount = 0
                owner_updated_obj.next_reset_at = int(time.time()) + 86400
                await auth_redis_conn.hset(
                    name=user_details_htable(owner_updated_obj.email),
                    mapping=owner_updated_obj.dict(),
                )
    else:
        # Handle unauthorized access
        return RateLimitAuthCheck(
            **auth_check.dict(),
            rate_limit_passed=False,
            retry_after=1,
            violated_limit='',
            current_limit='',
        )
