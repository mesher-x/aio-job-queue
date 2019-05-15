import timeit
import asyncio
import pytest
import aioredis
import aioredisqueue

@pytest.mark.asyncio
async def test_requeue_method():
    redis = await aioredis.create_redis(('localhost', 6379), db=0)
    queue = aioredisqueue.queue.Queue(redis)

    await queue.put(b'payload')
    await queue.get()
    main_queue_len = await redis.llen(queue._keys['queue'])
    ack_len = await redis.hlen(queue._keys['ack'])
    assert ack_len >= 1
    await redis.delete(queue._keys['last_requeue'])
    now = int(timeit.default_timer() * 1000)
    results = await queue.requeue()
    assert results[0] == b'ok'
    assert len(results[1]) == ack_len
    assert len(results[3]) == ack_len
    assert results[2][-1] == main_queue_len + ack_len
    assert int(await redis.get(queue._keys['last_requeue'])) == now

    redis.close()
    await redis.wait_closed()


@pytest.mark.asyncio
async def test_too_early():
    redis = await aioredis.create_redis(('localhost', 6379), db=0)
    queue = aioredisqueue.queue.Queue(redis)

    now = int(timeit.default_timer() * 1000)
    await redis.set(queue._keys['last_requeue'], now)
    results = await queue.requeue()
    assert results[0] == b'error'

    redis.close()
    await redis.wait_closed()


@pytest.mark.asyncio
async def test_in_background():
    redis = await aioredis.create_redis(('localhost', 6379), db=0)
    requeue_interval = 100
    eps = 0.01
    queue = aioredisqueue.queue.Queue(redis, requeue_interval=requeue_interval)

    await redis.delete(queue._keys['last_requeue'])

    await queue.put(b'payload')
    await asyncio.sleep(eps)
    task = await queue.get()

    await asyncio.sleep(requeue_interval / 1000)
    assert await redis.hget(queue._keys['ack'], task.id) is not None

    await asyncio.sleep(requeue_interval / 1000)
    assert await redis.hget(queue._keys['ack'], task.id) is None

    redis.close()
    await redis.wait_closed()
