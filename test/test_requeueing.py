import aioredis
import aioredisqueue
import asyncio
import pytest
import timeit

@pytest.mark.asyncio
async def test_requeue_method():
    r = await aioredis.create_redis(('localhost', 6379), db=0)
    q = aioredisqueue.queue.Queue(r)

    await q.put(b'payload')
    await q.get()
    main_queue_len = await r.llen(q._keys['queue'])
    ack_len = await r.hlen(q._keys['ack'])
    assert ack_len >= 1
    await r.delete(q._keys['last_requeue'])
    now = int(timeit.default_timer() * 1000)
    results = await q._requeue(now)
    assert len(results[1]) == ack_len
    assert len(results[3]) == ack_len
    assert results[2][-1] == main_queue_len + ack_len
    assert int(await r.get(q._keys['last_requeue'])) == now

    r.close()
    await r.wait_closed()


@pytest.mark.asyncio
async def test_too_early():
    r = await aioredis.create_redis(('localhost', 6379), db=0)
    q = aioredisqueue.queue.Queue(r)

    now = int(timeit.default_timer() * 1000)
    await r.set(q._keys['last_requeue'], now)
    results = await q._requeue(now)
    assert results[0] == b'error'

    r.close()
    await r.wait_closed()


@pytest.mark.asyncio
async def test_in_background():
    r = await aioredis.create_redis(('localhost', 6379), db=0)
    requeue_interval = 100
    eps = 0.01
    q = aioredisqueue.queue.Queue(r, requeue_interval=requeue_interval)

    await r.delete(q._keys['last_requeue'])

    await q.put(b'payload')
    await asyncio.sleep(eps)
    task = await q.get()

    await asyncio.sleep(requeue_interval / 1000)
    assert await r.hget(q._keys['ack'], task.id) is not None

    await asyncio.sleep(requeue_interval / 1000)
    assert await r.hget(q._keys['ack'], task.id) is None

    r.close()
    await r.wait_closed()
