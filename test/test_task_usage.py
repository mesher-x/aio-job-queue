import asyncio
import pytest
import aioredis
import aioredisqueue


@pytest.mark.asyncio
async def test_basic_put_get_and_get():
	r = await aioredis.create_redis(('localhost', 6379), db=0)
	queue = aioredisqueue.queue.Queue(r)
	
	message = b'payload'
	await queue.put(message)
	result = await queue.get()
	assert result.payload == message

	queue.stop()
	r.close()
	await r.wait_closed()

	
async def get_ack(queue):
	task = await queue.get()
	await asyncio.sleep(2)
	await task.ack()


async def get_fail_ack(queue):
	task = await queue.get()
	await asyncio.sleep(0.5)
	await task.fail()
	task = await queue.get()
	await asyncio.sleep(0.5)
	await task.fail()
	task = await queue.get()
	await asyncio.sleep(0.5)
	await task.ack()


@pytest.mark.asyncio
async def test_ack():
	r = await aioredis.create_redis(('localhost', 6379), db=0)
	queue = aioredisqueue.queue.Queue(r)
	
	message = b'payload'
	await queue.put(message)
	await get_ack(queue)


	queue.stop()
	r.close()
	await r.wait_closed()


@pytest.mark.asyncio
async def test_fail_ack():
	r = await aioredis.create_redis(('localhost', 6379), db=0)
	queue = aioredisqueue.queue.Queue(r)
	
	message = b'payload'
	await queue.put(message)
	await get_fail_ack(queue)

	queue.stop()
	r.close()
	await r.wait_closed()
