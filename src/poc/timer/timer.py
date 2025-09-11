# Timer sourced from  https://stackoverflow.com/questions/45419723/python-timer-with-asyncio-coroutine/67833774

import asyncio

class Timer:
    def __init__(self, timeout, callback):
        self._timeout = timeout
        self._callback = callback
        self._task = asyncio.ensure_future(self._job())

    async def _job(self):
        await asyncio.sleep(self._timeout)
        await self._callback()

    def cancel(self):
        self._task.cancel()


async def timeout_callback():
    print('echo!')


async def main():
    print('\nfirst example:')
    timer = Timer(2, timeout_callback)  # set timer for two seconds
    #await asyncio.sleep(2.5)  # wait to see timer works

    print('\nsecond example:')
    timer = Timer(2, timeout_callback)  # set timer for two seconds
    #await asyncio.sleep(1)
    timer.cancel()  # cancel it
    #await asyncio.sleep(1.5)  # and wait to see it won't call callback
    print('end of main')

    while(True):
        try:
            await asyncio.sleep(0.01) # 10 Milliseconds
            print(f'Timer is running...')
        except asyncio.CancelledError:
            break


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
try:
    loop.run_until_complete(main())
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
