import asyncio
import aiohttp


class AsnycGrab(object):

    def __init__(self, req_info, parse, session_url, max_threads=10):
        self.session_url = session_url
        self.req_infos = req_info
        self.parse = parse
        self.results = {}
        self.max_threads = max_threads

    async def get_data(self, session, work_queue):
        while not work_queue.empty():
            req_info = await work_queue.get()
            async with session.get(req_info['url'], timeout=30) as rsp:
                assert rsp.status == 200
                # text = await rsp.text()
                json = await rsp.json(content_type=None)
                self.results[req_info['code']] = self.parse(req_info, json)

    async def eventloop(self):
        q = asyncio.Queue()
        [q.put_nowait(req) for req in self.req_infos]
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
        async with aiohttp.ClientSession(headers=headers) as session:
            await session.get(self.session_url)
            tasks = [self.get_data(session, q)
                     for _ in range(self.max_threads)]
            await asyncio.wait(tasks)

    def start(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.eventloop())
        # loop.close()


if __name__ == '__main__':
    async_example = AsnycGrab(['http://edmundmartin.com',
                               'https://www.udemy.com',
                               'https://github.com/',
                               'https://zhangslob.github.io/',
                               'https://www.zhihu.com/'], 5)
    async_example.eventloop()
    print(async_example.results)
