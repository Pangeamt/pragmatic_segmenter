import asyncio
import logging
from pragmatic_segmenter.pragmatic_segmenter_server_shuttle import PragmaticSegmenterServerShuttle
from pragmatic_segmenter.pragmatic_segmenter_client import PragmaticSegmenterClient

logging.basicConfig(level=logging.DEBUG)


async def main():

    host = '0.0.0.0'
    port = 5000

    segmenter_server = PragmaticSegmenterServerShuttle(
        rackup_config_path='config.ru',
        host=host,
        port=port
    )

    segmenter_client = PragmaticSegmenterClient(
        host=host,
        port=port
    )
    await segmenter_server.start()

    results = await segmenter_client.segment(
        texts=["   Hello. My name is John. And you   "],
        lang="en"
    )
    print(results)
    await segmenter_server.stop()
    await asyncio.sleep(10000)


asyncio.run(main())