import aiohttp
from typing import List, Tuple


class PragmaticSegmenterClient:
    def __init__(self,
                 host: str,
                 port: int,
                 ):
        self._host = host
        self._port = port
        self._url = 'http://'+ host + ':' + str(port) + '/segment'

    async def segment(self,
                      texts: List[str],
                      lang: str,
                      use_white_segmenter: bool = True)->List[Tuple[List[str], str]]:

        """
        Segment texts
        :param texts: A list of texts to segment
        :param lang: The language of the texts
        :param use_white_segmenter:
        :return:  A list of tuples:
        - The first item ot the tuple is a list of segments
        - The second one is a string with the segmentation mask
        """

        data = {
            'lang': lang,
            'texts': texts,
            'use_white_segmenter': use_white_segmenter
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self._url, json=data) as response:
                    if response.status == 200:
                        results = await response.json()
                        output = []
                        for result in results:
                            segments = result["segments"]
                            mask = result['mask']
                            output.append((segments, mask))
                        return output
        except:
            #TODO raise Error
            pass


