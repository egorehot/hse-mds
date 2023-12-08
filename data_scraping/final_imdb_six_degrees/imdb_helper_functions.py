import os
import re
import json
import asyncio
from functools import lru_cache
from datetime import datetime
from collections import defaultdict, deque
from typing import Dict, Iterable, Iterator, List, Optional, Any, Set

from async_lru import alru_cache
from aiohttp import ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientConnectorError, ClientResponseError
from bs4 import BeautifulSoup


FILE_DATE_FORMAT = '%Y-%m-%dT%H:%M'
IMDB_URL = 'https://imdb.com'
HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'en',
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0',
           'Connection': 'keep-alive',
           'Host': 'www.imdb.com'}
LIMIT_TCP = 25
MAX_RETRIES = 3
RETRY_FACTOR = 10
RETRY_BASE = 3


@lru_cache(maxsize=128)
def create_url(url_path: str, fullcredits: bool = True) -> str:
    if not url_path.endswith('fullcredits/') and fullcredits:
        url_path += 'fullcredits/'
    if url_path.startswith('/'):
        return IMDB_URL + url_path
    else:
        return url_path


def create_urls(elements: Iterable[Any], fullcredits: bool = True) -> List[str]:
    urls = []
    for element in elements:
        if isinstance(element, str):
            urls.append(create_url(element, fullcredits))
        elif isinstance(element, Dict):
            urls.append(create_url(element.get('url'), fullcredits))
        else:
            raise ValueError(f'Unexpected result type {type(element)} expecting Dict or str')
    return urls


@alru_cache(maxsize=32)
async def fetch_page(session: ClientSession, url: str, retry_count: int = 0) -> str:
    async with session.get(url) as response:
        try:
            response.raise_for_status()
            return await response.text()
        except (ClientResponseError, ClientConnectorError) as err:
            if retry_count < MAX_RETRIES:
                print(f"\tRetrying {url}, got {err}, retry №{retry_count + 1}")
                await asyncio.sleep(RETRY_FACTOR * RETRY_BASE ** retry_count)
                return await fetch_page(session, url, retry_count + 1)
            else:
                print(f"\tFailed to fetch {url} after {MAX_RETRIES} retries.")
                return ''


async def get_soups(urls: Iterable[str]) -> List[BeautifulSoup]:
    async with ClientSession(headers=HEADERS, connector=TCPConnector(limit=LIMIT_TCP)) as session:
        tasks = [fetch_page(session, url) for url in urls]
        pages = await asyncio.gather(*tasks)
        return [BeautifulSoup(page, features='html.parser') for page in pages]


def _get_actor_by_movie(cast_page_soup: BeautifulSoup) -> Iterator[Dict[str, str]]:
    cast_tables = cast_page_soup.find_all('table', attrs={'class': 'cast_list'})

    for table in cast_tables:
        name_tags = table.find_all('td', attrs={'class': False, 'style': False})
        for name_tag in name_tags:
            url = name_tag.find('a').attrs.get('href').strip()
            yield {'name': name_tag.text.strip(), 'url': url[:url.rfind('?')]}


def _get_movie_by_actor(actor_page_soup: BeautifulSoup,
                        omitted_videos: Iterable[str] = None) -> Iterator[Dict[str, str]]:
    omitted_videos = omitted_videos or set()
    omitted_videos = {f'({film_category})' for film_category in omitted_videos}

    act_headers = actor_page_soup.find_all('div', attrs={'data-category': re.compile('act([or]|[ress])')})
    for act_header in act_headers:
        movie_section = act_header.find_next_sibling('div', attrs={'class': 'filmo-category-section'})
        if movie_section:
            for row in movie_section.find_all('div', recursive=False):
                video_type = row.find('b').next_sibling.strip()
                if not row.find_all('a', attrs={'class': True}) and video_type not in omitted_videos:
                    movie_tag = row.find('a')
                    url = movie_tag.attrs.get('href').strip()
                    yield {'name': movie_tag.text.strip(), 'url': url[:url.rfind('?')]}


def check_match(actor: str, target_actor: str, distance: int, actors_cache: Set[Optional[str]],
                next_level_actors: Optional[Set[str]] = None) -> Optional[int]:
    if actor == target_actor:
        return distance
    elif target_actor in actors_cache:
        return distance + 1  # TODO можем превысить max_distance
    elif next_level_actors:
        if actor in next_level_actors:
            return distance + 1  # TODO можем превысить max_distance
    return


def modify_actor_url(url: str) -> str:
    if url.startswith('/name'):
        return url
    elif 'imdb.com' in url:
        name_position = url.find('/name/')
        if name_position > 0:
            return url[name_position:]
        else:
            raise ValueError(f"/name/ NOT FOUND IN {url}")
    else:
        raise ValueError(f'WRONG URL {url}')


@lru_cache(maxsize=128)
def _get_movie_description(movie_soup: BeautifulSoup) -> Optional[str]:
    movie_description = movie_soup.find('span', attrs={'data-testid': "plot-xl"})
    if movie_description:
        return movie_description.text.strip()
    else:
        print("GOT WRONG SOUP")


# class ActorNode:
#     def __init__(self, link, name=None):
#         self.link = link
#         self.name = name
#
#     def __str__(self):
#         return self.link


class ActorsGraph:
    def __init__(self):
        self._graph = defaultdict(set)

    def __dict__(self):
        if hasattr(self, '_graph') and self._graph:
            graph = {}
            for node, adjacents in self._graph.items():
                node = str(node)
                if isinstance(adjacents, set):
                    adjacents = [str(node) for node in adjacents]
                graph[node] = adjacents
            return graph

    def get_graph(self):
        return self._graph

    def set_graph(self, graph: Dict[str, Optional[Iterable]]):
        for key, value in graph.items():
            self._graph[key].update(value)

    def get_adjacents(self, actor: str):
        return self._graph.get(actor, set())

    def add_edge(self, source: str, target: str):
        if source != target:
            self._graph[target].add(source)
            self._graph[source].add(target)

    def find_shortest_distance(self, source, target):
        visited = set()
        queue = deque()

        queue.append((source, 0))
        visited.add(source)

        while queue:
            node, distance = queue.popleft()
            for adjacent in self._graph.get(node):
                if adjacent == target:
                    return distance
                if adjacent in visited:
                    continue
                distance += 1
                queue.append((adjacent, distance))
            visited.add(node)

        return

    def save_file(self, path: Optional[str] = None):
        if not path:
            path = os.getcwd()
        today = datetime.today().strftime(FILE_DATE_FORMAT)
        with open(os.path.join(path, f'{today}_actors_graph.json'), 'w') as file:
            json.dump(self.__dict__(), file)

    def set_graph_from_file(self, dir_path: str = None):
        if not dir_path:
            dir_path = os.getcwd()
        files = ActorsGraph.get_sorted_by_date_files(dir_path)
        filepath = os.path.join(dir_path, files[0] if files else '_')
        if os.path.exists(filepath):
            with open(filepath) as file:
                self.set_graph(json.load(file))

    @staticmethod
    def get_sorted_by_date_files(path: str) -> List[str]:
        files_list = [file for file in os.listdir(path) if file.endswith('json')]
        if not files_list:
            return []
        try:
            sorted_list = sorted(files_list, key=lambda x: datetime.strptime(x[:x.find('_')], FILE_DATE_FORMAT),
                                 reverse=True)
            return sorted_list
        except ValueError:
            return []
