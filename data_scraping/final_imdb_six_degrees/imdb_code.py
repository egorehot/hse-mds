import os
import asyncio
from functools import lru_cache
from typing import List, Dict, Optional, Iterator, Set, Iterable

from bs4 import BeautifulSoup

from imdb_helper_functions import _get_actor_by_movie, _get_movie_by_actor, get_soups,\
    ActorsGraph, check_match, create_url, create_urls, _get_movie_description, modify_actor_url


OMITTED_VIDEOS = ('TV Series', 'Short', 'Video Game', 'Video short', 'Video', 'TV Movie', 'TV Mini Series',
                  'TV Mini-Series', 'TV Series short', 'TV Special', 'Music Video', 'Music Video short')
CURRENT_DIR = os.getcwd()


@lru_cache(maxsize=64)
def get_actors_by_movie_soup(cast_page_soup: BeautifulSoup,
                             num_of_actors_limit: Optional[int] = None
                             ) -> List[Dict[str, str]]:
    """
    Function for parsing movie page from imdb.com/title/{movieId}/fullcredits

    :param cast_page_soup:
    :param num_of_actors_limit:
    :return:
    """
    if num_of_actors_limit:
        actors = []
        for i, actor in enumerate(_get_actor_by_movie(cast_page_soup)):
            if i > num_of_actors_limit:
                break
            actors.append(actor)
        return actors

    return [actor for actor in _get_actor_by_movie(cast_page_soup)]


@lru_cache(maxsize=64)
def get_movies_by_actor_soup(actor_page_soup: BeautifulSoup,
                             num_of_movies_limit: Optional[int] = None
                             ) -> List[Dict[str, str]]:
    """
    Function for parsing movie page from imdb.com/name/{actorId}/fullcredits

    :param actor_page_soup:
    :param num_of_movies_limit:
    :return:
    """
    if num_of_movies_limit:
        movies = []
        for i, movie in enumerate(_get_movie_by_actor(actor_page_soup, omitted_videos=OMITTED_VIDEOS), 1):
            if i > num_of_movies_limit:
                break
            movies.append(movie)
        return movies

    return [movie for movie in _get_movie_by_actor(actor_page_soup, omitted_videos=OMITTED_VIDEOS)]


# In my opinion this function should be in this file, not in imdb_helper_functions,
# because it uses functions from the current file
def get_next_level_actors(actors: Iterable[str], visited_movies: Optional[Set[str]],
                          num_of_movies_limit: Optional[int], num_of_actors_limit: Optional[int]
                          ) -> Iterator[Dict[str, str]]:
    """
    Function gets the neighboring actors of the given actor.

    :param actors:
    :param visited_movies:
    :param num_of_movies_limit:
    :param num_of_actors_limit:
    :return: adjusted actors
    """
    actors_urls = create_urls(actors)
    actor_soups = asyncio.run(get_soups(actors_urls))
    for actor, actor_soup in zip(actors, actor_soups):
        movies = get_movies_by_actor_soup(actor_soup, num_of_movies_limit)
        movies_paths = [movie.get('url') for movie in movies if movie.get('url') not in visited_movies]
        movie_urls = create_urls(movies_paths)
        visited_movies.update(movies_paths)
        movie_soups = asyncio.run(get_soups(movie_urls))
        for movie_soup in movie_soups:
            for movie_next_actor in get_actors_by_movie_soup(movie_soup, num_of_actors_limit):
                yield actor, movie_next_actor


def get_movie_distance(actor_start_url: str, actor_end_url: str, max_distance: Optional[int] = 5,
                       num_of_actors_limit: Optional[int] = None, num_of_movies_limit: Optional[int] = None
                       ) -> Optional[int]:
    """
    Function finds distance between two actors.

    :param actor_start_url:
    :param actor_end_url:
    :param max_distance:
    :param num_of_actors_limit:
    :param num_of_movies_limit:
    :return: distance
    """
    actor_start_url = modify_actor_url(actor_start_url)
    actor_end_url = modify_actor_url(actor_end_url)

    start_queue = [actor_start_url]
    end_queue = [actor_end_url]
    visited_movies = set()  # TODO можно попробовать кэшировать посещённые фильмы в файл с графом

    actors_graph = ActorsGraph()
    actors_graph.set_graph_from_file(CURRENT_DIR)
    current_distance = 0

    while start_queue or end_queue:
        start_next_level_actors = set()

        for start_actor in start_queue:
            actor_cache = actors_graph.get_adjacents(start_actor)
            result = check_match(start_actor, actor_end_url, current_distance, actor_cache)
            if result:
                actors_graph.save_file(CURRENT_DIR)
                return result
            start_next_level_actors.add(start_actor)
        start_queue.clear()

        end_next_level_actors = set()
        for end_actor in end_queue:
            actor_cache = actors_graph.get_adjacents(end_actor)
            result = check_match(end_actor, actor_start_url, current_distance, actor_cache, start_next_level_actors)
            if result:
                actors_graph.save_file(CURRENT_DIR)
                return result
            end_next_level_actors.add(end_actor)
        end_queue.clear()

        current_distance += 1
        if current_distance > max_distance:
            actors_graph.save_file(CURRENT_DIR)
            return

        for actor, next_actor in get_next_level_actors(start_next_level_actors, visited_movies,
                                                       num_of_movies_limit, num_of_actors_limit):
            actors_graph.add_edge(actor, next_actor.get('url'))
            start_queue.append(next_actor.get('url'))
        start_next_level_actors.clear()

        for actor, next_actor in get_next_level_actors(end_next_level_actors, visited_movies,
                                                       num_of_movies_limit, num_of_actors_limit):
            actors_graph.add_edge(actor, next_actor.get('url'))
            end_queue.append(next_actor.get('url'))
        end_next_level_actors.clear()

    actors_graph.save_file(CURRENT_DIR)
    return


def get_movie_descriptions_by_actor_soup(actor_page_soup: BeautifulSoup) -> List[str]:
    """
    Function gets all movie descriptions of a given actor.

    :param actor_page_soup:
    :return: descriptions
    """
    descriptions = []
    actor_movies = get_movies_by_actor_soup(actor_page_soup)
    movie_paths = [movie.get('url') for movie in actor_movies]
    movie_urls = create_urls(movie_paths, fullcredits=False)
    movie_soups = asyncio.run(get_soups(movie_urls))
    for movie_soup in movie_soups:
        descriptions.append(_get_movie_description(movie_soup))
    return descriptions


if __name__ == '__main__':
    import time
    from itertools import combinations
    from datetime import datetime
    from pandas import DataFrame, concat

    MAX_DIST = 3
    NUM_ACTORS = 50
    NUM_MOVIES = 50

    top_paid_19 = {
        '/name/nm0425005/': 'Dwayne Johnson',
        '/name/nm1165110/': 'Chris Hemsworth',
        '/name/nm0000375/': 'Robert Downey Jr.',
        '/name/nm0474774/': 'Akshay Kumar',
        '/name/nm0000329/': 'Jackie Chan',
        '/name/nm0177896/': 'Bradley Cooper',
        '/name/nm0001191/': 'Adam Sandler',
        '/name/nm0424060/': 'Scarlett Johansson',
        '/name/nm0005527/': 'Sofia Vergara',
        '/name/nm0262635/': 'Chris Evans'
    }


    def movies_descriptions():
        start_loop = datetime.today()
        for url, name in top_paid_19.items():
            start = datetime.today()
            print(f"Starting {name} at {start.strftime('%Y-%m-%d %H:%M')}")
            actor_url = create_url(url)
            actor_soup = asyncio.run(get_soups([actor_url]))
            descs = get_movie_descriptions_by_actor_soup(actor_soup[0])
            with open(f'{name}.txt', 'w') as file:
                file.write('\n'.join(descs))
            print(f'\tTime spent: {datetime.today() - start}')
            time.sleep(2)
        print(f'ALL ACTORS TIME: {datetime.today() - start_loop}')


    def distances(actors_dict):
        data = []
        df_cols = ['actor_start_name', 'actor_start_link', 'actor_end_name', 'actor_end_link', 'distance']

        start = datetime.today()
        for pair in combinations(actors_dict, 2):
            actor_start_link, actor_end_link = pair
            actor_start_name = actors_dict.get(actor_start_link)
            actor_end_name = actors_dict.get(actor_end_link)
            print(f"Starting {actor_start_name} {actor_start_link}, {actor_end_name} {actor_end_link}")
            print(f"{datetime.today().strftime('%Y-%m-%d %H:%M')}")
            res = get_movie_distance(actor_start_link, actor_end_link, max_distance=MAX_DIST,
                                     num_of_actors_limit=NUM_ACTORS, num_of_movies_limit=NUM_MOVIES)
            print(f"Distance: {res}")
            row = {col: value for col, value in zip(df_cols, [actor_start_name, actor_start_link,
                                                              actor_end_name, actor_end_link, res])}
            data.append(row)
            print("Reverse")
            reversed_res = get_movie_distance(actor_end_link, actor_start_link, max_distance=MAX_DIST,
                                              num_of_actors_limit=NUM_ACTORS, num_of_movies_limit=NUM_MOVIES)
            print(f"Reversed distance: {reversed_res}")
            row = {col: value for col, value in zip(df_cols, [actor_end_name, actor_end_link,
                                                              actor_start_name, actor_start_link, reversed_res])}
            data.append(row)
            print('_' * 50)
            print()
        print(f'Time spent {datetime.today() - start}')

        df = DataFrame(columns=df_cols)
        df = concat([df, DataFrame(data)])
        df.to_csv(os.path.join(CURRENT_DIR, f'{start.strftime("%Y-%m-%dT%H:%M")}_distances.csv'), index=False)

        cache_files = ActorsGraph.get_sorted_by_date_files(CURRENT_DIR)
        print(f"Found {len(cache_files)} cache files. Removing..")
        for filename in cache_files[1:]:
            os.remove(os.path.join(CURRENT_DIR, filename))

    # distances(top_paid_19)
    # movies_descriptions()
