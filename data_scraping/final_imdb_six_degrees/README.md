# Detailed description

This task is not so easy, so we will solve the problem step by step. This week you are required to implement two auxiliary functions, that will be used for obtaining data the following week. Two functions are required:

* we want to be able to get a list of movies that a current actor played in
* we want to be able to get a list of actors, that played in the current movie

Get [this](https://github.com/magnitofonov/hse-coursera-data-scraping/tree/master/week10/project_templates) code template. This week, implement functions `get_actors_by_movie_soup` and `get_movies_by_actor_soup`. There are some other functions templates also, but leave them as they are for a while. We will implement them in the following weeks. Let's talk more precisely, what these functions are about and how they should behave:

`get_actors_by_movie_soup(cast_page_soup, num_of_actors_limit)`

* This function takes a beautifulsoup soup object (`cast_page_soup`) of a page for the cast & crew for the current film.
* The function should return a list of all actors that played in the movie. An actor should be defined by such a pair: `(name_of_actor, url_to_actor_page)`. So, the output of the function is expected to be the list of such pairs.
* The function should be able to take an optional argument `num_of_actors`. This argument allows us to limit the output. If we set the argument equal to, say, 10, then the function should return first 10 actors listed on the cast page, and no more than that. If we set the argument equal to `None`, then the function should return all the actors. If there are fewer actors than the argument, the function should work and return all actors that are there.

`get_movies_by_actor_soup(actor_page_soup, num_of_movies_limit)`

* This functions takes a beautifulsoup soup object (`actor_page_soup`) of a page for the current actor.
* The function should return a list of all movies that the actor played in. A movie should be defined by such a pair: `(name_of_movie, url_to_movie_page)`. So, the output of the function is expected to be the list of such pairs.
* The function should be able to take an optional argument `num_of_movies_limit`. This argument allows us to limit the output. If we set the argument equal to, say, 10, then the function should return 10 latest movies that the actor played in, and no more than that. If we set the argument equal to None, then the function should return all the movies. If there are fewer movies than the argument, the function should work and return all movies that are there.
* The function should return only those movies, that have already been released.
* Sometimes actors could be producers, or even directors, or something else. The function should return only those movies, where the actor did an acting job. So, we should omit all the movies, where the actor has not actually played a role.
* The function should return only full feature movies. So, it should omit other types of videos, which are marked on imdb like that: TV Series, Short, Video Game, Video short, Video, TV Movie, TV Mini-Series, TV Series short and TV Special.


You may want to define supplementary functions, to make your code better. So, feel free to implement any additional functions you might need, but place them in [`imdb_helper_functions.py`](https://github.com/magnitofonov/hse-coursera-data-scraping/tree/master/week10/project_templates) file (we want to keep additional functions separated from essential function, so it would be easier to check your code). You can import these functions to your `imdb_code.py` script or to jupyter notebook, and use them there.

In addition to imdb_code.py and imdb_helper_functions.py the archive should contain a file `imdb_week_10.ipynb`, which will show the work of your functions and outputs for given arguments.

Also, when you are done with the functions, think about the following actions. Next week we will be implementing a function, that takes two actors as an input, and returns the *movie distance* between actors. Try to come up with an algorithm, that would solve such a problem.

<br>
