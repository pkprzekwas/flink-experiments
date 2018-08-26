# Movie ranking implementation (Flink)

This package consists of example implementation of movie ranking based on The Movies Dataset from Kaggle. Movies are ranked based on IMDB weighted score formula.

## IMDB ranking formula.

```
(v ÷ (v+m)) × R + (m ÷ (v+m)) × C
```

- R = average for the movie (mean) = (Rating)
- v = number of votes for the movie = (votes)
- m = minimum votes required to be listed in the Top 250 (currently 25000)
- C = the mean vote across the whole report (currently 7.0)

## Dataset

Link: https://www.kaggle.com/rounakbanik/the-movies-dataset

#### Context

These files contain metadata for all 45,000 movies listed in the Full MovieLens Dataset. The dataset consists of movies released on or before July 2017. Data points include cast, crew, plot keywords, budget, revenue, posters, release dates, languages, production companies, countries, TMDB vote counts and vote averages.

This dataset also has files containing 26 million ratings from 270,000 users for all 45,000 movies. Ratings are on a scale of 1-5 and have been obtained from the official GroupLens website.

