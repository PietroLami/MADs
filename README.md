# MADs

**MADs - Movie Analyzer Datasets - Beta version**

## Project

Project developed for [Scalable and Cloud programming course at Unibo](https://www.unibo.it/it/didattica/insegnamenti/insegnamento/2019/412732).

This work is a collaboration between Teresa Signati and Pietro Lami.

## Structure
MADs project is divided in two parts:
- MADsCore, which can be executed either locally or on aws
- MADsPlotter, which is a util that generates plot for a graphical visualization of MADs Movie Time Analysis results

```
MADs
├── MADsCore
└── MADsPlotter
```

Edit localPath and awsPath (bucket name) in MADsMain and in MADsPlot 
with your directory and bucket containing the required datasets.

## MADsCore
### Execution


To compile the project and create the jar file

```
cd MADsCore
sbt assembly
```

To execute it locally

```
spark-submit MADs.jar local
```

To execute it on aws

```
spark-submit MADs.jar aws
```

### MADs usage details

MADs allows the analysis of movie datasets. The ones that we have taken into account during our project development are:

- [MovieLens](https://grouplens.org/datasets/movielens/latest/)
- [Stanford dataset](https://snap.stanford.edu/data/web-Movies.html)

We have implemented functions that allow to import these two datasets, but other functions can be added to import other datasets with the proper schema and analyze other ones so well.

The operations implemented by MADs use two different type of representation: DataFrame and RDD.

We decided to implement them for both the data types to compare their performances.

### Main commands

For all the main commands we'll use the following notation:

- dataset = L | N

  where L corresponds to the dataset of movieLens and S corresponds to the Stanford one

- repr = DF | RDD

  the representation type, where DF stands for DataFrame and RDD stands for Resilient Distributed Dataset

#### Movie Recommendation

Computes user recommended movies, considering its previous visualizations and ratings.

```
> recommend dataset repr userID
```

#### Movie Ranking

Computes user recommended movies, considering its previous visualizations and ratings.

```
> rank dataset repr
```

#### Movie Similarity

Computes movies similar to the given one(s), considering the ratings of all the users.

The similarity can be computed or as union of all the results for each movie given as input or as their intersection. In both cases, for results in the intersection of the results for some of the input movies.

##### Similarity intersection

Computes the results similar to all the input movies.

```
> similarI dataset repr movieID(s)
```

##### Similarity union

Computes the results similar to at least one of the input movies.

```
> similarU dataset repr movieID(s)
```

#### Movie Time Analysis

Creates a csv about the evolution of rating average in time, with columns 
```
| movieId | year/month | avgRating |
```
The generated csv can be passed to MADsPlotter to produce a plot.

There are two type of analysis: analysis in an interval of years and analysis in a given year by month.

##### Analysis in an interval

```
> evolutionY dataset repr yearBegin yearEnd movieID(s)
```

**Note:** to not provide the year of begin (end), replace them with a '-'. In this case MADs will consider the first(last) year recorded into the dataset.

##### Analysis in a year

```
> evolutionM dataset repr year movieID(s)
```

#### User Helpfulness

Computes the helpfulness of a given user. If an user has an helpfulness score that is lower than the average (of the other users that gave the same rating to the same movie), its score is incremented by adding the average score to the initial score and dividing by 2. The final helpfulness score is the average of the user helpfulness for the evaluated movies.

```
> helpfulness dataset repr userID
```

**Note:** operation provided only for Stanford dataset, because movieLens one lacks of the helpfulness score.

#### Note

Each one of the previous operations that give movieIds as result, can show the correspondents movie titles, if they are present in the dataset. This is what happens with movieLens dataset. In this case, after the query, you'll be asked if you want to print this correspondence.

### Additional commands

##### Print movie title

```
> title dataset movieID
```

**Note:** operation provided only for movieLens, because the other dataset doesn't provide movie titles.

##### Set the minimum number of movies to compare two users

```
> set minMovies number
```

##### Set the minimum number of users to compare two movies

```
> set minUsers number
```

##### Set the number of neighbors to consider in recommendation prediction and similarity

```
> set neighbors number
```

##### Set weight for prior estimate to compute Bayesian ranking

```
> set minVotes number
```

##### Set the maximum number of displayed results

```
> set results number
```

##### Set the similarity method to be used in recommendation prediction and similarity

```
> set simMethod method
```

where simMethod = COSINE | PEARSON

**Note:** operation provided only for DataFrames. RDD at the moment implement only cosine comparison.

##### Reprint the whole usage information

```
> help
```

##### Exit from MADs

```
> exit
```

## MADs Plotter

Creates a plot starting from a csv generated by MADs Movie Time Analysis.

To compile MADs Plotter
```
cd MADs Plotter
sbt assembly
```

To generate a plot, if the csv is in aws s3 bucket, run
```
spark-submit MADsPlotter.jar aws outputOfMovieTimeAnalysis.csv
```
instead, if the csv is in the local path, run
```
spark-submit MADsPlotter.jar local outputOfMovieTimeAnalysis.csv
```

© Pietro Lami, Teresa Signati.
