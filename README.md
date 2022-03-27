# Music Recommender System
#### Big Data Analytics Group 1

### Folders
Create two folders in this directory: `Dataset` and `ProcessedData`

### Raw Data
You need [this dataset](https://www.kaggle.com/competitions/kkbox-music-recommendation-challenge/data).
Move it to the `Dataset` folder in this directory, extract it, and extract the necessary archives:
* train.csv.7z

### Running the Program
Run the preprocessor first. This will generate `ProcessedData/users_songs.csv`, needed for the recommender.

    spark-shell --driver-memory 2g -I preprocessor.scala

Run the recommender next.

    spark-shell -I recommender.scala
