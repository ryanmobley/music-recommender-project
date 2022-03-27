/*
Preprocessing for Music Recommender System

Input File: Dataset/train.csv
Output File: ProcessedData/users_songs.csv

Input Columns:
- msno: String
- song_id: String
- target: Int

Output Columns
- user_id: Int
- song_id: Int
- target: Int

A Map is created for each the msno and song_id columns, mapping distinct values
to integers. The raw data is mapped so that the msno and song_id values are
converted to integers, but the data represented is still the same.
The processed data file is also much smaller and therefore easier to work with.

This will need more than the default memory available.
> spark-shell --driver-memory 2g

Run in spark-shell:
> :load preprocessor.scala
*/


import java.io._
import org.apache.spark.sql._

def writeToCSV(d: DataFrame, outFolder: String, outFile: String): Unit = {
    val dirName = outFolder + "/" + outFile

    // Write the CSV in the directory
    d.coalesce(1).write.option("header", true).csv(dirName)

    // Find the actual CSV file, rename it, and delete the rest
    val dir = new File(dirName)
    val f = dir.listFiles.filter(_.toString.endsWith(".csv"))(0)
    f.renameTo(new File(dirName + ".csv"))
    dir.listFiles.foreach(_.delete)
    dir.delete
}

val outFolder = "ProcessedData"

val rawData = spark.read.option("header", true).csv("Dataset/train.csv")
val df = rawData.select("msno", "song_id", "target")

val userIDArray = df.select("msno").distinct().collect().map(_.getString(0))
val userIDMap = userIDArray.zipWithIndex.toMap
val songIDArray = df.select("song_id").distinct().collect().map(_.getString(0))
val songIDMap = songIDArray.zipWithIndex.toMap

// This is very slow
val data = df.map(r => {
    val userID = userIDMap(r.getString(0))
    val songID = songIDMap(r.getString(1))
    val target = r.getString(2).toInt
    (userID, songID, target)
}).toDF("user_id", "song_id", "target")

writeToCSV(data, outFolder, "users_songs")
