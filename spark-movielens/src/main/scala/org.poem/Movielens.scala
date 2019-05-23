package org.poem

import java.util.logging.{Level, Logger}

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object Movielens {


  def setLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getGlobal.setLevel(Level.OFF)
  }

  def movieMain(): Unit = {
    setLogger
    System.setProperty("spark.ui.showConsoleProgress", "false")
    println(" start Spark Application")
    val sc = new SparkContext(new SparkConf().setAppName("wordCount").setMaster("local[4]"))

    println("开始读取文件")

    //UserID::Gender::Age::Occupation::Zip-code
    val rowUserData = sc.textFile("/root/workspace/recommend/data/ml-1m/users.dat")

    //UserID::MovieID::Rating::Timestamp
    val ratingsData = sc.textFile("/root/workspace/recommend/data/ml-1m/ratings.dat")

    //MovieID::Title::Genres
    val moviesData = sc.textFile("/root/workspace/recommend/data/ml-1m/movies.dat")

    //训练出来了
    val rawRatings = ratingsData.map(_.split("::").take(3)).map { case Array(user, movice, rating) => Rating(user.toInt, movice.toInt, rating.toDouble) }

    val model: MatrixFactorizationModel = ALS.train(rawRatings, 10, 10, 0.1D)


    //显示出电影名称
    val movieTitle = moviesData
      .map(_.split("::").take(2))
      .map(array => (array(0).toInt, array(1)))
      .collectAsMap()

    model.productFeatures.map(
      o =>
        (o._1, movieTitle(o._1))

    ).foreach(o => println(o._2))
  }

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val r = new Recommend
    r.setLogger
    val result = r.PrepareData()
    val model: MatrixFactorizationModel = ALS.train(result._1, 10, 10, 0.1D)
    r.recommend(model,result._2)
  }
}
