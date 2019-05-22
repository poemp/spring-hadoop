package org.poem

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

class Recommend {

  def setLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getGlobal.setLevel(Level.OFF)
  }

  /**
    * 推荐代码
    *
    * @param modle
    * @param movieTitle
    */
  def recommend(modle: MatrixFactorizationModel, movieTitle: Map[Int, String]): Unit = {
    setLogger
    var choose = ""
    while (choose != "3") {
      choose = scala.io.StdIn.readLine()
      if (choose == "1") {
        println("请输入用户id:")
        val inputUserId = scala.io.StdIn.readLine()
        //RecommendMovies(modle, movieTitle, inputUserId.toInt)
      } else if (choose == "2") {
        println("请输入电影的id:")
        val inputMovieId = scala.io.StdIn.readLine()
        //RecommonUsers(modle, movieTitle, inputMovieId.toInt)
      }
    }
  }

  /**
    * 准备数据
    *
    * @return
    */
  def PrepareData(): (RDD[Rating], Map[Int, String]) = {
    // ---------------- 1. 创建用户评分数据 --------------
    val sc = new SparkContext(new SparkConf().setAppName("wordCount").setMaster("local[4]"))
    println("开始读取文件")
    //UserID::Gender::Age::Occupation::Zip-code
    val rowUserData = sc.textFile("/root/workspace/recommend/data/ml-1m/users.dat")
    //UserID::MovieID::Rating::Timestamp
    val ratingsData = sc.textFile("/root/workspace/recommend/data/ml-1m/ratings.dat")

    //训练出来了
    val rawRatings = ratingsData.map(_.split("::").take(3)).map { case Array(user, movice, rating) => Rating(user.toInt, movice.toInt, rating.toDouble) }
    println("共计:" + rawRatings.count().toString + " 条 ratings")
    // ---------------- 2. 创建电影id 和名称的映射 --------------
    println("开始读取电影数据中 .... ")
    //MovieID::Title::Genres
    val moviesData = sc.textFile("/root/workspace/recommend/data/ml-1m/movies.dat")
    val moviesRDD = moviesData.map(_.split("::").take(2))
      .map(array => (array(0).toInt, array(1))).collect().toMap
    // ---------------- 3. 显示记录数量 --------------
    val numRatings = rawRatings.count()
    val numUsers = rawRatings.map(_.user).distinct().count()
    val numMovies = rawRatings.map(_.product).distinct().count()
    println(" 共计：" + numRatings.toString + " user: " + numUsers.toString + "movies:" + numMovies)
    (rawRatings, moviesRDD)
  }


  /**
    * 推荐电影
    *
    * @param modle
    * @param movieTitle
    * @param inputUserId
    */
  def RecommendMovies(modle: MatrixFactorizationModel, movieTitle: Map[Int, String], inputUserId: Int) = {
    val RecommendMovie = modle.recommendUsersForProducts(inputUserId)
    println("针对用户id：" + inputUserId + "推荐以下电影")
    RecommendMovie.foreach(
      r =>
        println( movieTitle(r._1) + " 评分：" + r._2)
    )
  }
}
