package org.poem

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.poem.dataframe.Main_Love_DataFrame
import org.poem.dataset.Main_Love_DataSet
import org.poem.ddr.{Main_Love_Movie_Top_10, Print_Movie_Top_10}
import org.poem.sort.Second_Sort_Key

object RDD_Movie_Users_Analyzer {

  /**
    * 新排序方式
    *
    * @param ratingsRDD
    */
  def New_Sort(ratingsRDD: RDD[String]): Unit = {
    val pairWithSortkey = ratingsRDD.map(line => {
      val splited = line.split("::")
      (new Second_Sort_Key(splited(3).toDouble, splited(2).toDouble), line)
    })
    //直接调用sortByKey, 此时会按照实现的 compare 方法排序
    val sorted = pairWithSortkey.sortByKey(false)

    println("\n第二次排序")
    val sorted_result = sorted.map(sortLine => sortLine._2)
    sorted_result.take(10).foreach(println)
  }

  /**
    * 准备数据
    */
  def Prepare_Data(): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark-Movie-Commend")
    /**
      * Spark 2.2 引入 SparkSession 封装 SparkContext 和SQLContext ，
      * 并且会在build 的getOrCreate 方法判断是否符合要求的 SparkSession 存在，
      * 有则使用， 没有则进行创建
      */
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 获取SparkSession的SparkContext
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val path = "/root/workspace/recommend/data/ml-1m/"
    println("start read data from hdfs ")
    //UserID::Gender::Age::Occupation::Zip-code
    println("===================== start read users data =========================")
    val usersRDD: RDD[String] = sc.textFile(path + "users.dat")
    println("users counts :" + usersRDD.count())
    //UserID::MovieID::Rating::Timestamp
    println("===================== start read ratings data =========================")
    val ratingsRDD: RDD[String] = sc.textFile(path + "ratings.dat")
    println("ratingsRDD counts :" + ratingsRDD.count())
    //MovieID::Title::Genres
    println("===================== start read movies data =========================")
    val moviesRDD: RDD[String] = sc.textFile(path + "movies.dat")
    println("moviesRDD counts :" + moviesRDD.count())

    //所有电影中平均得分最高(口碑最好)的电影
    new Print_Movie_Top_10().Print_Top_10(ratingsRDD, moviesRDD)
    new Main_Love_Movie_Top_10().Main_Movie_Top(ratingsRDD, moviesRDD, usersRDD)
    new Main_Love_DataFrame().Main_Movie_Top(ratingsRDD, moviesRDD, usersRDD, spark)
    new Main_Love_DataFrame().Main_Movie_DataFrame_Table(ratingsRDD, moviesRDD, usersRDD, spark)
    new Main_Love_DataSet().Main_Movie_Top(ratingsRDD,usersRDD, spark)
    New_Sort(ratingsRDD)
    // 关闭sparkSession
    spark.close()
  }


  /**
    * main
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Prepare_Data()
  }
}
