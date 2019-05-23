package org.poem.ddr

import org.apache.spark.rdd.RDD

class Print_Movie_Top_10 {
  /**
    * 所有电影中平均得分最高(口碑最好)的电影
    * @param ratingsRDD
    * @param moviesRDD
    */
  def Print_Top_10(ratingsRDD: RDD[String] ,moviesRDD: RDD[String] ): Unit ={
    /** 具体的处理逻辑 **/
    println("\n所有电影中平均得分最高(口碑最好)的电影:")
    val movieInfo = moviesRDD.map(_.split("::"))
      .map(
        x =>
          (x(0), x(1))
      ).cache()
    val ratings = ratingsRDD.map(_.split("::"))
      .map(
        x =>
          (x(0), x(1), x(2))
      ).cache()
    val movieAndRatings = ratings.map(
      x =>
        (x._2, (x._3.toDouble, 1))
    ).reduceByKey(
      (x, y)
      =>
        (x._1.toDouble + y._1.toDouble, x._2 + y._2)
    )
    val avg_ratings = movieAndRatings
      .map(
        x =>
          (x._1,x._2._1 /x._2._2)
      )
    avg_ratings.join(movieInfo).map(
      item =>
        (item._2._1, item._2._2)
    ).sortByKey( false).take(10).foreach(r => println(r._2 +"评分为:" + r._1))
  }
}
