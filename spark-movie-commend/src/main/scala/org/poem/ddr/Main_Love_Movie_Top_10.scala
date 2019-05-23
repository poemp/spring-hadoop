package org.poem.ddr

import org.apache.spark.rdd.RDD

class Main_Love_Movie_Top_10 {
  /**
    *
    * @param ratingsRDD UserID::MovieID::Rating::Timestamp
    * @param moviesRDD  MovieID::Title::Genres
    * @param usersRDD   UserID::Gender::Age::Occupation::Zip-code
    */
  def Main_Movie_Top(ratingsRDD: RDD[String], moviesRDD: RDD[String], usersRDD: RDD[String]): Unit = {
    //(UserID, Gender)
    val userGender = usersRDD.map(_.split("::")).map(x => (x(0), x(1)))
    //UserID::(MovieID::Rating::Timestamp)
    val genderRating = ratingsRDD.map(_.split("::")).map(x => (x(0), (x(1), x(2), x(3)))).join(userGender).cache()

    val maleFilteredRatings = genderRating.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    val femaleFilterRatings = genderRating.filter(x => x._2._2.equals("F")).map(x => x._2._1)

    val movieInfo = moviesRDD.map(_.split("::"))
      .map(
        x =>
          (x(0), x(1))
      ).cache()
    println("\n所有电影中最受男性喜爱的电影 Top10:")

    maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
      .join(movieInfo).map(em => (em._2._1, em._2._2))
      .sortByKey(false).take(10)
      .foreach(record => println(record._2+ "评分为:" + record._1))

    println("\n所有电影中最受女性喜爱的电影 Top10:")
    femaleFilterRatings.map(x => (x._2, (x._3.toDouble,1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
      .join(movieInfo).map(em => (em._2._1, em._2._2))
      .sortByKey(false).take(10)
      .foreach(record => println(record._2+ "评分为:" + record._1))
  }
}
