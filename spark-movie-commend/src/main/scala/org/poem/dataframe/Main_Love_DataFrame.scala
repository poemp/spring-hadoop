package org.poem.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Main_Love_DataFrame {

  /**
    * 创建user data frame
    *
    * @param usersRDD
    * @param spark
    * @return
    */
  def Create_User_DataFrame(usersRDD: RDD[String], spark: SparkSession): DataFrame = {
    //首先把 users 数据格式化， 在DDR 的基础上增加数据的元数据信息
    val schemaForUsers = StructType(
      "UserID::Gender::Age::Occupation::Zip-code".split("::")
        .map(_.trim)
        .map(
          column =>
            StructField(column, StringType, true)
        )
    )

    //然后把我么的每一条数据变成row
    val userRDDRows = usersRDD
      .map(
        line =>{
          val newLine = line.split("::")
          Row(newLine(0), newLine(1), newLine(2), newLine(3), newLine(4))
        }
      )

    // 使用SparkSession 的 CreateDataFrame 方法 ， 结合Row 和StrunctType的元数据信息
    // 基于 RDD 创建DataFrame ， 这时 RDD 就有了元数据信息的描述
    val d: DataFrame = spark.createDataFrame(userRDDRows, schemaForUsers)
    println("default.user\n")
    //d.show(10)
    //d.write.mode(SaveMode.Overwrite).saveAsTable("default.user")
    d
  }

  /**
    * 创建rating 的 data frame
    *
    * @param ratingsRDD
    * @param spark
    */
  def Create_Ratings_DataFrame(ratingsRDD: RDD[String], spark: SparkSession): DataFrame = {
    // 也可以对StructType 调用add方法对不同的StructTypeField 赋予不同的类型
    val schemaForratings = StructType(
      "UserID::MovieID::Rating::Timestamp".split("::")
        .map(_.trim)
        .map(
          column =>
            StructField(column, StringType, true))
    )


    //然后把我么的每一条数据变成row
    val ratingRDDRows = ratingsRDD
      .map(
        line =>{
          val newLine = line.split("::")
          Row(newLine(0), newLine(1), newLine(2), newLine(3))
        }
      )

    // 使用SparkSession 的 CreateDataFrame 方法 ， 结合Row 和StrunctType的元数据信息
    // 基于 RDD 创建DataFrame ， 这时 RDD 就有了元数据信息的描述
    val d: DataFrame = spark.createDataFrame(ratingRDDRows, schemaForratings)
    println("default.ratings:\n")
    //d.show(10)
    //d.write.mode(SaveMode.Overwrite).saveAsTable("default.ratings")
    d
  }

  /**
    * 创建 movie data frame
    *
    * @param moviesRDD
    * @param spark
    */
  def Create_Movies_DataFrame(moviesRDD: RDD[String], spark: SparkSession): DataFrame = {
    // 也可以对StructType 调用add方法对不同的StructTypeField 赋予不同的类型
    val schemaForratings = StructType(
      "MovieID::Title::Genres".split("::")
        .map(_.trim)
        .map(
          column =>
            StructField(column, StringType, true))
    )

    val ratingRDDRows = moviesRDD
      .map(
        line =>{
          val newLine = line.split("::")
          Row(newLine(0), newLine(1), newLine(2))
        }
      )
    val d: DataFrame = spark.createDataFrame(ratingRDDRows, schemaForratings)
    println("default.movie\n")
    //d.show(10)
    //d.write.mode(SaveMode.Overwrite).saveAsTable("default.movie")
    d
  }

  /**
    *
    * @param ratingsRDD
    * @param moviesRDD
    * @param usersRDD
    */
  def Main_Movie_Top(ratingsRDD: RDD[String], moviesRDD: RDD[String], usersRDD: RDD[String], spark: SparkSession): Unit = {
    println("\n功能一：通过dataFrame 实现某部电影观看者中男性和女性不同年龄人数")
    //UserID::Gender::Age::Occupation::Zip-code
    val userDataFrame: DataFrame = Create_User_DataFrame(usersRDD, spark)
    //MovieID::Title::Genres
    val moviesDataFrame: DataFrame = Create_Movies_DataFrame(moviesRDD, spark)
    //UserID::MovieID::Rating::Timestamp
    val ratingsDataFrame: DataFrame = Create_Ratings_DataFrame(ratingsRDD, spark)

    // 直接通过列明 MovieId 为1193 过滤电影
    ratingsDataFrame.filter(" MovieID='1193' ")
      //join 的时候 指定基于UserID进行Join， 对于原生的RDD操作而言更加方便快捷
      .join(userDataFrame, "UserID")
      //使用元数据信息中的Gender 和Age 进行数据的帅选
      .select("Gender", "Age")
      //直接通过 元数据信息中的Gender 和Age 进行数据进行 GroupBy 操作
      .groupBy("Gender", "Age")
      // 基于 groupBy 分组信息进行count统计操作，并显示出分组统计后的前10条信息
      .count().show(10)
  }

  /**
    * 注册成表
    *
    * @param ratingsRDD
    * @param moviesRDD
    * @param usersRDD
    * @param spark
    */
  def Main_Movie_DataFrame_Table(ratingsRDD: RDD[String], moviesRDD: RDD[String], usersRDD: RDD[String], spark: SparkSession): Unit = {
    println("\n功能一：通过LocalTempView实现某部电影观看者中不同年龄和不同性别有多少人")

    //UserID::MovieID::Rating::Timestamp
    val ratingsDataFrame: DataFrame = Create_Ratings_DataFrame(ratingsRDD, spark)
    ratingsDataFrame.createTempView("ratings")

    //UserID::Gender::Age::Occupation::Zip-code
    val userDataFrame: DataFrame = Create_User_DataFrame(usersRDD, spark)
    userDataFrame.createTempView("users")


    val sql: String = s"SELECT users.Gender, users.Age , count(1) as counts from users join ratings on users.UserID = ratings.UserID group by  users.Gender, users.Age ORDER BY counts asc  "
    val sqlDataFrame: DataFrame = spark.sql(sql)

    sqlDataFrame.show(10)
  }
}
