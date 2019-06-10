package org.poem.dataset

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.poem.dataset.entity.DataSetClass.{Rating, User}
class Main_Love_DataSet {

  def Main_Movie_Top(ratingsRDD: RDD[String], usersRDD: RDD[String], spark: SparkSession): Unit = {
    import spark.implicits._
    println("\n功能二：通过createDataset实现某部电影观看者中不同年龄和不同性别有多少人")
    //然后把我么的每一条数据变成row
    val userRDDRows = usersRDD
      .map(
        line =>{
          val newLine = line.split("::")
          User(newLine(0), newLine(1), newLine(2), newLine(3), newLine(4))
        }
      )
    val usersDataSet = spark.createDataset[User](userRDDRows)
    println("user data set is :")
    usersDataSet.show(10)



    val ratingsForDSRDD = ratingsRDD.map(
      _.split("::")
    ).map(
      line =>
       Rating(line(0).trim, line(1).trim , line(2).trim, line(3).trim)
    )
    val ratingsDataSet = spark.createDataset[Rating](ratingsForDSRDD)
    println("ratings data set  is :")
    ratingsDataSet.show(10)
    ratingsDataSet.filter(s"MovieID = 1193")
      .join(usersDataSet, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender","Age").count().orderBy($"Gender".desc , $"Age".desc ).show()

  }
}
