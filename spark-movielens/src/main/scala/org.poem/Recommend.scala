package org.poem

import java.util.logging.{Level, Logger}

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

class Recommend {

  def setLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getGlobal.setLevel(Level.OFF)
  }

  def recommend(modle:MatrixFactorizationModel,movieTitle:Map[Int, String]): Unit ={
    setLogger
    var choose = ""
    while (choose != "3"){
      choose = scala.io.StdIn.readLine()
      if (choose == "1"){
        println("请输入用户id:")
        val inputUserId = scala.io.StdIn.readLine()
        //RecommendMovies(modle, movieTitle, inputUserId.toInt)
      }else if (choose == "2"){
        println("请输入电影的id:")
        val inputMovieId = scala.io.StdIn.readLine()
        //RecommonUsers(modle, movieTitle, inputMovieId.toInt)
      }
    }
  }
}
