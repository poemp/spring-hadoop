package org.poem.dataset.entity

object DataSetClass {
  case class  User(UserID:String , Gender:String, Age:String, Occupation:String , ZipCode:String)
  case class  Rating(UserID:String, MovieID:String, Rating:String, Timestamp:String)
}
