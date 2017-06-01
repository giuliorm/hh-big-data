package ru.hh.bigdata
/**
  * Created by JuriaSan on 01.06.2017.
  */

class Vacancy(name: String, requirements: List[String], skills: List[String], salary: Option[Salary]) {

  lazy val stringRepresentation: String =  wordsVector.mkString(" ")
  lazy val wordsVector: List[String] = name :: requirements ::: skills

  override def toString(): String = {
    stringRepresentation
  }
}
