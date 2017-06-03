package ru.hh.bigdata

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by JuriaSan on 03.06.2017.
  */
object StringUtil {

  def containsOnlyLetters(word: String): Boolean = {
    word.matches(onlyLetters)
  }

  val alphabets: List[String] = List("абвгдеёжзийклмнопрстуфхцчшщъыьэюя", "a-z")

  val charactersToOmit: String = "[^" +
    alphabets.flatMap(i => List(i.toLowerCase, i.toUpperCase()))
      .mkString("") + "1-9\\s]"

  val onlyLetters: String = "[" +
    alphabets.flatMap(i => List(i.toLowerCase, i.toUpperCase()))
      .mkString("") + "]+"

  def clearString(s: String): String = {
    s.trim()
      .toLowerCase()
      .replaceAll(charactersToOmit, "")
  }

}
