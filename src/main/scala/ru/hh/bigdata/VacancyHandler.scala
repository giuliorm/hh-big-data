package ru.hh.bigdata

import com.mongodb.{BasicDBList, BasicDBObject}
import org.apache.lucene.analysis.CharArraySet
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import ru.hh.bigdata.StringUtil
import ru.hh.bigdata.domain.Salary
import ru.hh.bigdata.util.stemmer.RussianStemmer

/**
  * Created by JuriaSan on 03.06.2017.
  */
object VacancyHandler {
  def retrieveName(m: BSONObject): List[String] =
    List(StringUtil.clearString(m.get("name").asInstanceOf[String]))

  def retrieveSkills(m: BSONObject): List[String] = {
    m.get("key_skills")
      .asInstanceOf[BasicDBList]
      .toArray()
      .map(x => x.asInstanceOf[BasicDBObject])
      .map(dbObject => dbObject.get("name").asInstanceOf[String])
      .flatMap(str => str.split(","))
      .map(s => StringUtil.clearString(s))
      .toList
  }



  def retrieveSalary(m: BSONObject): Option[Salary] = {
    val salaryObj = m.get("salary").asInstanceOf[BasicDBObject]
    if (salaryObj != null) {
      val salaryFrom = salaryObj.get("from").asInstanceOf[Int].toDouble
      val salaryTo = salaryObj.get("to").asInstanceOf[Int].toDouble
      val currency = salaryObj.get("currency").asInstanceOf[String]
      Some(new Salary(if (salaryFrom == 0.0) None else Some(salaryFrom),
        if (salaryTo == 0.0) None else Some(salaryTo),
        currency))
    } else None
  }

  def retrieveDescription(m: BSONObject): List[String] = {
    m.get("description")
      .asInstanceOf[String]
      .split("""<(?!\/?a(?=>|\s.*>))\/?.*?>""")
      .flatMap(req => req.split(","))
      .filter(x => StringUtil.clearString(x) != "")
      .map(x => StringUtil.clearString(x))
      .toList
  }

  def retrieveRequirements(words: List[String]): List[String] = {
    val startKey = "требования"
    val stopKeys = List("обязанности", "условия")

    val reqIndex = words.indexOf(startKey)
    if (reqIndex > -1) {
      val nwords = words.dropWhile(word => word != startKey).tail
      val stopInd = stopKeys
        .map(key => nwords.indexOf(key))
        .filter(ind => ind > -1)
      if (stopInd.isEmpty)
        nwords
      else {
        val stopWord = nwords(stopInd.min)
        nwords.reverse.dropWhile(word => word != stopWord).tail
      }
    }
    else
      words
  }

  def vacancyAsMap(m: BSONObject): Map[String, List[String]] = {
    val name = retrieveName(m)
    val skills = retrieveSkills(m)
    val requirements = retrieveRequirements(retrieveDescription(m))
    val salary = retrieveSalary(m)
    Map("name" -> name, "requirements" -> requirements, "skills" -> skills)
  }

  lazy val stopwordSet: CharArraySet = new RussianAnalyzer().getStopwordSet

  def stemAndClearVector(vec: List[String]): List[String] = {
    //val russianStopWords = new RussianAnalyzer().getStopwordSet

    vec.map(word => StringUtil.clearString(word))
      .filter(word => word != "" && !stopwordSet.contains(word))
      .map(word => RussianStemmer.stem(word))
      .filter(word => StringUtil.containsOnlyLetters(word) && word.length > 2)
  }



  def makeWordVector(vacancy: Map[String, List[String]]): List[String] = {
    (vacancy("name") :::
      vacancy("requirements").flatMap(r => r.split(" ")) :::
      vacancy("skills").flatMap(s => s.split(" ")))
      .filterNot(s => s == null || s == "")
  }

  def bagOfWords(vacancies: RDD[List[String]]): List[String] = {
    vacancies.fold(Nil)((v1, v2) => v1 ::: v2).distinct
  }

  def vectorizeVacanciesRDD(vacancies: RDD[List[String]], bagOfWords: List[String]):
  RDD[List[Tuple2[String, Int]]] = {
    val vacFeatures = vacancies.map(vac => {
      bagOfWords
        .map(wordFromBag => (wordFromBag, vac.count(word => word == wordFromBag)))
    })
    vacFeatures
  }


  def stemAndClearRDD(vacanciesRaw: RDD[Map[String, List[String]]]):
  RDD[List[String]] = {
    val vacancies = vacanciesRaw.map(v =>  {
      val wordVec = VacancyHandler.makeWordVector(v)
      VacancyHandler.stemAndClearVector(wordVec)
    })
    vacancies
  }

}

