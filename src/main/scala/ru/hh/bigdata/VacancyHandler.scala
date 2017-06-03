package ru.hh.bigdata
import scala.reflect.runtime.universe.TypeTag
import com.mongodb.{BasicDBList, BasicDBObject}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object VacancyHandler {

  //val charsToRemove: Set[Char] = Set('\"', '[', ']' ,''', '(', ')', '.', ',', '-', ':', ';', '&', '?', '!')

  val alphabets = List("абвгдеёжзийклмнопрстуфхцчшщъыьэюя", "a-z")
  val charactersToOmit: String = "[^" + alphabets.flatMap(i => List(i.toLowerCase, i.toUpperCase())).mkString("") + "1-9\\s]"

  def clearString(s: String): String = {
    s.trim()
     .toLowerCase()
     .replaceAll(charactersToOmit, "")
  }

  def retrieveName(m: BSONObject): List[String] =
    List(clearString(m.get("name").asInstanceOf[String]))


  def retrieveSkills(m: BSONObject): List[String] = {
    m.get("key_skills")
      .asInstanceOf[BasicDBList]
      .toArray()
      .map(x => x.asInstanceOf[BasicDBObject])
      .map(dbObject => dbObject.get("name").asInstanceOf[String])
      .flatMap(str => str.split(","))
      .map(s => clearString(s))
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
      .filter(x => clearString(x) != "")
      .map(x => clearString(x))
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

  def vacancyAsObject(m: BSONObject): Map[String, List[String]] = {
    val name = retrieveName(m)
    val skills = retrieveSkills(m)
    val requirements = retrieveRequirements(retrieveDescription(m))
    val salary = retrieveSalary(m)
    Map("name" -> name, "requirements" -> requirements, "skills" -> skills)
  }

  def vacanciesAsFeatureVectors(vacancies: RDD[Vacancy]) = {

  }

  def makeWordVector(vacancy: Map[String, List[String]]): List[String] = {
    (vacancy("name") :::
      vacancy("requirements").flatMap(r => r.split(" ")) :::
      vacancy("skills").flatMap(s => s.split(" "))).filterNot(s => s == null || s == "")
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]","Extract words ")
    val ss = new SQLContext(sc)
  //  val session = SparkSession.builder().master("local").getOrCreate()
    val config = new Configuration()
    val databaseName = "hh-crawler"
    val collectionName = "vacancies-test"
    config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/" + databaseName + "." + collectionName)
    config.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat")

    // Read Mongo database
    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])

    // Convert BSON to JSON
   // Array[(Object, org.bson.BSONObject)] --> Array[org.bson.BSONObject]]
    val count = mongoRDD.count()
    if( count != 0) {
      val vacancies = mongoRDD.map(x => vacancyAsObject(x._2))

      val vacanciesWords = vacancies.map(v => makeWordVector(v))
      val vacanciesVectors = vacanciesWords.map(vac => {
        val v = vac.map(word => (word, vac.indexOf(word)))
        v
      })

     // vacanciesVectors.foreach(v => {
     //   val df = ss.createDataFrame(v).toDF("word", "id")
     // })

     // val h = vacanciesVectors.first()
     //

      /*val data = vacancies.map(vac => {
        val v = vac.wordsVector
          .map().toSeq
        ss.createDataFrame(v).toDF("word", "id")
      }).take(5) */

    /*  val schema = StructType(
        StructField("str", StringType) ::
          StructField("dbl", DoubleType) :: Nil
      )

      data.foreach(v => {
        val stemmed = new Stemmer()
          .setLanguage("Russian")
          .setInputCol("word")
          .setOutputCol("stemmed")
          .transform(v)
        stemmed.show
      }) */
    //  val bagOfWords = vacanciesStemmed
    //    .fold(Nil)((v1, v2) => (v1 ::: v2)
     //     .distinct)

    //  val counts = vacancies.map(vac => vac
   //     .wordsVector
  //      .groupBy(word => word)
   //     .mapValues(_.size))

      val i = 1
      //vacanciesText.foreach(x => println(x.toString()))
    }
//      jsonStringRDD.foreach(raw_data => {
//          implicit  val formats = DefaultFormats
//          val parsedJson = parse(raw_data)

 //         val list_data = for { x <- (parsedJson \ "name").extract[String] } yield x
          //for(single_element <- list_data)
           // {
  //        println(list_data)
           // }
     //     println("--------------------------------------------------------- \n ")
    //    })
   //   }
    // ------------------------------------------- Don't Delete -------------------------------------------------------
    //val bsonRDD = mongoRDD.map(x=>x._2.get("Plane_03_07_16"))	// Array[(Object, org.bson.BSONObject)] --> Array[org.bson.BSONObject]
    /*
    val jsonStringRDD = bsonRDD.map(x => x.toString)	// org.apache.spark.rdd.RDD[org.bson.BSONObject] --> org.apache.spark.rdd.RDD[String]
    jsonStringRDD.foreach(rddJson=>{
         println(rddJson)
        //var number_list = for { JString(x) <- parse(rddJson) \\ "number" } yield x  // WHERE "number" is a key of the data document
        //for (extract_elemtn_ <- number_list)  println(extract_elemtn_)
    })*/

    /*
    val data__rdd = mongoRDD.foreach(a=>{
        val k = JSON.parseFull(a)
        print(k)
    })
    */
    /*
    val countsRDD = mongoRDD.flatMap(arg => {
      var str = arg._2.get("text").toString
      str = str.toLowerCase().replaceAll("[.,!?\n]", " ")
      str.split(" ")
    }).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    val saveRDD = countsRDD.map((tuple) => {
      var bson = new BasicBSONObject()
      bson.put("word", tuple._1)
      bson.put("count", tuple._2)
      (null, bson)
    })
    saveRDD.saveAsNewAPIHadoopFile("E://spark-count.txt", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)
    */

  }
}
