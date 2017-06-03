package ru.hh.bigdata
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.{BSONObject, BasicBSONObject}
import ru.hh.VacancyHandler

object Main {

  def dbConfig(uri: String,
               databaseName: String,
               inputCol: String,
               outputCol: String): Configuration = {

    val config = new Configuration()
    config.set("mongo.input.uri",
      uri + databaseName + "." + inputCol)
    config.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat")
    config.set("mongo.output.uri",
       uri + databaseName + "." + outputCol)
    config
  }

  def main(args: Array[String]): Unit = {

   val sc = new SparkContext("local[*]","Extract words ")

   val databaseName = "hh-crawler"
   val inputCol = "vacancies"
   val outputCol = "vacancies-output"
   val config = dbConfig("mongodb://127.0.0.1:27017/", databaseName, inputCol, outputCol)

   val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])

   val count = mongoRDD.count()
    println(count)
    return
    /*change*/
   if( count != 0) {
     val vacanciesRaw = mongoRDD.map(x => VacancyHandler.vacancyAsMap(x._2))

     val vacancies = VacancyHandler.stemAndClearRDD(vacanciesRaw)
     val bagOfWords = VacancyHandler.bagOfWords(vacancies)
     val vacFeatures = VacancyHandler.vectorizeVacanciesRDD(vacancies, bagOfWords)

     val vacFinalRDD = SerializationUtil.serialize(vacFeatures)

     vacFinalRDD.saveAsNewAPIHadoopFile(
      "file:///this-string-is-unused.txt",
      classOf[Any],
      classOf[Any],
      classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)
    }
  }
}
