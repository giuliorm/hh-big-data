package ru.hh.bigdata
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.{BSONObject, BasicBSONObject}

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

   System.setProperty("hadoop.home.dir", "c:\\winutils\\")

   val sc = new SparkContext("local[*]","Extract words ")

   val databaseName = "hh-crawler"
   val inputCol = "vacancies-test"
   val outputCol = "vacancies-output"
   val config = dbConfig("mongodb://127.0.0.1:27017/", databaseName, inputCol, outputCol)

   val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])

   val count = mongoRDD.count()

   if( count != 0) {
     val vacanciesRaw = mongoRDD.map(x => VacancyHandler.vacancyAsMap(x._2))

     //vacanciesRaw.take(1000).foreach(v => println(v("requirements")))

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
