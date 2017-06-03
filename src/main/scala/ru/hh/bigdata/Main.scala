package ru.hh.bigdata
import com.mongodb.{BasicDBList, BasicDBObject}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.{BSONObject, BasicBSONObject}
import org.apache.lucene.analysis.ru.RussianAnalyzer
import ru.hh.VacancyHandler
import ru.hh.bigdata.util.stemmer.RussianStemmer


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
   val inputCol = "vacancies-test"
   val outputCol = "vacancies-output"
   val config = dbConfig("mongodb://127.0.0.1:27017/", databaseName, inputCol, outputCol)

   val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])

   val count = mongoRDD.count()
   if( count != 0) {
      val vacanciesRaw = mongoRDD.map(x => VacancyHandler.vacancyAsMap(x._2))

      val vacanciesWordVectors = vacanciesRaw.map(v => VacancyHandler.makeWordVector(v))
      val vacancies = vacanciesWordVectors.map(vec =>
        VacancyHandler.stemAndClearVector(vec))

      val bagOfWords = vacancies
       .fold(Nil)((v1, v2) => v1 ::: v2).distinct

      val vacFeatures = vacancies.map(vac => {
          bagOfWords
            .map(wordFromBag => (wordFromBag, vac.count(word => word == wordFromBag)))
        })

        val vacFinalRDD = vacFeatures.map(wc => {
          var bson = new BasicBSONObject()
          var words = SerializationUtil.serialize(wc)
          bson.put("words", words)
          (null, bson)
        })

        vacFinalRDD.saveAsNewAPIHadoopFile(
          "file:///this-string-is-unused.txt",
          classOf[Any],
          classOf[Any],
          classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)
    }
  }
}
