package ru.hh.bigdata
import com.mongodb.BasicDBList
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
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
  val databaseName = "hh-crawler"

  // reads the bag of words from database
  def getBagOfWords(sc: SparkContext): List[String] = {

    val inputCol = "bag-of-words"
    val outputCol = "vacancies-output"

    val config = dbConfig("mongodb://127.0.0.1:27017/", databaseName, inputCol, outputCol)

    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])

    mongoRDD.map(i => i._2.get("word").asInstanceOf[String]).collect().toList
  }

  // collects bags of words, saves it to database and returns a bag of words in-memory
  // collection
  def collectBagOfWords(sc: SparkContext, vacancies: RDD[List[String]]): List[String] = {

      val bagOfWordsRDD = VacancyHandler.bagOfWords(vacancies)

      val bagConfig = new Configuration()
      bagConfig.set("mongo.output.uri",
        "mongodb://127.0.0.1:27017/" + databaseName + ".bag-of-words")

      SerializationUtil.serializeStringVec(bagOfWordsRDD).saveAsNewAPIHadoopFile(
        "file:///this-string-is-unused.txt",
        classOf[Any],
        classOf[Any],
        classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], bagConfig)


      bagOfWordsRDD.collect().toList
  }

  def main(args: Array[String]): Unit = {

   System.setProperty("hadoop.home.dir", "c:\\winutils\\")

   val sc = new SparkContext("local[*]","Extract words ")
    val inputCol = "vacancies"
    val outputCol = "vacancies-output"
    val config = dbConfig("mongodb://127.0.0.1:27017/", databaseName, inputCol, outputCol)
    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])
    val vacanciesRaw = mongoRDD.map(x => VacancyHandler.vacancyAsMap(x._2))
   // val count = mongoRDD.count()
     val vacancies = VacancyHandler.stemAndClearRDD(vacanciesRaw)
     val bagOfWords = collectBagOfWords(sc, vacancies)
     //val bagOfWords = getBagOfWords(sc)

    // val vacFeatures = VacancyHandler.vectorizeVacanciesRDD(vacancies, bagOfWords)
    // val m = new RowMatrix(vacFeatures)
     //val vacFinalRDD = SerializationUtil.serializeIntVec(vacFeatures)

   //  val partitionNum = 3000
    // val vaFinalRDDReparitioned = vacFinalRDD.repartition(partitionNum)

     //println("Starting to save to mongodb")
    // vacFinalRDD.saveAsNewAPIHadoopFile(
    //  "file:///this-string-is-unused.txt",
    //  classOf[Any],
    //  classOf[Any],
    //  classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)

  }
}
