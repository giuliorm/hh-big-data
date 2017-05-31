//import org.json4s.JsonDSL._
import com.fasterxml.jackson.annotation.JsonValue
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

object scala_word_count {
  def main(args: Array[String])
  {
    val conf = new SparkConf().setAppName("hh-scala-spark").setMaster("local").set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    val config = new Configuration()
    val collectionName = "vacancies-test"
    val outputCollectionName = "vacancies-test-output"
    val databaseName = "hh-crawler"

    //config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/local.test_mongo_spark")
    config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/" + databaseName + "." + collectionName)  // "new_plane_collection" the collection name
    config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/" + databaseName + "." + outputCollectionName)


    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    print("NUMNER OF RECORDS FROM  MONGODB --- > \n " + mongoRDD.count())

    if(mongoRDD.count()!=0)
    {
      val jsonStringRDD = mongoRDD.map(x => x._2.get("Plane").toString)	// Array[(Object, org.bson.BSONObject)] --> Array[org.bson.BSONObject]
      implicit val formats = DefaultFormats
      val vacanciesTestRepresentation = jsonStringRDD
            .map(json => {
              val parsedJson = parse(json)
              val name = (parsedJson \ "name").extract[String]
              val description = (parsedJson \ "description").extract[String]
              val skills = (parsedJson \ "key_skills").extract[Array[String]]

              //remove same words
              //remove most common words
              //

            })


      /*     jsonStringRDD.foreach(rddJson => {
            var values__ = for { JString(x) <- parse(rddJson) \ "number"} yield x
            for(extract_val <- values__){println(extract_val)}
            print("-------------------------------------------------------------------------------\n")
          })*/
    }

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
