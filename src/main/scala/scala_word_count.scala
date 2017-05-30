//import org.json4s.JsonDSL._
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

object scala_word_count {
  def main(args: Array[String])
  {
    val sc = new SparkContext("local", "Scala Vacancy Keyword Generator")
    val config = new Configuration()
    //config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/local.test_mongo_spark")
    config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/local.new_plane_collection")  // "new_plane_collection" the collection name
    config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/local.output__")
    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])
    //print("COUNT FROM MONGODB --- > " + mongoRDD.count())
    //val bsonRDD = mongoRDD.map(x=>x._2.get("Plane_17_12_16"))	// Array[(Object, org.bson.BSONObject)] --> Array[org.bson.BSONObject]
    val bsonRDD = mongoRDD.map(x=>x._2.get("Plane_03_07_16"))	// Array[(Object, org.bson.BSONObject)] --> Array[org.bson.BSONObject]

    val jsonStringRDD = bsonRDD.map(x => x.toString)	// org.apache.spark.rdd.RDD[org.bson.BSONObject] --> org.apache.spark.rdd.RDD[String]
    jsonStringRDD.foreach(rddJson=>{
        var number_list = for { JString(x) <- parse(rddJson) } yield x  // WHERE "number" is a key of the data document
        println(number_list)
        //for (extract_elemtn_ <- number_list)  println(extract_elemtn_)
    })
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
