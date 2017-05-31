import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.bson.BSONObject

object keyword_extractor_ {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","Extract words ")
    val config = new Configuration()
    ////////// EXAMPLE DATASET ///////////////
    config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/local.Plane_03_07_16")
    config.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat")
    // Read Mongo database
    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])
    // Convert BSON to JSON
    val bsonRDD = mongoRDD.map(x => x._2.get("Plane"))	// Array[(Object, org.bson.BSONObject)] --> Array[org.bson.BSONObject]
    val jsonStringRDD = bsonRDD.map(x => x.toString())	// org.apache.spark.rdd.RDD[org.bson.BSONObject] --> org.apache.spark.rdd.RDD[String]
    if(jsonStringRDD.count()!=0)
      {
        jsonStringRDD.foreach(raw_data=>{
          var list_data = for { JString(x) <- parse(raw_data) \\ "company" } yield x
          for(single_element <- list_data)
            {
              println(single_element)
            }
          println("--------------------------------------------------------- \n ")
        })
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
