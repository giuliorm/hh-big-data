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

  }
}
