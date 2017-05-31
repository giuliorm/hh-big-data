import com.mongodb.{BasicDBList, BasicDBObject}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject
import scala.collection.mutable

object keyword_extractor {

  def clearString(s: String): String = {
    val charsToRemove: Set[Char] = Set('\"', '[', ']' ,''', '(', ')', '.', ',', ':')
    s.trim()
     .toLowerCase()
     .filterNot(c => charsToRemove.contains(c))
  }

  def retrieveName(m: BSONObject): String =
    clearString(m.get("name").asInstanceOf[String])


  def retrieveSkills(m: BSONObject): List[String] = {
    m.get("key_skills")
      .asInstanceOf[BasicDBList]
      .toArray()
      .map(x => x.asInstanceOf[BasicDBObject])
      .map(dbObject => dbObject.get("name").asInstanceOf[String])
      .map(s => clearString(s))
      .toList
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

    def iter(s: List[String]): List[String] = {
      if (stopKeys.contains(s.head))
        Nil
      else if (s.head == startKey)
        iter(s.tail)
      else s.head :: iter(s.tail)
    }

    iter(words)
  }

  def vacancyAsText(m: BSONObject): String = {
    val name = retrieveName(m)
    val skills = retrieveSkills(m)
    val requirements = retrieveRequirements(retrieveDescription(m))
    val r = name + " " + requirements + " " + skills
    r
  }


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","Extract words ")
    val config = new Configuration()
    ////////// EXAMPLE DATASET ///////////////
    val databaseName = "hh-crawler"
    val collectionName = "vacancies-test"
    config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/" + databaseName + "." + collectionName)
    config.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat")
    // Read Mongo database
    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])
    // Convert BSON to JSON
   // Array[(Object, org.bson.BSONObject)] --> Array[org.bson.BSONObject]
    if(mongoRDD.count() != 0) {
      val vacanciesText = mongoRDD.map(x => {
        val vt = vacancyAsText(x._2)
        val i = 1
      })
      vacanciesText.foreach(x => print(x))
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
