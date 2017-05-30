/**
  * Created by FCUKKKKKKK on 5/28/2017.
  */

import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.bson.BSONObject
import org.bson.BasicBSONObject


object spark_word_count extends App{
  val sc = new SparkContext(new SparkConf().setAppName("IQ Simple Spark App").setMaster("local[*]"))
  //val rdd = sc.parallelize(Seq(1,2,3,4,5))
  //println(rdd.count())

  import org.apache.spark.sql.SQLContext
  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("IQ Simple Spark App")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/local.Plane_03_07_16")
    //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/local.new_plane_collection22")
    .getOrCreate()
  val sqlContext = SQLContext.getOrCreate(sc)
  val df = MongoSpark.load(sqlContext)
  df.printSchema()


  /*
  val sc = new SparkContext("local[*]", "Scala Word Count")
  val config = new Configuration()
  config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/local.Plane_03_07_16")
  //config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/local.output")
  val mongoRDD = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])
  print(mongoRDD.saveAsObjectFile("E:\\spark_output.txt"))
  */
  /*
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/local.Plane_03_07_16")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/local.new_plane_collection22")
    .getOrCreate()
  val df = MongoSpark.load(sparkSession)
  df.printSchema()
  import com.mongodb.spark._
  import com.mongodb.spark.config._
  */


}
