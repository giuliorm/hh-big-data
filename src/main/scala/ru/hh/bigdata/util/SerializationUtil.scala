package ru.hh.bigdata

import com.mongodb.{BasicDBList, BasicDBObject}
import org.bson.BasicBSONObject

/**
  * Created by JuriaSan on 03.06.2017.
  */
object SerializationUtil {

  def serialize(tuple: Pair[String, Int]): BasicBSONObject = {
    var o = new BasicBSONObject()
    o.put("word", tuple._1)
    o.put("count", tuple._2)
    o
  }

  def serialize(list: List[Pair[String, Int]]): BasicDBList = {
     var l = new BasicDBList()
     list.foreach(w => {
      val o = SerializationUtil.serialize(w)
      l.add(o)
    })
    l
  }
}
