package ru.hh.bigdata

/**
  * Created by JuriaSan on 01.06.2017.
  */

object Currency {
  lazy val course = Map(
    ("RUR", "EURO") -> 65.0,
    ("EURO", "RUR") -> 1 / 65.0,
    ("DOLLAR", "RUR") -> 1 / 63.0,
    ("RUR", "DOLLAR") -> 63.0)
    .withDefaultValue(1.0)

  lazy val currency = List("RUR","EURO", "DOLLAR")
  lazy val defaultCurrency = "RUR"
}
