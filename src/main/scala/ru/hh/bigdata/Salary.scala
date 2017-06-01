package ru.hh.bigdata

/**
  * Created by JuriaSan on 01.06.2017.
  */
class Salary(from: Option[Double], to: Option[Double], currency: String) {

  def convert(currency: String): Salary = {
    val fromCur = this.currency
    val toCur = currency

    from match {
      case Some(f) => to match {
        case Some(t) => if (this.currency == currency)
          this
        else
          new Salary(Some(f * Currency.course(fromCur, toCur)),
                Some(t * Currency.course(fromCur, toCur)), currency)
        case None => new Salary(Some(f / Currency.course(fromCur,toCur)), None, currency)
      }
      case None => to match {
        case Some(t) =>
          new Salary(None, Some(t / Currency.course(fromCur, toCur)), currency)
        case None => this
      }
    }
  }

  def average: Option[Double] =  from match {
    case Some(f) => to match {
      case Some(t) => Some((f + t) / 2)
      case None => Some(f)
    }
    case None => to match {
      case Some(t) => Some(t)
      case None => None
    }
  }
}
