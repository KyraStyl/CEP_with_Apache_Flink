package flinkprogram

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{EventComparator, CEP => JCEP}
import org.apache.flink.streaming.api.scala.DataStream

case class StockEvent(id: Int, timestamp: Long, symbol: Int, price: Int, volume: Int){

  def this() {
    this(-1,0L,0,0,0)
  }

  override def toString: String = {
    s"THIS IS AN EVENT WITH  ID: $id \t Timestamp: $timestamp \t Symbol: $symbol \t Price: $price \t Volume: $volume"
  }

  override def hashCode(): Int = {
    id*31+symbol
  }

  override def equals(other: Any): Boolean = other match {
    case that: StockEvent =>
      this.id == that.id &&
      this.timestamp == that.timestamp &&
      this.price == that.price &&
      this.volume == that.volume &&
      this.symbol == that.symbol
    case _ => false
  }
}