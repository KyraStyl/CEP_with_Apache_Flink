package flinkprogram

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import scala.math.max

class StockTimeAssigner extends AssignerWithPeriodicWatermarks[StockEvent] with Serializable {

  //1000L == 1 second
  val maxOutOfOrderness = 1000L
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(element: StockEvent, prevElementTimestamp: Long) = {
    val timestamp = element.timestamp+1
    currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark(): Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}