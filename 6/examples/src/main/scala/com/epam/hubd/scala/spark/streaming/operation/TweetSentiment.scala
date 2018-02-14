package com.epam.hubd.scala.spark.streaming.operation

case class TweetSentiment(lineAge: Seq[(String, Int)], sentiment: String) {

  def next(time: Long, count: Int): TweetSentiment = {
    val newLineage = getNewLineAge(time, count)
    TweetSentiment(newLineage, getNewSentiment(newLineage))
  }

  private def getNewLineAge(time: Long, count: Int) = {
    lineAge match {
      case l if l.size < 10 => l :+ (StreamingUtils.YYYYMMDDHHMM.print(time), count)
      case l if l.size == 10 => l.tail :+ (StreamingUtils.YYYYMMDDHHMM.print(time), count)
      case l => l
    }
  }

  private def getNewSentiment(lineage: Seq[(String, Int)]) = {
    lineage.size match {
      case 1 => "NEW"
      case _ => {
        val reversed = lineage.reverse
        reversed.head._2 - reversed.tail.head._2 match {
          case diff if diff < 0 => "FADING"
          case diff if diff == 0 => "STAYING"
          case _ => "TRENDING"
        }
      }
    }
  }
}
