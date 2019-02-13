package com.app

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object StreamOperations {

  def capitalizeWindowed(input: DStream[Char]): DStream[Char] = {
    input.map(_.toUpper)
      .window(windowDuration = Seconds(3), slideDuration = Seconds(2))
  }

}
