package com.app

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class KafkaSpec extends FlatSpec with EmbeddedKafka with BeforeAndAfterAll {

  override def beforeAll():Unit = {
    EmbeddedKafka.start()
  }

  override def afterAll():Unit = {
    EmbeddedKafka.stop()
  }

}
