package com.mytaxi.data.test.paymentservice

import org.apache.log4j.Logger

object PaymentService {
var bootStarpServerIP:String = _

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getName)

    val topic = args(0)
    bootStarpServerIP = args(1)
    val CLIENT_ID = args(2)

    logger.info(s"Starting Data Generator for topic ${topic}...")
    logger.info(s"kafka.bootstrap.servers: ${bootStarpServerIP}")
    logger.info(s"CLIENT_ID: ${CLIENT_ID}")

    while (true) {

      val r = scala.util.Random
      val id = r.nextInt(1000)
      val alphabet = ('a' to 'z') ++ ('A' to 'Z')
      val ename = (1 to 7).map(_ => alphabet(r.nextInt(alphabet.size))).mkString

      val s1 = ename.toLowerCase
      var gender: String = null
      var match1 = ename.charAt(0)
      var match2 = s1.charAt(0)
      if (match1.equals(match2)) {
        gender = "Female"
      } else {
        gender = "Male"
      }

      val salary = r.nextInt(100000)
      val deptNo = r.nextInt(100)

      val payload =
        s"""{"identifier":"Source1","Data":{"empid": $id,"ename": "$ename","sal": $salary,"gender": "$gender","deptNo": $deptNo}}"""
      ConfluentProducer.send(topic, payload)
      Thread.sleep(1000)
    }
  }
}