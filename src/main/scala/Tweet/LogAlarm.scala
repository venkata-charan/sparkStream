package Tweet

import java.util.regex.Matcher

import Tweet.LogRegex.apacheLogPatter
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogAlarm extends App {

  val conf = new SparkConf()
  conf.setAppName("LogParser").setMaster("local[2]")
  val ssc = new StreamingContext(conf,Seconds(1))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")
  val pattern = apacheLogPatter()
  val lines = ssc.socketTextStream("localhost",9999)

  val requests = lines.map(x => {
    val matcher:Matcher = pattern.matcher(x)
    if (matcher.matches()) matcher.group(6)
    else "Wrong regex function"
  })

  // now Map these status results to success and failure
  val SuccessFailure = requests.map(x => {
    val statusCode = util.Try(x.toInt) getOrElse(0)
    if (statusCode >= 200 && statusCode < 300){
      "Success"
    }else if (statusCode >= 500 && statusCode < 600){
      "Failure"
    }else "Other"
  })

  // get count - key and count for successfailure
  val statusCount = SuccessFailure.countByValueAndWindow(Seconds(3000),Seconds(1))

  statusCount.foreachRDD((rdd,time) => {

    var positive:Long = 0
    var negative:Long = 0

    if (rdd.count()> 0 ){

      val elements = rdd.collect()

      for (element <- elements){
        if (element._1 == "Success"){
          positive += element._2
        }else  if (element._1 == "Failure"){
          negative += element._2
        }
      }
    }

    println(s"Total success is $positive , total failure is $negative")

    // start alarm if it has 100 events
    if (positive + negative > 100) {

      val ratio:Double = util.Try(negative.toDouble/positive.toDouble) getOrElse(1)
      if (ratio > 0.5){
        println("jambal hot ayyindi call ganesh single hand")
      }else{
        println("running smooth")
      }
    }else{
      println("No 100 events to work with")
    }

  })

  //lets start the app
  ssc.checkpoint("hdfs:///user/charanrajlv3971/checkpoint/")
  ssc.start()
  ssc.awaitTermination()

}
