package Tweet

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SaveTweets extends App {

  println(" --------------------- lets start app -------------------------" )
  val conf = new SparkConf()
  val ssc = new StreamingContext(conf,Seconds(5))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")
  conf.setAppName("SaveTweets").setMaster("local[2]")
  val tweets = TwitterUtils.createStream(ssc,Some(SetUpTwitter.getAuth))
  //---- set up of twitter stream object completed

  val hindi_tweets = tweets.filter(_.getLang() == "hi")
  val statuses = hindi_tweets.map(status => status.getText())
  var TotalTweets:Long = 0

  println(" --------------------- setup done, lets save it ------------------------- " )

  statuses.foreachRDD((rdd,time)=>{
    // don't bother empty Rdd, only consider actual rdd's
    if (rdd.count() > 0){

      val repart_Rdd = rdd.repartition(1).cache()
      repart_Rdd.saveAsTextFile("hdfs:///user/charanrajlv3971/saveTweets_" + time.milliseconds.toString)
      TotalTweets += repart_Rdd.count()
      println("Tweet count " + TotalTweets)

      if(TotalTweets > 1000){
        System.exit(0)
      }

    }
  })

  ssc.start()
  ssc.awaitTermination()

}
