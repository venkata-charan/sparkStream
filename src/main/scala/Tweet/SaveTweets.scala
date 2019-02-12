package Tweet


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object SaveTweets extends App {

  println(" --------------------- lets start app -------------------------" )

  val tweets = SetUpTwitter.getTweetSetup("SaveTweets")
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
