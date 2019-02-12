package Tweet


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object SaveTweets extends App {

  println(" --------------------- lets start app -------------------------" )
  val consumerKey = "vMRUQDIOL13ivVkNeKgLcHe0C"
  val consumerSecret =  "88TMCrCk08IXESlXs85OqjtTuzBwo2Qv7gIP1IbBhYjLRDEZmh"
  val accessToken = "1094902540607602688-qWPl4eTnJGKuD4ZFXT01MtohL3Ln2o"
  val accessTokenSecret = "OM0481sm8qkti91TwYNpzYNuYEuKhlsLRQQfqWDMGBKdh"

  val appName = "cherryTweets"
  val conf = new SparkConf()
  conf.setAppName(appName).setMaster("local[2]")
  val ssc = new StreamingContext(conf,Seconds(5))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")

  val cb = new ConfigurationBuilder

  cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessTokenSecret)

  val auth = new OAuthAuthorization(cb.build())


  val tweets = TwitterUtils.createStream(ssc,Some(auth))
 // val english_tweets = tweets.filter(_.getLang() == "en")
  val statuses = tweets.map(status => status.getText())

   statuses.print(1)
  var TotalTweets:Long = 0

  println(" --------------------- setup done, lets save it ------------------------- and count is " + statuses.count() )

  statuses.foreachRDD((rdd,time)=>{
    // don't bother empty Rdd, only consider actual rdd's
    if (rdd.count() > 0){

      val repart_Rdd = rdd.repartition(1).cache()
      repart_Rdd.saveAsTextFile("hdfs:///user/charanrajlv3971/saveTweets_" + time.milliseconds.toString)
      TotalTweets += repart_Rdd.count()
      println("Tweet count " + TotalTweets)

      if(TotalTweets > 100){
        System.exit(0)
      }

    }
  })

  ssc.start()
  ssc.awaitTermination()

}
