package Tweet

import java.util.concurrent.atomic.AtomicLong


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import SetUpTwitter.set

object AverageTweets extends App {

  val appName = "cherryTweets"
  val conf = new SparkConf()
  conf.setAppName(appName).setMaster("local[2]")
  val ssc = new StreamingContext(conf,Seconds(1))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")
  set()
  val tweets = TwitterUtils.createStream(ssc,None)
  val english_tweets = tweets.filter(_.getLang() == "en")

  val tweets_list = english_tweets.map(statuses => statuses.getText())
  val len_tweets = tweets_list.map(tweet => tweet.length)

  val totalTweets = new AtomicLong(0)
  val totalChars = new AtomicLong(0)

  len_tweets.foreachRDD( (rdd,_) => {

    if (rdd.count > 0) {

      totalTweets.getAndAdd(rdd.count())
      totalChars.getAndAdd(rdd.reduce((a, b) => a + b))
      println("Total Tweets = "+ totalTweets.get()  +
        ", Total chars = "+ totalChars.get() +
        ", Average of tweets length = " + (totalChars.get()/totalTweets.get()))


    }
  })

  ssc.start()
  ssc.awaitTerminationOrTimeout(30000)

}
