package Tweet

import java.util.concurrent.atomic.AtomicLong

import Tweet.Read.ssc
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object AverageTweets extends App {
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

  }
  )

  ssc.start()
  ssc.awaitTerminationOrTimeout(30000)



}
