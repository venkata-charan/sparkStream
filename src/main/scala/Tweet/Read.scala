package Tweet

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


object Read extends  App {

  val consumerKey = "vMRUQDIOL13ivVkNeKgLcHe0C"
  val consumerSecret =  "88TMCrCk08IXESlXs85OqjtTuzBwo2Qv7gIP1IbBhYjLRDEZmh"
  val accessToken = "1094902540607602688-qWPl4eTnJGKuD4ZFXT01MtohL3Ln2o"
  val accessTokenSecret = "OM0481sm8qkti91TwYNpzYNuYEuKhlsLRQQfqWDMGBKdh"

  val appName = "cherryTweets"
  val conf = new SparkConf()
  conf.setAppName(appName).setMaster("local[2]")
  val ssc = new StreamingContext(conf,Seconds(5))
  val cb = new ConfigurationBuilder

  cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessTokenSecret)

  val auth = new OAuthAuthorization(cb.build())


  val tweets = TwitterUtils.createStream(ssc,Some(auth))
  val english_tweets = tweets.filter(_.getLang() == "en")

  english_tweets.print(20)

  ssc.start()
  ssc.awaitTerminationOrTimeout(30000)

}
