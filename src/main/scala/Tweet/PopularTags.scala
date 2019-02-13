package Tweet

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.concurrent.duration.Duration

object PopularTags extends App {

  // common twitter set up
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
  // common twitter set up done
  // now popular hash tags
  val windowSize = args(0).toInt
  val slidingInterval = args(1).toInt
  val tweet_text = english_tweets.map(status => status.getText()) // extract text
  val tweet_words = tweet_text.flatMap(line => line.split(" ")) // extract words from the line
  val hash_words = tweet_words.filter(word => word.startsWith("#")) // filter words with #hashtag starting with #
  val map_hash_words = hash_words.map(word => (word,1)) // map hashtag with 1, to do countingg
  val reduce_hash_words = map_hash_words.reduceByKeyAndWindow(_+_,_-_,Seconds(windowSize),Seconds(slidingInterval))// reduce the hashtags by count
  val sorted_tags = reduce_hash_words.transform(rdd => rdd.sortBy(x=> x._2,false))// sort the rdd to get descending hashtags first
  sorted_tags.print()// print top 10 hashtags
  println("-------------------sliding interval completed ------------------------")

  ssc.checkpoint("hdfs:///user/charanrajlv3971/checkpoint/")
  ssc.start()
  ssc.awaitTermination()

}
