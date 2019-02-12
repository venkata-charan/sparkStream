package Tweet

object SaveTweets extends App {

  val tweets = SetUpTwitter.getTweetSetup("SaveTweets")

  val english_tweets = tweets.filter(_.getLang() == "en")
  val statuses = english_tweets.map(status => status.getText())

  var TotalTweets:Long = 0

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
}
