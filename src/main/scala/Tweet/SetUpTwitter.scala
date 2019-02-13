package Tweet


object SetUpTwitter {

  val consumerKey = "vMRUQDIOL13ivVkNeKgLcHe0C"
  val consumerSecret =  "88TMCrCk08IXESlXs85OqjtTuzBwo2Qv7gIP1IbBhYjLRDEZmh"
  val accessToken = "1094902540607602688-qWPl4eTnJGKuD4ZFXT01MtohL3Ln2o"
  val accessTokenSecret = "OM0481sm8qkti91TwYNpzYNuYEuKhlsLRQQfqWDMGBKdh"

  def set()={

    val lines = List (("consumerKey","vMRUQDIOL13ivVkNeKgLcHe0C"),
      ("consumerSecret","88TMCrCk08IXESlXs85OqjtTuzBwo2Qv7gIP1IbBhYjLRDEZmh"),
      ("accessToken","1094902540607602688-qWPl4eTnJGKuD4ZFXT01MtohL3Ln2o"),
      ("accessTokenSecret","OM0481sm8qkti91TwYNpzYNuYEuKhlsLRQQfqWDMGBKdh"))

    for (line <- lines){
      System.setProperty("twitter4j.oauth."+ line._1,line._2)
    }
  }

}
