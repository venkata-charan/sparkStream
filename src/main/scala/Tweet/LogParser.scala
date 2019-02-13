package Tweet

import java.util.regex.Matcher
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import LogRegex.apacheLogPatter

object LogParser extends App {

  val conf = new SparkConf()
  conf.setAppName("LogParser").setMaster("local[2]")
  val ssc = new StreamingContext(conf,Seconds(1))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")
  val pattern = apacheLogPatter()
  val lines = ssc.socketTextStream("localhost",9999)

  val requests = lines.map(x => {
    val matcher:Matcher = pattern.matcher(x)
    if (matcher.matches()) matcher.group(5)
    else "Wrong regex function"
  })

  val urls = requests.map( x => {
    val ary = x.toString.split(" ")
    if (ary.size == 3) ary(1)
    else "[Error]"
  })

  val urlsCount = urls.map(line => (line,1))
    .reduceByKeyAndWindow(_+_,_-_,Seconds(300),Seconds(1))
    .transform(rdd => rdd.sortBy( x => x._2, false))

  //print top 10
  urlsCount.print()

  ssc.start()
  ssc.awaitTermination()

}
