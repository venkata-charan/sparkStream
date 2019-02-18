package Tweet

import java.util.regex.Matcher

import org.apache.spark.sql.{Row, SparkSession}
import LogRegex.apache2LogPatter
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.streaming.{Minutes, StreamingContext}



object StrucStream extends App {

  case class LogEntry(ip:String, client:String, user:String, dateTime:String,nano:String, request:String, status:String, bytes:String)
  val logPattern = apache2LogPatter

  // 1.create spark session
  // 2.create a stream read on a folder
  // 3. in each stream session read a line and get status field and time for that record
  // 4. do count operation on status

  val spark = SparkSession.builder()
    .appName("Structured Streaming")
    .config("spark.sql.streaming.checkpointLocation","hdfs:///user/charanrajlv3971/checkpoint/")
    .getOrCreate()

  val inputStream = spark.readStream.text("hdfs:///user/charanrajlv3971/logs/")
  import spark.implicits._

  val structuredData = inputStream.flatMap (x => {
    println("something i am doing in step 1")
    val matcher:Matcher = logPattern.matcher(x.getString(0))
    if (matcher.matches()){
      Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        matcher.group(4),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8)))}
    else None
  }).toDF().select($"status",
    unix_timestamp($"dateTime", "dd/MMM/YYYY:HH:mm:ss").cast(TimestampType).as("date")
  )

  structuredData.createOrReplaceTempView("test")

  spark.sql("Select * from test").show()

  val ssc = new StreamingContext(spark.sparkContext, Minutes(2))

  ssc.start()
  ssc.awaitTermination()

}


