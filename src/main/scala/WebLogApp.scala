import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object WebLogApp {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("WebLogChallenge")
      .getOrCreate()

    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._


    val datadir = "data/"
    val logfile = "2015_07_22_mktplace_shop_web_log_sample.log"
    val sampleLength = 5
    //Load raw data

    val rawRDD = sc.textFile(datadir + logfile)
    println("rawRDD record count: " + rawRDD.count)


    //Parse raw data into RDD[LogRecord]

    val parsedRDD = rawRDD.map {
      case Utils.lineRE(time, ip, url) =>
        LogRecord(Utils.timeStrToLong(time), ip, url)
      case badLine =>
        println(s"Unexpected line: $badLine")
        LogRecord(0L, "", "")
    }

    parsedRDD.cache()

    println("ParsedRDD:")
    parsedRDD.take(sampleLength).foreach(println)


    val logDF = parsedRDD.toDF.cache()
    logDF.createOrReplaceTempView("log")

    // 1. Sessionize log data

    def sessionFlag(timeout: Long) = udf((duration: Long) => if (duration > timeout) 1 else 0)

    def sessionName = udf((ip: String, session: Long) => ip + "_" + session)

    val timeout = 900000L //15 minutes session timeout

    val window = Window.partitionBy("ip").orderBy("timestamp") //Define window settings

    val lagCol = lag(col("timestamp"), 1).over(window)

    val sessionDF = logDF.withColumn("prevtime", lagCol)
      .withColumn("ptime", when($"prevtime".isNull, $"timestamp").otherwise(lagCol))
      .withColumn("duration", $"timestamp" - $"ptime")
      .withColumn("newsession", sessionFlag(timeout)($"duration"))
      .withColumn("session", sum($"newsession").over(window))
      .withColumn("session_id", sessionName($"ip", $"session"))
      .cache()

    sessionDF.createOrReplaceTempView("session")
    sessionDF.count()

    //2. Determine the average session time

    val sessionLenDF = spark.sql("""select max(timestamp)-min(timestamp) as sessionlen, ip, session_id from session group by session_id, ip""").cache()

    println("sessionLenDF:")
    sessionLenDF.limit(sampleLength).collect().foreach(println)

    println("2. Average session time:")
    sessionLenDF.select(avg($"sessionlen").alias("avg_session_time")).collect().foreach(println)

    //3. Determine unique URL visits per session

    val uniqueDF = spark.sql("select count(distinct(url)) uniqueurlcount, session_id from session group by session_id").cache()

    println("3. Unique URLs per session:")
    uniqueDF.sort($"uniqueurlcount".desc).limit(sampleLength).collect().foreach(println)

    // 4. Find the most engaged users, ie the IPs with the longest session times
    println("4. Most engaged users (IPs with longest sessions:")
    sessionLenDF.select($"ip", $"session_id", $"sessionlen").sort($"sessionlen".desc).limit(sampleLength).collect().foreach(println)

    spark.stop()
  }
}
