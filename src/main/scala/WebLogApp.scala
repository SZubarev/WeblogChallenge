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
    val logfile = "2015_07_22_mktplace_shop_web_log_sample.gz"

    val rawRDD = sc.textFile(datadir + logfile)
    println("rawRDD" + rawRDD.count)


    val parsedRDD = rawRDD.map {
      case Utils.lineRE(time, ip, url) =>
        LogRecord(Utils.timeStrToLong(time), ip, url)
      case badLine =>
        println(s"Unexpected line: $badLine")
        LogRecord(0L, "", "")
    }

    parsedRDD.cache()

    println("ParsedRDD:")
    parsedRDD.take(3).foreach(println)


    val logDF = parsedRDD.toDF.cache()
    logDF.createOrReplaceTempView("log")

    //Define Spark UDFs

    def sessionFlag(timeout: Long) = udf((duration: Long) => if (duration > timeout) 1 else 0)

    def sessionName = udf((ip: String, session: Long) => ip + "_" + session)

    val timeout = 900000L //15 minutes session timeout

    //Define window settings
    val window = Window.partitionBy("ip").orderBy("timestamp")

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

    val sessionLenDF = spark.sql("""select max(timestamp)-min(timestamp) as sessionlen, ip, session_id from session group by session_id, ip""").cache()

    println("sessionLenDF:")
    sessionLenDF.limit(10).collect().foreach(println)

    println("Avg session time:")
    sessionLenDF.select(avg($"sessionlen").alias("avg_session_time")).collect().foreach(println)

    val uniqueDF = spark.sql("select count(distinct(url)) uniqueurlcount, session_id from session group by session_id").cache()

    uniqueDF.sort($"uniqueurlcount".desc).limit(10).collect().foreach(println)

    sessionLenDF.select($"ip", $"session_id", $"sessionlen").sort($"sessionlen".desc).limit(10).collect().foreach(println)

    spark.stop()
  }
}
