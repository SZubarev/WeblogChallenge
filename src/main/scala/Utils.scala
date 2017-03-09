import java.time.Instant

object Utils {


  def timeStrToLong(timeString: String): Long =
    Instant.parse(timeString).toEpochMilli()


  val lineRE = """(\S*)\s\S*\s(\S*):.*(http\S*).*""".r


}
