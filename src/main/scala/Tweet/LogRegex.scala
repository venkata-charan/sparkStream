package Tweet

import java.util.regex.Pattern

object LogRegex {

  def apacheLogPatter():Pattern={

    val ip = "([0-9].*)"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request =  "\"(.+?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"

    val regex = s"$ip $client $user $dateTime $request $status $bytes"
    Pattern.compile(regex)
  }

  def apache2LogPatter():Pattern={

    val ip = "([0-9].*)"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "\\[(.*?)"
    val nano = "((\\s).*\\])"
    val request =  "\"(.+?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"

    val regex = s"$ip $client $user $dateTime$nano $request $status $bytes"
    Pattern.compile(regex)
  }



}