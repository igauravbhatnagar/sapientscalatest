package org.example

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.udf

object UserDefinedFunctions extends Serializable {

  def changeColType(df: org.apache.spark.sql.DataFrame, col: String, newType: String) = {
    df.withColumn(col, df(col).cast(typeMatch(newType)))
  }

  def typeMatch(input: String) = {
    import org.apache.spark.sql.types._
    input match {
      case "Int" => IntegerType
      case "String" => StringType
      case "Date" => DateType
      case "Double" => DoubleType
      case "Timestamp" => TimestampType
    }
  }

  var sess_id_flag = 0 //assignSessionId -Clojure

  var sess_id_suffix = 0 //assignSessionId -Clojure
  var session_expired_flag = 0

//  var totalSessionDuration:Long = 0
  def sessionDuration = udf((current_ts: Timestamp, next_ts: Timestamp)  => {
      var diff = (next_ts - current_ts)/60
      if (diff > 30) diff=30
      diff
  })

  def assignSessionId = udf((dur_mins: Int) => {
      if (sess_id_flag == 0){
        sess_id_suffix += 1
        sess_id_flag = 1
      }
      else {
        if (session_expired_flag == 1) sess_id_suffix += 1
        else if (dur_mins == 30){
          session_expired_flag = 1
          sess_id_flag = 0
        }

      }
    "Session" + sess_id_suffix
  })
}
