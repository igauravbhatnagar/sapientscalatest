package org.example

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import java.util.Date

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

  var totalSessionDuration:Long = 0
  def sessionDuration = udf((ts1: Timestamp, ts2: Timestamp) => {
    if (ts2 == null) {
      totalSessionDuration = 0
      0
    }
    else {
      val sessionDifferenceInMinutes:Long = (ts1 - ts2)/60
      totalSessionDuration = totalSessionDuration + sessionDifferenceInMinutes
      if (totalSessionDuration > 120) 0 else (ts1 - ts2)/60
    }
  })

  var sess_id_suffix = 0 //assignSessionId -Clojure

  def assignSessionId = udf((dur_mins: Int) => {
    if (dur_mins == 0 && sess_id_suffix == 0) sess_id_suffix = 1                      //for first session of the users
    else if (dur_mins == 0 && sess_id_suffix > 0) sess_id_suffix += 1
    else if (dur_mins > 30) sess_id_suffix += 1
//    else if (dur_mins > 120) sess_id_suffix += 1
    "Session" + sess_id_suffix
  })
}
