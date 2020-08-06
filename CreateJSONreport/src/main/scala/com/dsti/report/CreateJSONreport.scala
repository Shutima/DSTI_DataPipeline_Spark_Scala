package com.dsti.report

import org.apache.spark.sql.SparkSession
//import com.typesafe.config._

//Create case class for json report
case class UriReport(access: Map[String, Long])
case class UriReport2(access: Map[String, Long])

object CreateJSONreport {

  //App spark session and imports
  val spark: SparkSession = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
  import org.apache.spark.sql.functions._
  //import spark.sqlContext.implicits._
  //import library
  import spark.implicits._

  //Define the AccessLog structure
  case class AccessLog(ip: String, ident: String, user: String, datetime: String, request: String, status: String, size: String, referer: String, userAgent: String, unk: String)

  def main(args: Array[String]): Unit = {

    //Load the log file
    val path = "C:\\Users\\spoti\\Documents\\DSTI\\DataPipeline_2\\spark-hands-on\\almhuette-raith.log"
    val logs = spark.read.text(path)
    //logs.count

    //Convert row to a string
    val logAsString = logs.map(_.getString(0))
    //logAsString.count
    // should be 2660050

    //Create AccessLog
    val l1 = AccessLog("ip", "ident", "user", "datetime", "request", "status", "size", "referer", "userAgent", "unk")
    //you can now access the field directly, with l1.ip for example

    AccessLog.apply _

    //Use regex
    val R = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r

    //Parse the string
    val dsParsed = logAsString.flatMap(x => R.unapplySeq(x))
    //dsParsed.count
    // should be 2655452

    //Define toAccessLog
    def toAccessLog(params: List[String]) = AccessLog(params(0), params(1), params(2), params(3), params(4), params(5), params(6), params(7), params(8), params(9))

    val stringExample = logAsString.take(2).drop(1).head
    val parsed = R.unapplySeq(stringExample)
    R.unapplySeq("not a good string")

    //Convert Parsed list of string to an AccessLog
    val ds = dsParsed.map(toAccessLog _)
    //ds.count
    // should be 2655452
    //ds.printSchema

    //Convert time string to timestamp
    val dsWithTime = ds.withColumn("datetime", to_timestamp(ds("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))

    //Check the time type has been updated
    //dsWithTime.printSchema

    //Cache data to avoid everytime a new computation
    //dsWithTime.cache

    //Parse the request field
    val REQ_EX = "([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+)".r
    val s = "POST /administrator/index.php HTTP/1.1"
    REQ_EX.unapplySeq(s)

    //All requests can be filtered by regex
    //dsWithTime.filter(x => REQ_EX.unapplySeq(x.getString(x.fieldIndex("request"))).isEmpty).show(false)

    //Replace the request column by method /uri/http
    val dsExtended = dsWithTime.withColumn("method", regexp_extract(dsWithTime("request"), REQ_EX.toString, 1)).withColumn("uri", regexp_extract(dsWithTime("request"), REQ_EX.toString, 2)).withColumn("http", regexp_extract(dsWithTime("request"), REQ_EX.toString, 3)).drop("request").cache

    //Check that the Schema has been updated
    //dsExtended.printSchema

    //Find all dates having too large number of connections (>20000)
    dsExtended.createOrReplaceTempView("AccessLogExt")
    //spark.sql("select count(distinct(ip)) as count, cast(datetime as date) as date from AccessLogExt where uri like '/administrator%' group by cast(datetime as date) order by cast(datetime as date) asc").show(false)

    val sql = "select count(*) as count, cast(datetime as date) as date from AccessLogExt group by date HAVING count(*) > 20000 order by count desc"

    //spark.sql(sql).show(false)

    //Declare variable dates
    val dates = Seq("2018-06-27")

    //Find dates having more than 20K connections
    //Put the SQL query above in the variable data frame
    val countAndDates = spark.sql(sql)
    //Get only the dates
    val onlyDates = spark.sql(sql).select("date")
    //onlyDates.collect()

    def findDatesHavingMoreThan20kConnections: Seq[java.sql.Date] = spark.sql(sql).select("date").map(_.getDate(0)).collect()
    //When call this function findDatesHavingMoreThan20kConnections, it will give a lits of dates

    //Assign the function findDatesHavingMoreThan20kConnections which is the list of dates with more than 20k connections to the var theDates
    val theDates = findDatesHavingMoreThan20kConnections

    //for each date
    //compute the list of number of access by URI for each URI

    //Assign the first date to currentDate
    val currentDate = theDates(0)

    //def numberOfAccessByUri(date: java.sql.Date): Map[String, Long] = ???

    //All the accesses for the currentDate
    //spark.sql("select cast(datetime as date) as date from AccessLogExt").where(col("datetime") === currentDate)
    //This will give only the dates, all the dates
    //To select both date and URI
    //spark.sql("select cast(datetime as date) as date, uri from AccessLogExt").where(col("datetime") === currentDate)

    //If want to show only URI
    //col("datetime").cast("date") === currentDate
    //spark.sql("select uri from AccessLogExt").where(col("datetime").cast("date") === currentDate).show(false)

    //To get a count of URI by URI
    //spark.sql("select uri, cast(datetime as date) as date, count(*) as countaccess from AccessLogExt group by date, uri").show(false)

    //Then filter only for the currentDate
    //spark.sql("select uri, cast(datetime as date) as date, count(*) as countaccess from AccessLogExt group by date, uri").filter(col("date") === currentDate).show(false)

    //Then order by countaccess
    //spark.sql("select uri, cast(datetime as date) as date, count(*) as countaccess from AccessLogExt group by date, uri order by countaccess desc").filter(col("date") === currentDate).show(false)

    //Assign this new sql query to the function numberOfAccessByUri
    //def numberOfAccessByUri(date: java.sql.Date) = spark.sql("select uri, cast(datetime as date) as date, count(*) as countaccess from AccessLogExt group by date, uri order by countaccess desc").filter(col("date") === currentDate)

    //Confirm that this function works properly
    numberOfAccessByUri(currentDate)
    //numberOfAccessByUri(currentDate).show(false)

    //Then drop the date col
    def numberOfAccessByUri(date: java.sql.Date) = spark.sql("select uri, cast(datetime as date) as date, count(*) as countaccess from AccessLogExt group by date, uri order by countaccess desc").filter(col("date") === currentDate).drop("date")
    //Now we don't display the date col anymore
    //numberOfAccessByUri(currentDate).show(false)

    //When we use this function numberOfAccessByUri, we get the list as a result
    numberOfAccessByUri(currentDate).collect

    //Assign value to UriReport class
    UriReport(numberOfAccessByUri(currentDate).collect.map(r => (r.getString(0), r.getLong(1))).toMap)

    //Define function reportByDate
    def reportByDate(currentDate: java.sql.Date) = UriReport(numberOfAccessByUri(currentDate).collect.map(r => (r.getString(0), r.getLong(1))).toMap)

    //When we use this function on currentDate, it will give the UriReport
    reportByDate(currentDate)

    //To have a report for all the dates
    //Variable theDates contains all the dates, therefore we can use this with the function reportByDate by mapping it to give the report for all the dates
    theDates.map(reportByDate)

    //We can use function above to assign the report for all dates to the variable reportAsSeq
    val reportAsSeq: Seq[(java.sql.Date, UriReport)] = theDates.map(date => (date, reportByDate(date)))

    //Now we can change to dataframe to get date and uriReport
    //reportAsSeq.toDF("date", "uriReport")
    Seq((1, "a")).toDF().show(false)
    Seq(UriReport(Map.empty)).toDF().show(false)
    Seq(("2015-03-31", UriReport(Map.empty))).toDF().show(false)

    //Then we write to json format
    reportAsSeq.toDF("date", "uriReport").coalesce(1).write.mode("Overwrite").json("myjsonreport_reportByDate")

    //compute the list of number of access per IP address for each IP address
    def reportByIp(date: java.sql.Date) = spark.sql("select ip as ip, count(*) as countaccess from AccessLogExt group by ip order by countaccess desc")

    //Test the function generates correct report of the list of number of access per IP address for each IP address
    reportByIp(currentDate).show(false)

    reportByIp(currentDate).collect

    //Create function reportByIpComplete to use the case class
    def reportByIpComplete(currentDate: java.sql.Date) = UriReport2(reportByIp(currentDate).collect.map(r => (r.getString(0), r.getLong(1))).toMap)

    //Test the function reportByIpComplete with currentDate
    //reportByIpComplete(currentDate)
    theDates.map(reportByIpComplete)

    //Declare variable reportAsIp to prepare for new json report by IP
    val reportAsIp = theDates.map(date => (date, reportByIpComplete(date)))

    //Convert this to dataframe to create new json report by IP
    //reportAsIp.toDF("date", "uriReport2").coalesce(1).write.mode("Overwrite").json("myjsonreport_reportByIp")

    //compute the list of number of connections from 10  days before to 10 days after
    //generate at outputPath a report in json format with one json report line per date
  }
}
