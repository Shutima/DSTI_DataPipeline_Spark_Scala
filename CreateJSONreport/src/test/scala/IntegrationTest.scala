
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ LongType, StringType, StructField, StructType }
import org.scalatest._

case class Report(date: String, accessCount: Long)

class IntegrationTest extends FunSuite with SharedSparkContext {

  def createSession: SparkSession = SparkSession.builder().getOrCreate()

  // sample implementation only
  def createReport(csvPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.option("header", true).csv(csvPath)

    val reports = Seq(Report("today", 1), Report("two days ago", 20))
    reports.toDF.write.mode("Overwrite").json(outputPath) // not correspond to the right interface
  }

  test("report is not empty") {

    implicit val spark: SparkSession = createSession

    // Given I have a test dataset and an output path
    //val csvPath = "src/test/resources/test-data.csv"
    val csvPath = "C:\\Users\\spoti\\Documents\\DSTI\\DataPipeline_2\\spark-hands-on\\almhuette-raith.log"
    //val outputPath = "target/test/report"
    val outputPath = "C:\\Users\\spoti\\Documents\\DSTI\\DataPipeline_2\\CreateJSONreport"

    // When I create a report in output path
    createReport(csvPath, outputPath)

    // Then the report have more than 0 records
    val df = spark.read.json(outputPath)
    assert(df.count > 0)

  }

  test("report have the expected structure") {

    implicit val spark: SparkSession = createSession

    // Given I have a test dataset and an output path and an expected DDL
    //val csvPath = "src/test/resources/test-data.csv"
    val csvPath = "C:\\Users\\spoti\\Documents\\DSTI\\DataPipeline_2\\spark-hands-on\\almhuette-raith.log"
    //val outputPath = "target/test/report"
    val outputPath = "C:\\Users\\spoti\\Documents\\DSTI\\DataPipeline_2\\CreateJSONreport"
    val expectedDDL = "`accessCount` BIGINT,`date` STRING"
    val expectedStructType = StructType(Seq(StructField("accessCount", LongType), StructField("date", StringType)))

    // When I create a report in output path
    createReport(csvPath, outputPath)

    // Then the report ddl is the expected one
    val df = spark.read.json(outputPath)
    val ddl = df.schema.toDDL
    val schema = df.schema
    assert(ddl === expectedDDL)
    assert(schema === expectedStructType)

  }

  test("report for day X have the right value") {

    implicit val spark: SparkSession = createSession

    import spark.implicits._

    // Given I have a test dataset and an output path and an expected DDL
    //val csvPath = "src/test/resources/test-data.csv"
    val csvPath = "C:\\Users\\spoti\\Documents\\DSTI\\DataPipeline_2\\spark-hands-on\\almhuette-raith.log"
    //val outputPath = "target/test/report"
    val outputPath = "C:\\Users\\spoti\\Documents\\DSTI\\DataPipeline_2\\CreateJSONreport"
    val expectedTodayReport = Report("today", 1)

    // When I create a report in output path
    createReport(csvPath, outputPath)

    // Then the report of today is the expected one
    val df = spark.read.json(outputPath).as[Report]
    val Array(today) = df.where(df("date") === expectedTodayReport.date).take(1)
    assert(today === expectedTodayReport)

  }

}
