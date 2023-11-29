import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object streaming_kafka {
    def main(args: Array[String]) = {

        val spark = SparkSession
        .builder
        .appName("StructuredNetworkWordCount")
        .config("spark.master", "local")
        .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._
        // Subscribe to 1 topic, with headers
        val inpuDf = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "quickstart-events")
        .option("includeHeaders", "true")
        .load()

        // df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
        // .as[(String, String, Array[(String, Array[Byte])])]   
        // val outputDf = inpuDf.selectExpr("CAST(value AS STRING)").as[String]
        val outputDf = spark.read.json(Seq(inpuDf.selectExpr("CAST(value AS STRING)").as[String]).toDS)
        val query = outputDf.writeStream
        .outputMode("append")
        .format("console")
        .start()

        query.awaitTermination()
    }
}

object streaming_1 {
    def main_1(args: Array[String]) = {

        val spark = SparkSession
        .builder
        .appName("StructuredNetworkWordCount")
        .config("spark.master", "local")
        .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._
        
        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        val lines = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

        // Split the lines into words
        val words = lines.as[String].flatMap(_.split(" "))

        // Generate running word count
        val wordCounts = words.groupBy("value").count()

        // Start running the query that prints the running counts to the console
        val query = wordCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()

        query.awaitTermination()
    }
}