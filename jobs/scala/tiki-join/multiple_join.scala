import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._

object TikiJoin {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Multiple Join")
            .master("spark://spark-master:7077")
            .getOrCreate()
    }
}