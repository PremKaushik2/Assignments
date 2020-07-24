import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionUtil {

    def createSparkSession():SparkSession={
      System.setProperty("hadoop.home.dir", "C:/winutils");
      val sparkConfiguration = new SparkConf().setAppName("Simple Application").
        setMaster(("local[*]")).
        set("spark.executor.memory", "1g") // 4 workers
        .set("spark.executor.instances", "1")
        // 5 cores on each workers
        .set("spark.executor.cores", "5")

      SparkSession.builder
        .config(sparkConfiguration
        ).getOrCreate()
    }

    val spark = createSparkSession()

}
