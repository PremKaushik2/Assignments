import Ingestor.{args, getOddsOccurrence, parse, spark}

object Ingestor2  extends App with SparkSessionUtil{

  def parse(input: String) = {
    if(input.contains(",")) {
      val arr = input.split(",")
      (arr(0).trim.toInt, arr(1).trim.toInt)
    } else if (input.contains("\t")) {
      val arr = input.split("\t")
      (arr(0).trim.toInt, arr(1).trim.toInt)
    } else {
      (-1,-1)
    }
  }
  // Find odd occurring elements in the given array
  def getOddsOccurrence(input: Array[Int]): List[Int] = {
    val lstBuff = scala.collection.mutable.ListBuffer[Int]()
    var xor: Int = 0
    for (i <- input) {
      xor ^= (1 << i)
    }

    for (i <- input if (xor & (1 << i)) != 0) {
      lstBuff.append(i)
      // to avoid printing duplicates
      xor ^= (1 << i)
    }
    lstBuff.toList
  }


  def parseInput(inputPath:String) : Unit={
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val filesRDD = spark.sparkContext.wholeTextFiles(inputPath)

    val linesRDD = filesRDD.flatMap(_._2.split("\n").drop(1))

    val parsedRDD = linesRDD.map(parse).filter(rec => rec._1 != -1 && rec._2 != -1)

    val groupedRDD = parsedRDD.groupByKey()
    groupedRDD.foreach(println)

    val oddOccurRDD = groupedRDD.mapValues(v => getOddsOccurrence(v.toArray))

    val finalRDD = oddOccurRDD.filter(t => t._2.size == 1)
      .map(t => (t._1, t._2.head))

    val df = finalRDD.toDF("key", "value")
    df.printSchema()
    df.show()

    df.coalesce(1)
      .write.mode("overwrite")
      .option("header", true)
      .option("delimiter", "\t")
      .csv(args(1))
  }

  parseInput(args(0))
}


