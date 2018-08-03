package me.ycan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Try

case class RawData(a: String, b: String, c: String)
case class GroupedMyMotherName(mother: String, count: Long){
  override def toString(): String= s"Mother name: $mother, Count: $count"
}

object ExampleSparkApp extends App{

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test-app")
  val sparkContext: SparkContext = SparkContext.getOrCreate(conf)

  val rawData: RDD[String] = sparkContext.textFile(path = "/home/ryskulova/Documents/SparkScala/kodluyoruzBigData/test-scala-project/test.txt")

  val readData: RDD[Try[RawData]] =rawData.map{ line =>
    val fields = line.split(" ")
    Try(
      RawData(fields(0), fields(1), fields(2))
    )
  }

  val filteredRawData: RDD[RawData] = readData
    .filter(maybeRawData => maybeRawData.isSuccess)
    .map(data => data.get)

 val result1 = filteredRawData.map(r=>(r.a,1)).reduceByKey((value1, value2)=>(value1+value2)).collect()
  result.foreach(println)
  //.reduceByKey((acc, n) => (acc + n)).collect()

 val result1: RDD[GroupedMyMotherName] = filteredRawData.groupBy(_.a).map{ case (key, values) =>
    GroupedMyMotherName(key, values.size)
  }

  val collectedResult: Array[GroupedMyMotherName] = result.collect()

  collectedResult.foreach(println)













//    val sparkSession = SparkSession
//      .builder()
//      .appName("my-test-spark-app")
//      .getOrCreate()
//


}
