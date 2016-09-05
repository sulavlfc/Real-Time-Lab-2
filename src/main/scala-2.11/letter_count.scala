/**
  * Created by sulav on 9/4/16.
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.apache.spark.SparkContext._
import java.io._

object letter_count {


  def main(args: Array[String]) {
    implicit val formats = DefaultFormats
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val txtfile = sc.textFile("labsentence.txt").flatMap(line => line.split(""))//.sortBy[String]({a => a})//.flatMap(a => a)
    val filtered_rdd = txtfile.filter(a => a.matches("[A-Za-z]")).flatMap(a => a.toLowerCase()).map(a => (a,1))
    println(filtered_rdd.count())
    val lowered_case = filtered_rdd.countByKey() //lowered_case.reduceByKey(_+_))

    //val lower_case = filtered_rdd.flatMap(a => a.toLowerCase()).map(a => (a,1)).reduceByKey(_+_)
    //lower_case.collect().foreach(a => println(a))

    lowered_case foreach (x => println (x._1 + "-->"+x._2))
    //lowered_case foreach (x =>  writer.append(x._1 + "-->"+x._2))
    val fw = new FileWriter("output.txt", true)
    lowered_case foreach (x =>  fw.append(x._1 + " : "+x._2 + '\n'))
    fw.close()

    //println(lowered_case.keys, lowered_case.values)
  }
}