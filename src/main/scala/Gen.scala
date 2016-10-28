/**
  * Created by lizbai on 27/10/16.
  */


import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory
import org.apache.spark.mllib.random.RandomRDDs._

//case class Record(ColA:Int, ColB:Int)

object Gen {

  def main(args:Array[String])={
    val conf = new SparkConf().setAppName("generating raw dataset")
    val sc = new SparkContext(conf)
    val vecRDD = normalVectorRDD(sc, 5000000000L,2,10)
    vecRDD.map(a => a(0).toInt+" "+a(1).toInt).saveAsTextFile(args(0))
  }

}
