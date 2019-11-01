import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration
import org.apache.spark.sql.types._
    object test1 {

      case class cdrsms(col1: String , col2: String , col3: String , col4: String ,
                       col5: String , col6: String , col7: String , col8: String,
                       col9: String ,  col10: String)


      case class cdrcalls(col1: String , col2: String , col3: String , col4: String ,
                        col5: String , col6: String , col7: String , col8: String,
                        col9: String ,  col10: String , col11: String ,col12 : String , col13 : String)

      def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("hwtest").setMaster("local[*]")
        val topicsSet = "hwkf01".split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.2.210:9092")
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(20))
        val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet)
        val lines = messages.map(_._2)
        val fil_sms = lines.filter(_.startsWith("1|"))
        val fil_calls = lines.filter(_.startsWith("7|"))
       val sqlContext = new HiveContext(sc)
        import sqlContext.implicits._

        fil_sms.foreachRDD(rdd=> if(!rdd.isEmpty){
          //val sms = rdd.filter(_.startsWith("1|"))
          rdd.map(_.split('|')).map(p => cdrsms(p(0), p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9))).toDF().registerTempTable("cdr_sms")
          val tbl1 = sqlContext.sql("SELECT * FROM cdr_sms")
          tbl1.foreach(println)
          sqlContext.sql("insert into table sms select * from cdr_data")
        })

        fil_calls.foreachRDD(rdd=> if(!rdd.isEmpty){
          rdd.map(_.split('|')).map(p => cdrcalls(p(0), p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12))).toDF().registerTempTable("cdr_calls")
          val tbl1 = sqlContext.sql("SELECT * FROM cdr_calls")
          tbl1.foreach(println)

          sqlContext.sql("insert into table calls select * from cdr_data")
        })
        ssc.start()
        ssc.awaitTermination()
      }
    }
	
	#######################################################
	import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration
import org.apache.spark.sql.types._

object test1 {

  case class cdrsms(col1: String , col2: String , col3: String , col4: String ,
                   col5: String , col6: String , col7: String , col8: String,
                   col9: String ,  col10: String)


  case class cdrcalls(col1: String , col2: String , col3: String , col4: String ,
                    col5: String , col6: String , col7: String , col8: String,
                    col9: String ,  col10: String , col11: String ,col12 : String , col13 : String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("hwtest").setMaster("local[*]")
    val topicsSet = "hwkf01".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.2.210:9092")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)

   val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    lines.foreachRDD(rdd=> if(!rdd.isEmpty){
      val sms = rdd.filter(_.startsWith("7|"))
      val calls = rdd.filter(_.startsWith("1|"))

      sms.map(_.split('|'))
        .map(p => cdrsms(p(0), p(1),p(2), p(3),p(4),p(5),p(6),p(7),p(8),p(9)))
        .toDF()
        .write.mode("append")
        .insertInto("sms_cdr")

      calls.map(_.split('|'))
        .map(p => cdrcalls(p(0), p(1),p(2), p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12)))
        .toDF()
        .write.mode("append")
        .insertInto("calls_cdr")

   })


    ssc.start()
    ssc.awaitTermination()
  }
}