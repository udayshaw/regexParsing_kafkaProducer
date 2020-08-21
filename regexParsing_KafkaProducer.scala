import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ KafkaUtils }
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import scala.util.parsing.json.JSON
import scala.util.{ Try, Success, Failure }
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._
import org.json4s.jackson.Serialization.write
import java.util.Date
import java.text.SimpleDateFormat
import java.time.{ Instant, ZoneId, ZonedDateTime }
import java.util.Calendar
import org.apache.spark.streaming.dstream.DStream
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object Parsing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.streaming.kafka.consumer.cache.enabled", "false").set("spark.sql.autoBroadcastJoinThreshold","-1")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val conf_data = JsonParser.parseConfigJson(spark, args(0))

    val regex=conf_data("regex").asInstanceOf[String]
    val url = conf_data("url").asInstanceOf[String]
    val username = conf_data("username").asInstanceOf[String]
    val password = conf_data("password").asInstanceOf[String]
    val driver= conf_data("driver").asInstanceOf[String]
    val bootstrap_servers = conf_data("bootstrap_servers").asInstanceOf[String]
    val zookeeper_connect = conf_data("zookeeper_connect").asInstanceOf[String]
    val oup_id = conf_data("oup_id").asInstanceOf[String]
    val zkpr_conn_timeout_ms = conf_data("zkpr_conn_timeout_ms").asInstanceOf[String]
    val topicsSet = conf_data("topic").asInstanceOf[String].split(",").toSet
    val autoCommit = conf_data("autoCommitFlag").asInstanceOf[String].toBoolean
    val noOfConcurrentSparkJobs = Try { spark.sparkContext.getConf.get("spark.streaming.concurrentJobs").toInt }.getOrElse(1)
    val StreamingInterval=conf_data("StreamingInterval").asInstanceOf[String].toInt

    val ssc = new StreamingContext(spark.sparkContext, Seconds(StreamingInterval))
    val df_ngo_app_regex=spark.read.format("jdbc").option("url", url).option("dbtable", ngo_app_regex_table).option("user", username).option("password", password).option("driver", driver).load.select($"patternfilter",$"PATTERNCAPTUREINPERL",$"FIELDNAMES").distinct
    broadcast(df_ngo_app_regex)
    
    val dstream = ReadInput.readInput(2, spark, ssc, bootstrap_servers, zookeeper_connect, oup_id, autoCommit, zkpr_conn_timeout_ms, topicsSet).asInstanceOf[DStream[String]]
    
   dstream.foreachRDD(rdd =>{
     val df_raw = rdd.toDF("value")
     val df_filtered=df_raw.select($"value",get_json_object($"value", "$.message").alias("message")).filter($"message".rlike(regex)).repartition(100)
     val df_write=df_filtered.crossJoin(df_ngo_app_regex)
     
val props:Properties = new Properties()
props.put("bootstrap.servers",bootstrap_servers)
props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
props.put("acks","all")
val topic = "regex_test"

df_write.foreachPartition(partition => {
    val producer = new KafkaProducer[String, String](props)
       val batchSize = 50000
       partition.grouped(batchSize).foreach(batch=> {
         batch.par.foreach { row =>
            var s: String="{"
            val messageIndex  = row.fieldIndex("message")
            val message = row.getString(messageIndex)
            val PATTERNCAPTUREINPERLIndex  = row.fieldIndex("PATTERNCAPTUREINPERL")
            val PATTERNCAPTUREINPERL = row.getString(PATTERNCAPTUREINPERLIndex).r
            val FIELDNAMESIndex  = row.fieldIndex("FIELDNAMES")
            val FIELDNAMES = row.getString(FIELDNAMESIndex).split(",").toList
            val patternfilterIndex  = row.fieldIndex("patternfilter")
            val patternfilter = row.getString(patternfilterIndex).r
            if(patternfilter.pattern.matcher(message).matches){
            val outList=PATTERNCAPTUREINPERL.unapplySeq(message)
            //printstr.print("outlist"+outList.get(0));
            if(outList!=None){
                for(i<-0 to FIELDNAMES.size-1){val outvalue= try { outList.get(i).asInstanceOf[String] } catch { case ex: Exception => {""}}; s+="\""+FIELDNAMES(i).asInstanceOf[String]+"\":\""+ outvalue+"\","}
                s=s.substring(0,s.length-1)+"}"
                //val out=outList.toList
                //s=scala.util.parsing.json.JSONObject(FIELDNAMES.zip(out(0)).toMap)
                val record = new ProducerRecord[String, String](topic, s.toString, s.toString)
                producer.send(record)
            }  //else { s=s+"no data}"}
          //printstr.print(s);
         }}
      })
      producer.close()
})

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
