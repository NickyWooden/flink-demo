

package com.ly.flink

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.ly.flink.consts.CaseClass.{CrimeDoc, Phone, Warn}
import com.ly.flink.job.sources.PostgreSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

import scala.collection.mutable
/**
 *author : liyuan
 *time : 2019/9/19 19:18
 *desc : {kafka实时4G基站数据 与关系数据库中重点人库数据关联预警 ，采取广播状态方式}
**/
object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    /*
      postgre FZ档案库数据
     */
    val crime = env.addSource(new PostgreSource)
      .map(c =>mutable.HashMap(c.num -> c))
    val stateDescriptor =  new MapStateDescriptor[String,mutable.HashMap[String,CrimeDoc]](
      "crime-state",
      classOf[String],
      //hashMap检索较快
      classOf[mutable.HashMap[String,CrimeDoc]])
    val crimeState = crime.broadcast(stateDescriptor)
    /*
      kafka基站实时数据
     */
    val stream = env
      .addSource(createKafkaSource())
      .map(line => {
        var phone: Phone = Phone("None", "", 0.0f, 0.0f)
        try {
          phone = JSON.parseObject(line, classOf[Phone])
        } catch {
          case e: Exception => println("jsonSerializer Exception!" + e.getCause)
        }
        phone
      })
      .filter(_.num != "None")
        .keyBy(_.num)
        .timeWindow(Time.seconds(10))
        .reduce((p1,p2)=>p1)
        .connect(crimeState)
        .process(broadStateProcessFunction(stateDescriptor))
        //预警成功记录过滤
        .filter(_.num!="None")
        .map(w =>JSON.toJSONString(w,true)).setParallelism(10)

    stream.print()
    val myProducer = new FlinkKafkaProducer[String](
      "192.168.1.110:9092",         // broker list
      "warn-crime",               // target topic
      new SimpleStringSchema)   // serialization schema

    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
    myProducer.setWriteTimestampToKafka(true)
    stream.addSink(myProducer)

    // execute program
    env.execute("warn-crime")
  }

  /**
   * author : liyuan
   * time : 2019/9/19 17:30
   * desc : {创建Kafka 数据源}
   **/
  def createKafkaSource() = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.110:9092")
    properties.setProperty("group.id", "flink-group")
    val kafka = new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties)
    kafka.setStartFromEarliest()
    kafka
  }
  /**
   *author : liyuan
   *time : 2019/9/19 19:03
   *desc : { 广播状态处理函数}
  **/
  def broadStateProcessFunction(descipter: MapStateDescriptor[String,mutable.HashMap[String,CrimeDoc]]) ={
    val keyedBroadProcess = new KeyedBroadcastProcessFunction[String,Phone,mutable.HashMap[String,CrimeDoc],Warn] {
      /*
        与广播状态合并处理
       */
      override def processElement(in1: Phone,
                                  readOnlyContext: KeyedBroadcastProcessFunction[String, Phone, mutable.HashMap[String,CrimeDoc], Warn]#ReadOnlyContext,
                                  collector: Collector[Warn]): Unit = {
        val crimeMap: mutable.HashMap[String, CrimeDoc] = readOnlyContext.getBroadcastState(descipter).get("crime")
        val doc = crimeMap.getOrElse(in1.num,CrimeDoc(-1,"None","","",""))
        if("None" != doc){
          collector.collect(Warn(in1.num,doc.name,"2019-09-20",doc.detail))
        }else{
          collector.collect(Warn("None","","",""))
        }
      }
      /*
        读取广播状态
       */
      override def processBroadcastElement(in2: mutable.HashMap[String,CrimeDoc],
                                           context: KeyedBroadcastProcessFunction[String, Phone, mutable.HashMap[String,CrimeDoc], Warn]#Context,
                                           collector: Collector[Warn]): Unit = {
        context.getBroadcastState(descipter).put("crime",in2)
      }
    }
    keyedBroadProcess
  }
}
