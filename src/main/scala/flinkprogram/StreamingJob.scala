/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flinkprogram

import java.util.concurrent.TimeUnit

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //env.registerCachedFile(System.getProperty("user.dir"),"eventStream.txt", false)
    //env.registerCachedFile(System.getProperty("user.dir"),"eventStream_tw.txt", false)
    //env.registerCachedFile(System.getProperty("user.dir"),"eventStream_query.txt", false)

    // Read Input File with Events from SASE
    val text = env.readTextFile(System.getProperty("user.dir")+"/eventStream.txt")
    val text_tw = env.readTextFile(System.getProperty("user.dir")+"/eventStream_tw.txt")
    val text_q3 = env.readTextFile(System.getProperty("user.dir")+"/eventStream_query.txt")

    // Convert file content to StockEvents
    val events = text.flatMap { str => str.split("\n") } //split to lines
      .map(str => str.replaceAll("\t", ",").replaceAll("[^0-9,]", "")) //extract numbers separated by commas
      .map(str => str.split(",")) //convert each line to string array (each string is a number)
      .map(e => StockEvent(e(0).toInt, e(1).toLong, e(2).toInt, e(3).toInt, e(4).toInt)) //convert each line to a StockEvent


    // JUST FOR DEBUG!
    // events.map(e => e.toString).print() //print events to stdout

    val events_tw = text_tw.flatMap { str => str.split("\n") } //split to lines
      .map(str => str.replaceAll("\t", ",").replaceAll("[^0-9,]", "")) //extract numbers separated by commas
      .map(str => str.split(",")) //convert each line to string array (each string is a number)
      .map(e => StockEvent(e(0).toInt, e(1).toLong, e(2).toInt, e(3).toInt, e(4).toInt)) //convert each line to a StockEvent

    val events_query = text_q3.flatMap { str => str.split("\n") } //split to lines
      .map(str => str.replaceAll("\t", ",").replaceAll("[^0-9,]", "")) //extract numbers separated by commas
      .map(str => str.split(",")) //convert each line to string array (each string is a number)
      .map(e => StockEvent(e(0).toInt, e(1).toLong, e(2).toInt, e(3).toInt, e(4).toInt)) //convert each line to a StockEvent


    // Create the event stream
    val eventStream: DataStream[StockEvent] = events.assignTimestampsAndWatermarks(new StockTimeAssigner) //assign timestamps extracted by each event
    val eventStream_tw: DataStream[StockEvent] = events_tw.assignTimestampsAndWatermarks(new StockTimeAssigner)
    val eventStream_q3: DataStream[StockEvent] = events_query.assignTimestampsAndWatermarks(new StockTimeAssigner)

    // Partition input eventStream by symbol
    val partBySym = eventStream.keyBy(e => e.symbol)
    val partBySym_q3 = eventStream_q3.keyBy(e => e.volume)

    // Partition input eventStream by price
    val partByPr = eventStream.keyBy(e => e.price)
    val partByPr_tw = eventStream_tw.keyBy(e => e.price)
    val partByPr_q3 = eventStream_q3.keyBy(e => e.price)

    // Create the patterns to be detected

    // Simple Select Function for two events
    def simpleSelect2(pattern: Map[String, Iterable[StockEvent]]): (StockEvent,StockEvent) ={
      val startEvent = pattern.get("a").get.head
      val nextEvent = pattern.get("b").get.head
      (startEvent,nextEvent)
    }

    // A more complex select function, used for kleene patterns
    def complexSelect(pattern: Map[String, Iterable[StockEvent]]): (Iterable[StockEvent],StockEvent) ={
      val startEvent = pattern.get("a").get
      val nextEvent = pattern.get("b").get.head
      (startEvent,nextEvent)
    }

    def complexSelect3(pattern: Map[String, Iterable[StockEvent]]): Iterable[StockEvent] ={
      val startEvent = pattern.get("a").get
      startEvent
    }

    // Simple Select Function for three events
    def simpleSelect3(pattern: Map[String, Iterable[StockEvent]]): (StockEvent,StockEvent,StockEvent) ={
      val startEvent = pattern.get("a").get.head
      val nextEvent = pattern.get("b").get.head
      val endEvent = pattern.get("c").get.head
      (startEvent,nextEvent,endEvent)
    }

    // Define After Match Skip Strategy
    val skipStrategy= AfterMatchSkipStrategy.noSkip()

    // QUERY 1
    val query1_pc = Pattern.begin[StockEvent]("a",skipStrategy)
      .where(_.price % 500 == 0)
      .next("b") // query 1 with partition-contiguity ESS
      .where((ev,ctx)=> ev.volume < ctx.getEventsForPattern("a").head.volume )
      .within(Time.milliseconds(1001)) // within 1 sec

    val query1_stnm = Pattern.begin[StockEvent]("a",skipStrategy)
      .where(_.price % 500 == 0)
      .followedBy("b") // query 1 with skip-till-next-match ESS
      .where((ev,ctx)=> ev.volume < ctx.getEventsForPattern("a").head.volume )
      .within(Time.milliseconds(1001)) // within 1 sec

    val query1_stam = Pattern.begin[StockEvent]("a",skipStrategy)
      .where(_.price % 500 == 0)
      .followedByAny("b") // query 1 with skip-till-any-match ESS
      .where((ev,ctx)=> ev.volume < ctx.getEventsForPattern("a").head.volume )
      .within(Time.milliseconds(1001)) // within 1 sec

    val query1 = query1_stam

    val patternStreamQ1 = CEP.pattern(partBySym, query1)

    val matchesQ1:DataStream[(StockEvent,StockEvent)] = patternStreamQ1.select(event => simpleSelect2(event))

    matchesQ1.print()

    val cnts: DataStream[Int] = matchesQ1
      .map(element => 1).timeWindowAll(Time.seconds(100)).sum(0)

     cnts.print()

  // QUERY 2
    val T=100000
    val query2_stam = Pattern.begin[StockEvent]("a",skipStrategy)
      .where(_.symbol==1)
      .followedByAny("b")
      .where(_.symbol==2)
      .followedByAny("c")
      .where(_.symbol==3)
      .within(Time.milliseconds(T))

    val patternStreamQ2 = CEP.pattern(partByPr_tw, query2_stam)

    val matchesQ2: DataStream[(StockEvent, StockEvent, StockEvent)] = patternStreamQ2.select(event => simpleSelect3(event))

    matchesQ2.print()

    val cnts2: DataStream[Int] = matchesQ2
      .map(element => 1).timeWindowAll(Time.seconds(1000)).sum(0)

    cnts2.print()

  // QUERY 3
    val query3_pc = Pattern.begin[StockEvent]("a",skipStrategy)
      .next("b")
      .where( (ev,ctx) => ev.volume > ctx.getEventsForPattern("a").head.volume)
      .within(Time.milliseconds(2001))

    val query3_stnm = Pattern.begin[StockEvent]("a",skipStrategy)
      .followedBy("b")
      .where( (ev,ctx) => ev.volume > ctx.getEventsForPattern("a").head.volume)
      .within(Time.milliseconds(101))

    val query3 = query3_stnm

    val patternStreamQ3 = CEP.pattern(partByPr_q3, query3)

    val matchesQ3: DataStream[(StockEvent, StockEvent)] = patternStreamQ3.select( event => simpleSelect2(event))

    matchesQ3.print()

    val cnts3: DataStream[Int] = matchesQ3
      .map(element => 1).timeWindowAll(Time.seconds(10000)).sum(0)
    cnts3.print()

  // QUERY 4
    // best strategy for query 4 is skipToNext() !!!
    val s = AfterMatchSkipStrategy.skipToNext()

    // kleene 3
    val query4_pc = Pattern.begin[StockEvent]("a",s)
      .oneOrMore.consecutive()
      .where( (ev,ctx) =>{
        var ok=false
        if(ctx.getEventsForPattern("a").nonEmpty== false){
          ok = ev.price%200==0
        }else {
          ok = ev.price > ctx.getEventsForPattern("a").last.price
        }
        ok
      })
      .next("b")
      .where(e => e.volume < 150 && e.price > 950)
      .within(Time.milliseconds(501))


    // kleene 1 2 4 (with different time window)
    val query4_stnm = Pattern.begin[StockEvent]("a",s)
      .oneOrMore
      .where( (ev,ctx) =>{
        var ok=false
        if(ctx.getEventsForPattern("a").nonEmpty== false){
          ok = ev.price%200==0
        }else {
          ok = ev.price > ctx.getEventsForPattern("a").last.price
        }
        ok
      })
      .followedBy("b")
      .where(e => e.volume < 150 && e.price > 950)
      .within(Time.milliseconds(5001)) // TW = {501, 101, 5001}

    // kleene 5 6 7 (with different aggregations)
    val query4_agg = Pattern.begin[StockEvent]("a",s)
      .oneOrMore
      .where( (ev,ctx) =>{
        var ok=false
        if(ctx.getEventsForPattern("a").nonEmpty== false){
          ok = ev.price%200==0
        }else {
          //for avg
          lazy val len = ctx.getEventsForPattern("a").count(x=>true)
          ok = ev.price > ctx.getEventsForPattern("a").map(_.price).sum/len

          //for min and max
          //ok = ev.price > ctx.getEventsForPattern("a").map(_.price).min //.min or .max
        }
        ok
      })
      .followedBy("b")
      .where(e => e.volume < 150 && e.price > 950)
      .within(Time.milliseconds(501))

    val query4 = query4_agg

    val patternStreamQ4 = CEP.pattern(partBySym, query4)

    val matchesQ4: DataStream[(Iterable[StockEvent], StockEvent)] = patternStreamQ4.select( event => complexSelect(event))

    matchesQ4.print()

    val cnts4: DataStream[Int] = matchesQ4
      .map(element => 1).timeWindowAll(Time.seconds(10000)).sum(0)

    cnts4.print()


  // Additional Query
    val query_add = Pattern.begin[StockEvent]("a",s)
      .oneOrMore.greedy
      .where( (ev,ctx) => {
        var ok = false
        if(ctx.getEventsForPattern("a").nonEmpty) {
          ok = ctx.getEventsForPattern("a").last.volume > ev.volume
        }else{
          ok = ev.price < 500
        }
        ok
      })
      .followedByAny("b")
      .where(ev => ev.volume > 150)
      .within(Time.milliseconds(2000))

    val patternStreamQAdd = CEP.pattern(partBySym_q3, query_add)

    val matchesQ5: DataStream[(Iterable[StockEvent], StockEvent)] = patternStreamQAdd.select( event => complexSelect(event))

    matchesQ5.print()

    val cnts5: DataStream[Int] = matchesQ5
      .map(element => 1).timeWindowAll(Time.seconds(10000)).sum(0)

    cnts5.print()

    //println(env.getExecutionPlan)  //json for visualizing the execution plan
    val result = env.execute("CEP with Flink")
    println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " to execute");

  }
}
