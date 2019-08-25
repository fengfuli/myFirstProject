package streaming.customSourceCount

import org.apache.flink.streaming.api.functions.source.SourceFunction

class customSource  extends SourceFunction[Long]{

  var count=1L
  var inRunning=true
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

       while (inRunning){

         ctx.collect(count)

         count+=1

       Thread.sleep(1000)
       }
  }

  override def cancel(): Unit = {
    inRunning=false


  }


}
