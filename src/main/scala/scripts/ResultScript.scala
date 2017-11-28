package scripts

import java.io.PrintWriter

import importExport.PathFormatter

/**
  * Created by marc on 04.09.17.
  */
object ResultScript {

  def count(tuples: Array[(String, Array[Double])], tuples1: Array[(String, Array[Double])], b: Boolean, equals : Boolean): (Array[Int],Boolean) = {
    assert(tuples.length==tuples1.length)
    var matches = new Array[Int](tuples.length)
    for(i <- 0 to tuples.length-1){
      matches(i) = getMatch(tuples(i), tuples1(i), b, equals)
    }
    return (matches,b)
  }

  def getMatch(tuples: (String, Array[Double]), tuples1: (String, Array[Double]), b: Boolean, equals : Boolean): Int = {
    if(equals){
      if (tuples._2.reduce(_ + _) > tuples1._2.reduce(_ + _)) {
         return 2
      } else {
        return 3
      }
    } else if (tuples._2.reduce(_ + _) > tuples1._2.reduce(_ + _)) {
        if (b) {
          return 1
        }
     } else if (!b) {
        return 1
     }
    return 0
  }

  def main(args: Array[String]): Unit = {
    val month = "_1" //"",2,1
    val settings = MetrikValidation.getScenarioSettings().slice(0,64)
    val colorMap = new Array[(Array[Int],Boolean)](settings.length)
    val detailMap = new Array[(Array[(String, Array[Double])],Array[(String, Array[Double])],Double,Double,(Double,Double),(Double,Double))](settings.length)
    val time = new Array[(Array[Double],Array[Double])](settings.length)
    for (i <- 0 to settings.length - 1) {
      val s = settings(i)
      s.focal = false
      var gStar = PathFormatter.getAllResultsFor(s)
      s.focal = true
      var focalGStar = PathFormatter.getAllResultsFor(s)
      detailMap(i) = (gStar.getMetrik(false), focalGStar.getMetrik(true), gStar.getMedian() , focalGStar.getMedian(), gStar.getMinMax(), focalGStar.getMinMax())
      colorMap(i) = count(detailMap(i)._1,detailMap(i)._2,detailMap(i)._3>detailMap(i)._4, detailMap(i)._3==detailMap(i)._4)
        //count(gStar.getMetrik(), focalGStar.getMetrik(), gStar.getMedian() > focalGStar.getMedian())

      time(i) = (gStar.getTime().map(x=> x._2),focalGStar.getTime().map(x=> x._2))
    }
    println("next")

    var writer = new PrintWriter("/home/marc/media/SS_17/output/server/evaluation/validationMedian"+month+".csv")
    var out = "median,medianFocal,min,max,maxFocal,minFocal \n"
    (detailMap.map(x => (x._3,x._4,x._5,x._6)).foreach(x => out += x._1+ ","+ x._2 +","+ x._3._1 +","+ x._3._2 +","+ x._4._1 +","+ x._4._2 + "\n"))
    writer.write(out)
    writer.flush()
    writer.close()
    writer = new PrintWriter("/home/marc/media/SS_17/output/server/evaluation/all"+month+".csv")
    out = "v \n"
    (colorMap.map(x => x._1.reduce(_ + _)).foreach(x => out += x + "\n"))
    writer.write(out)
    writer.flush()
    writer.close()
    for (i <- 0 to colorMap(0)._1.length-1) {
      writer = new PrintWriter("/home/marc/media/SS_17/output/server/evaluation/" + (i) +month+".csv")
      out = "predictedCondition,trueCondition\n"
      (colorMap.map(x => (x._1(i),x._2)).foreach(x => out += x._1+","+x._2 + "\n"))
      writer.write(out)
      writer.flush()
      writer.close()
    }

    for (i <- 0 to colorMap(0)._1.length-1) {
      writer = new PrintWriter("/home/marc/media/SS_17/output/server/evaluation/detail" + (i) + month+ ".csv")
      out = "gStar,focal,medianGstar,medianFocal\n"
      //(detailMap.map(x => (x._1(i),x._2(i),x._3,x._4)).zipWithIndex.foreach(x => out += x._1._1._2(x._2)+","+x._1._2._2(x._2)+","+x._1._3+","+x._1._4 + "\n"))

      detailMap.map(x=> (x._1(i),x._2(i),x._3,x._4)).foreach(x=> out += x._1._2.reduce(_+_)+","+x._2._2.reduce(_+_)+","+x._3+","+x._4+"\n")
      writer.write(out)
      writer.flush()
      writer.close()
    }

    writer = new PrintWriter("/home/marc/media/SS_17/output/server/evaluation/detailSetting" +month+".csv")
    out = settings(0).runToStringHead()+"\n"
    MetrikValidation.getScenarioSettings().filter(x=> x!=null).zipWithIndex.map(x => out += (x._2+1)+","+x._1.runToString() +"\n")
    writer.write(out)
    writer.flush()
    writer.close()


    for (i <- 0 to colorMap(0)._1.length-1) {
      writer = new PrintWriter("/home/marc/media/SS_17/output/server/evaluation/detailSetting" + (i) + month+".csv")
      out = settings(i).getHeader()+"\n"
      out += settings(i).runToString()
      writer.write(out)
      writer.flush()
      writer.close()
    }

    writer = new PrintWriter("/home/marc/media/SS_17/output/server/evaluation/time"+month+".csv")
    out = "gstar,focalgstar\n"
    (time.map(x => (x._1(0).toInt/1000,x._2(0).toInt/1000)).foreach(x => out += x._1+","+x._2 + "\n"))
    writer.write(out)
    writer.flush()
    writer.close()
  }


}
