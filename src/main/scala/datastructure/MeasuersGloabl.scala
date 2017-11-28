package datastructure

/**
  * Created by marc on 14.08.17.
  */
class MeasuersGloabl(){
  var statsTime = 0.0
  var allTime = 0.0
  var RoW = 0.0
  var MW = 0.0
  var NW2 = 0.0
  var W2 = 0.0
  var denominator = 0.0
  var devision = 0.0
  var writing = 0.0
  var broadCast = 0.0
  var collect = 0.0
  var stitch = 0.0

  def setcollect(time : Long): Unit ={
    collect = time
  }

  def setStitch(time : Long): Unit ={
    stitch = time
  }



  def setWriting(time : Long): Unit ={
    writing = time
  }

  def setStats(time : Long): Unit ={
    statsTime = time
  }

  def setAllTime(time : Long): Unit ={
    allTime = time
  }

  def setRoW(time : Long): Unit ={
    RoW = time
  }

  def setMW(time : Long): Unit ={
    MW = time
  }

  def setNW2(time : Long): Unit ={
    NW2 = time
  }

  def setW2(time : Long): Unit ={
    W2 = time
  }

  def setBroadCast(time : Long): Unit ={
    broadCast = time
  }

  def setDenominator(time : Long): Unit ={
    denominator = time
  }

  def setDevision(time : Long): Unit ={
    devision = time
  }

  def getPerformanceMetrik(): String ={
    val out = "Time for G* was:"+allTime+
      "\n time for stats:"+statsTime+
      "\n time for RoW:"+RoW+
      "\n time for MW:"+MW+
      "\n time for NW2:"+NW2+
      "\n time for W2:"+W2+
      "\n time for Denominator:"+denominator+
      "\n time for Devision:"+devision+
      "\n time for Writing:"+writing+
      "\n time for Broadcast:"+broadCast+
      "\n time for collect:"+collect+
      "\n time for stitch:"+stitch
    out
  }
}
