package geotrellis

import clustering.ClusterHotSpotsTime
import geotrellis.raster.MultibandTile
import geotrellis.raster.mapalgebra.focal.Square
import timeUtils.MultibandUtils

/**
  * Created by marc on 03.07.17.
  */
trait TimeNeighbourhood {
  val a : Int
  val c : Int
  def isInRange(x:Double,y:Double,z:Double): Boolean
  def getSum(): Int
  def countInRange(clusterId: Int, mbT: MultibandTile, startBand: Int, startRows: Int, startCols: Int): (Int,Int,Int,Int) = {
    var sum = 0
    for(x <- -a to a){
      for(y <- -a to a){
        for(z <- -c to c){
          var b = (startBand+z) % 24
          if(b<0){
            b+=24
          }
          if(isInRange(x,y,z)
            && MultibandUtils.isInTile(startRows+x,startCols+y,mbT)
            && mbT.band(b).get(startRows+x,startCols+y)==clusterId){
            sum += 1
          }
        }
      }
    }

    (sum,(startBand+24)%24,startRows,startCols)
  }

  def clusterPercent(clusterId : Int, mbT : MultibandTile, startBand : Int, startRows : Int, startCols : Int, max : Int): Double ={
    var array = new Array[(Int,Int,Int,Int)](27)
    var counter = 0
    for(i <- -1 to 1){
      for(j<- -1 to 1){
        for(k <- -1 to 1){
          array(counter) = countInRange(clusterId,mbT,startBand+i,startRows+j,startCols+k)
          counter+=1
        }
      }
    }
    val max = array.map(x=>x._1).max
    val maxKey = array.filter(x=>x._1==max).head
    if(maxKey._2==startBand && maxKey._3==startRows && maxKey._4==startCols || max==maxKey._1){
      return maxKey._1/SpheroidHelper.getVolume(this)
    }
    clusterPercent(clusterId,mbT,maxKey._2,maxKey._3,maxKey._4, maxKey._1)
  }
}

case class Cube(a:Int,c:Int) extends TimeNeighbourhood {

  override def isInRange(x:Double,y:Double,z:Double): Boolean = {
    (x<=a && y<=a && z<=a)
  }

  override def getSum(): Int = {
    a*a*a
  }

}

case class Spheroid(a:Int,c:Int) extends TimeNeighbourhood {
  //https://en.wikipedia.org/wiki/Spheroid


  override def isInRange(x:Double,y:Double,z:Double): Boolean = {
    val tmp1 = (x*x+y*y)/(a*a)
    val tmp2 = (z*z)/(c*c)
    val tmp = tmp1+tmp2
    tmp<=1
  }

  def getSquare(z:Int): Square ={
    assert(z<c)
    //x==y => equation to x
    val r = Math.sqrt(((c*c*a*a)/(z*z))/2).ceil.toInt
    return Square(r)
  }

  override def getSum(): Int ={
    var sum = 0
    for(x <- -a to a){
      for(y <- -a to a){
        for(z <- -c to c){
          if(isInRange(x,y,z)){
            //println(x+","+y+","+z)
            sum += 1
          }
        }
      }
    }
    sum
  }


}



object SpheroidHelper{

  def getSpheroidWithSum(sum : Double, z : Double): Spheroid ={
    //Volume equation
    new Spheroid(Math.sqrt(3/(Math.PI*4)*sum/(z+0.5)).toInt,z.toInt)

  }

  //Nearly same as some for smaller a,c bigger % gap
  def getVolume(timeNeighbourhood: TimeNeighbourhood): Double ={
    if(timeNeighbourhood.isInstanceOf[Spheroid]){
      getVolume(timeNeighbourhood)
    } else {
      timeNeighbourhood.getSum()
    }

  }

  //Nearly same as some for smaller a,c bigger % gap
  def getVolume(spheroid: Spheroid): Double ={
    (4*Math.PI/3)*Math.pow(spheroid.a+0.5,2)*(spheroid.c+0.5)
  }
}