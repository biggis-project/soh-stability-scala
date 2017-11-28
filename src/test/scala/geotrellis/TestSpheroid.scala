package geotrellis

import org.scalatest.FunSuite

/**
  * Created by marc on 11.07.17.
  */
class TestSpheroid extends FunSuite{
  ignore("sum and volume"){
    val spheroid = new Spheroid(100,100)
    println(SpheroidHelper.getVolume(spheroid))
    println(spheroid.getSum())
  }

  test("IsInRange"){
    val spheroid = new Spheroid(2,2)
    assert(false==spheroid.isInRange(-2,-1,-1))
    assert(false==spheroid.isInRange(-2,-1,0))
    assert(spheroid.isInRange(-2,0,0))
  }

}
