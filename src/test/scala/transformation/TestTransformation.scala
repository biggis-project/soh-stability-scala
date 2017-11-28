package transformation

import java.io.{File, PrintWriter}

import org.scalatest.FunSuite
import parmeters.Settings
import rasterTransformation.Transformation

/**
  * Created by marc on 07.06.17.
  */
class TestTransformation extends FunSuite{

  test("Test CSV Transformation"){
    val settings = new Settings()
    settings.latMin = 0
    settings.lonMin = 0
    settings.latMax = 100
    settings.lonMax = 100
    settings.sizeOfRasterLat = 10
    settings.sizeOfRasterLon = 10
    settings.multiToInt = 1
    settings.shiftToPostive = 0
    val fileName = "/tmp/test.csv"
    val testFile = new File(fileName)
    val writer = new PrintWriter(testFile)
    writer.println("Header")

    //Corners
    writer.println("99.0,99.0,2001-01-01 00:00:00")
    writer.println("0.00001,0.0001,2001-01-01 00:00:00")
    writer.println("99.99,99.99,2001-01-01 00:00:00")
    writer.println("0.0001,99.99,2001-01-01 00:00:00")
    writer.println("99.9,0.0001,2001-01-01 00:00:00")
    writer.println("0.0,0.0,2001-01-01 00:00:00")

    //Not in range
    writer.println("0.0,100.0,2001-01-01 00:00:00")
    writer.println("100.0,0.0,2001-01-01 00:00:00")
    writer.println("100.0,100.0,2001-01-01 00:00:00")

    //In Range
    writer.println("10.0,10.0,2001-01-01 00:00:00")

    writer.flush()
    writer.close()
    val transformation = new Transformation()
    val tile = transformation.transformCSVtoRasterParametrised(settings, fileName, 0,1,2)
    println(tile.asciiDrawDouble())
    assert(tile.toArrayDouble().reduce(_+_)==7)
  }


  test("Test CSV TransformationTime"){
    val settings = new Settings()
    settings.latMin = 0
    settings.lonMin = 0
    settings.latMax = 1000
    settings.lonMax = 1000
    settings.sizeOfRasterLat = 10
    settings.sizeOfRasterLon = 10
    settings.multiToInt = 1
    settings.shiftToPostive = 0
    val fileName = "/tmp/test.csv"
    val testFile = new File(fileName)
    val writer = new PrintWriter(testFile)
    writer.println("Header")


    for(i <- 0 to 23){
      //Corners
      writer.println("99.0,99.0,2001-01-01 "+i.formatted("%02d")+":00:00")
      writer.println("0.00001,0.0001,2001-01-01 "+i.formatted("%02d")+":00:00")
      writer.println("99.99,99.99,2001-01-01 "+i.formatted("%02d")+":00:00")
      writer.println("0.0001,99.99,2001-01-01 "+i.formatted("%02d")+":00:00")
      writer.println("99.9,0.0001,2001-01-01 "+i.formatted("%02d")+":00:00")
      writer.println("0.0,0.0,2001-01-01 "+i.formatted("%02d")+":00:00")
    }


    writer.flush()
    writer.close()
    val transformation = new Transformation()
    val tile = transformation.transformCSVtoTimeRasterParametrised(settings, fileName, 0,1,2)

//    For Debug
//    for(i <- 0 to 23){
//      println(tile.band(i).asciiDrawDouble())
//    }
    for(i <- 0 to 23){
      if(!(tile.band(i).toArrayDouble().reduce(_+_)==6)){
        println("Not working at index: "+i)
      }
      assert(tile.band(i).toArrayDouble().reduce(_+_)==6)
    }

  }

  test("Test 2 CSV Transformation"){
    val settings = new Settings()
    settings.latMin = 0
    settings.lonMin = 0
    settings.latMax = 50
    settings.lonMax = 100
    settings.sizeOfRasterLat = 5
    settings.sizeOfRasterLon = 10
    settings.multiToInt = 1
    settings.shiftToPostive = 0
    val fileName = "/tmp/test.csv"
    val testFile = new File(fileName)
    val writer = new PrintWriter(testFile)
    writer.println("Header")

    //Corners
    writer.println("99.0,49.9,2001-01-01 00:00:00")
    writer.println("0.00001,0.0001,2001-01-01 00:00:00")
    writer.println("99.99,49.9,2001-01-01 00:00:00")
    writer.println("0.0001,49.99,2001-01-01 00:00:00")
    writer.println("99.9,0.0001,2001-01-01 00:00:00")
    writer.println("0.0,0.0,2001-01-01 00:00:00")

    //Not in range
    writer.println("0.0,50.0,2001-01-01 00:00:00")
    writer.println("100.0,0.0,2001-01-01 00:00:00")
    writer.println("100.0,50.0,2001-01-01 00:00:00")

    //In Range
    writer.println("10.0,5.0,2001-01-01 00:00:00")

    writer.flush()
    writer.close()
    val transformation = new Transformation()
    val tile = transformation.transformCSVtoRasterParametrised(settings, fileName, 0,1,2)
    println(tile.asciiDrawDouble())
    assert(tile.toArrayDouble().reduce(_+_)==7)
  }

  test("Test 3 CSV Transformation"){
    val settings = new Settings()
    settings.latMin = 0
    settings.lonMin = 0
    settings.latMax = 50
    settings.lonMax = 100
    settings.sizeOfRasterLat = 5
    settings.sizeOfRasterLon = 10
    settings.multiToInt = 1
    settings.shiftToPostive = 100
    val fileName = "/tmp/test.csv"
    val testFile = new File(fileName)
    val writer = new PrintWriter(testFile)
    writer.println("Header")

    //Corners
    writer.println("-99.0,49.9,2001-01-01 00:00:00")
    writer.println("-0.00001,0.0001,2001-01-01 00:00:00")
    writer.println("-99.99,49.9,2001-01-01 00:00:00")
    writer.println("-0.0001,49.99,2001-01-01 00:00:00")
    writer.println("-99.9,0.0001,2001-01-01 00:00:00")
    writer.println("-0.0,0.0,2001-01-01 00:00:00")

    //Not in range
    writer.println("-0.0,50.0,2001-01-01 00:00:00")
    writer.println("-100.0,0.0,2001-01-01 00:00:00")
    writer.println("-100.0,50.0,2001-01-01 00:00:00")

    //In Range
    writer.println("-10.1,5.1,2001-01-01 00:00:00")

    writer.flush()
    writer.close()
    val transformation = new Transformation()
    //settings : Settings, fileName : String, indexLon : Int, indexLat : Int, indexDate : Int
    val tile = transformation.transformCSVtoRasterParametrised(settings, fileName, 0,1,2)
    println(tile.asciiDrawDouble())
    assert(tile.toArrayDouble().reduce(_+_)==7)
  }

}
