package export

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.scalatest.FunSuite
import parmeters.Settings

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 31.05.17.
  */
class ResultExport extends FunSuite {

  test("Test time parsing"){
    val formatter = DateTimeFormatter.ofPattern("dd_MM")
    assert(!LocalDateTime.now().format(formatter).equals("dd_MM"))
  }

}
