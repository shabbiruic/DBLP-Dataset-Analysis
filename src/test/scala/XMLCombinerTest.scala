import org.scalatest.funsuite.AnyFunSuite

import scala.xml.Elem

class XMLCombinerTest extends AnyFunSuite {

  test("getYearEncoding") {
    val xmlCombiner = new XMLCombiner
    val years = Set(2019,2012,2018,2017,2016)
    assertResult("_111")(xmlCombiner.getYearEncoding(years))
  }
}
