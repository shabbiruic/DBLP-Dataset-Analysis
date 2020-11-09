import org.scalatest.funsuite.AnyFunSuite

import scala.xml.Elem

class XMLMapperTest extends AnyFunSuite {

  val sample1:Elem = <article mdate="2020-06-25" key="tr/meltdown/s18" publtype="informal">
    <author>Paul Kocher</author>
    <author>Daniel Genkin</author>
    <author>Daniel Gruss</author>
    <author>Yuval Yarom</author>
    <title>Spectre Attacks: Exploiting Speculative Execution.</title>
    <journal>meltdownattack.com</journal>
    <year>2018</year>
    <ee type="oa">https://spectreattack.com/spectre.pdf</ee>
  </article>


  val xmlMapper = new XMLMapper

  test("getAuthorOrEditorList should give sorted author list"){
        assertResult(Array("Daniel Genkin","Daniel Gruss","Paul Kocher","Yuval Yarom"))(xmlMapper.getAuthorOrEditorSortedList(sample1))
  }

  test("getAuthorCoAuthorList should generate all the possible sorted pairs of author coauthor")
  {
    val authorList = xmlMapper.getAuthorOrEditorSortedList(sample1)
    val authorCoauthorList = xmlMapper.getAuthorCoAuthorList(authorList)
    assertResult(6)(authorCoauthorList.length)
    assertResult(Array("Paul Kocher","Yuval Yarom"))(authorCoauthorList(5))
  }
}
