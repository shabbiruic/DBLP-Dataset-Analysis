package Utils

import org.scalatest.funsuite.AnyFunSuite

class MyConfigTest extends AnyFunSuite{

  val myConfig = MyConfig

  test("getString should get the valid value for key")
  {
    assertResult("TopPublishAuthors")(myConfig.getString("resultKeys.topPublishAuthor"))
  }

  test("should throw ConfigKeyException for wrong type")
  {
    assertThrows[ConfigKeyException](myConfig.getInt("resultKeys.maxAuthorPublicationTitlePrefix"))
  }

}
