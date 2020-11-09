import java.lang

import Utils.MyConfig
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer

class XMLReducer extends Reducer[Text,Text,Text,Text] {

//  fetching the required key prefixes which is need to get the key type.
  val venueAuthorCountPrefix:String = MyConfig.getString("keyPrefixes.venueAuthorCount")
  val authorPublishedYearPrefix:String = MyConfig.getString("keyPrefixes.authorPublishedYear")
  val venuePublicationAuthorCountsPrefix:String = MyConfig.getString("keyPrefixes.venuePublicationAuthorCounts")
  val authorAndItsCoAuthorCountPrefix:String = MyConfig.getString("keyPrefixes.authorAndItsCoAuthorCount")

  val topPublishAuthor:String = MyConfig.getString("resultKeys.topPublishAuthor")
  val authorWithMostCoAuthors:String = MyConfig.getString("resultKeys.authorWithMostCoAuthors")
  val authorWithLeastCoAuthors:String = MyConfig.getString("resultKeys.authorWithLeastCoAuthors")
  val authorPublishedContinuallyKey:String = MyConfig.getString("resultKeys.authorPublishedContinually")
  val leastAuthorPublicationTitlePrefix:String= MyConfig.getString("resultKeys.leastAuthorPublicationTitlePrefix")
  val maxAuthorPublicationTitlePrefix:String =MyConfig.getString("resultKeys.maxAuthorPublicationTitlePrefix")

//  fetching the key value for output keys from config
    val numberOfContinuousYears:Int = MyConfig.getInt("statisticsParameters.numberOfContinuousYears")
    val numberTopAuthorsAtEachVenue:Int = MyConfig.getInt("statisticsParameters.numberTopAuthorsAtEachVenue")
    val numberOfTopAuthorsForCoAuthorCount:Int = MyConfig.getInt("statisticsParameters.numberOfTopAuthorsForCoAuthorCount")
    val numberOfButtomAuthorsForCoAuthorCount:Int = MyConfig.getInt("statisticsParameters.numberOfButtomAuthorsForCoAuthorCount")


  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * function which reduces the key and values from the combiner into the final resultant key value pair
   * @param key  gets all the different types of key generated bu combiner
   * @param values  gets all the values for specific key
   * @param context
   */
  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    //    fetch the key prefix to decide what kind of reduce operation has to be done for this
    //    key.
    val keyType:String = key.toString.split("#")(0)

    logger.info("reduce function is called for key: "+ keyType)

//    calling the required reduce operation depending on the key type
    keyType match{
      case `venueAuthorCountPrefix`=>{
        logger.info("getAuthorsPublishedHighestPaperInVenue is calling....")
        getAuthorsPublishedHighestPapersInVenue(key, values, context)
      }

      case `authorAndItsCoAuthorCountPrefix` =>
        {
          logger.info("getAuthorsWithMostAndLeastCoAuthors is calling....")
          getAuthorsWithMostAndLeastCoAuthors(key, values, context)
        }

      case `authorPublishedYearPrefix` =>
        {
          logger.info("getAuthorPublishedContinously is calling ....")
          getAuthorPublishedContinously(key,values,context)
        }

      case `venuePublicationAuthorCountsPrefix`=>
        {
          logger.info("venueAuthorCountTitleMerge is calling....")
          getPublicationsWithMostAndLeastAuthors(key,values,context)
        }

      case _ =>

    }
  }


  /**
   * Reduce the Authors and coauthor Details by getting only those author which are at specific position
   * on the Author list arranged according to their coauthor count.
   *
   * @param key of form AuthorAndItsCoAuthorCount
   * @param values of form Count:<Number of coAuthor>#Author:<AuthorName>
   * @param context
   *
   * generates the key of form numberOfTopAuthorsForCoAuthorCount
   * generates the values of form [<Author1>,<Author2>,<Author3>.........]
   *
   * generates the key of form numberOfTopAuthorsForCoAuthorCount
   * generates the values of form Authors: [<Author1>,<Author2>,<Author3>.........]
   *
   */
  def getAuthorsWithMostAndLeastCoAuthors(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context) = {

    logger.info("getAuthorsWithMostCoAuthors is called for key: "+key.toString)

    var authorWithCoAuthorBuffer = new ListBuffer[String]()

    values.forEach(((coAuthorDetails: Text) => authorWithCoAuthorBuffer += coAuthorDetails.toString))

    var authorWithCoAuthorList:List[String] = authorWithCoAuthorBuffer.sortWith(_.split("#")(0).split(":")(1).toInt<_.split("#")(0).split(":")(1).toInt).toList

    var topAuthors = new ListBuffer[String]
    val start = math.max(authorWithCoAuthorList.length-numberOfTopAuthorsForCoAuthorCount,0)

    authorWithCoAuthorList.length-1 to start by -1 foreach {
      index =>
        topAuthors += authorWithCoAuthorList(index).split("#")(1).split(":")(1)
    }
    context.write(new Text(authorWithMostCoAuthors), new Text("["+topAuthors.mkString(",")+"]"))

    var buttomsAuthors = new ListBuffer[String]
    val end = scala.math.min(numberOfButtomAuthorsForCoAuthorCount-1,authorWithCoAuthorList.length-1)
    0 to end foreach {
      index =>
        buttomsAuthors += authorWithCoAuthorList(index).split("#")(1).split(":")(1)
    }
    context.write(new Text(authorWithLeastCoAuthors), new Text("Authors: ["+buttomsAuthors.mkString(",")+"]"))
  }

  /**
   * Produces the list of authors who has published the highest number of publication at the given venue.
   * @param key of the form VenueAuthorCount#Venue:<VenueName>
   * @param values of the form Count:<NumberOfPaper>#Author:<AuthorName>
   * @param context
   * generates the key of the form TopPublishAuthors#Venue:<VenueName>
   * generates the value of the form Authors:[<author1>,<author2>,........]
   */
  def getAuthorsPublishedHighestPapersInVenue(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context) = {

    logger.info("getAuthorsPublishedHighestPaperInVenue is called for: "+key.toString)

    var maxPublishCount: Int = 0
    val venueDetails:String = key.toString.split("#")(1)
    var topAuthorsBuffer = new ListBuffer[String]()

//    collecting only those author which has highest number of publication at the given venue
    values.forEach((value) =>
      {
        val valueDetails:Array[String] = value.toString.split("#")
        logger.info("value: "+value.toString)
        val count = valueDetails(0).split(":")(1).toInt

        if(count > maxPublishCount)
          {
            maxPublishCount=count
            topAuthorsBuffer.clear()
          }
        if(count == maxPublishCount)
            topAuthorsBuffer += valueDetails(1).split(":")(1)

      })
    context.write(new Text(topPublishAuthor+"#"+venueDetails), new Text("Authors: ["+topAuthorsBuffer.mkString(",")+"]"))
  }

  /**
   *
   * this generates the list of author who published continuously for specific number of years.
   * @param key is of the form AuthorAndPublishYear
   * @param values is of the form Author:<AuthorName>#YearEncode:<1111__11,111_11111111111,....>
   * @param context
   *
   * generates the key of the form AuthorsWhoPublishedContinually
   * generates the value of the form Authors: [<Author1>,<Author2>,<Author3>.....]
   */
  def getAuthorPublishedContinously(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context) = {

    logger.info("getAuthorPublishedContinously for key: "+key.toString)

    val authorNameBuffer = new ListBuffer[String]

//    fetching the encoded version of year which has to be present in author year encoding for
//    author to be selected.
    val targetPattern:String = getPatternForYearEncoding()

//    fetching only those authors which has continually published for specific number of years by
//    checking whether they have required pattern in their year encoding or not.
    values.forEach(((value) =>
      {
        val yearEncoding = value.toString.split("#")(1).split(":")(1)
        if(yearEncoding.contains(targetPattern))
          {
            val authorName = value.toString.split("#")(0).split(":")(1)
            authorNameBuffer += authorName
          }
      }))
      context.write(new Text(authorPublishedContinuallyKey), new Text("Authors: ["+authorNameBuffer.mkString(",")+"]"))
    }

  /**
   *
   * It produces the list of authors who have published the least and the most number of publication at given venue.
   * @param key is of form VenuePublicationAuthorCounts#Venue:<VenueName>
   * @param values is of form AuthorCount:<NumberOfAuthors>#Titles:<titles separated by comma>
   * @param context
   *
   * generates the key of the form MaxAuthorPublicationTitle#Venue:<VenueName>
   * generates the value of the form Titles: [<title1>,<title2>,<title3>,........]
   *
   * generates the key of the form LeastAuthorPublicationTitle#Venue:<VenueName>
   * generates the value of the form Titles: [<title1>,<title2>,<title3>,........]
   */
  def getPublicationsWithMostAndLeastAuthors(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context) = {

    logger.info("getPublicationsWithMostAndLeastAuthors is called for key :"+key)

    var maxAuthorCount:Int =0
    var minAuthorCount:Int = Int.MaxValue
    var maxTitles:String = ""
    var minTitles:String = ""
    val venueDetails:String = key.toString.split("#")(1)

//    iterating through the values and collecting the titles of publication
//    only if it has maximum number of authors or minimum number of authors.
    values.forEach(value =>
      {
        val authorCount:Int = value.toString.split("#")(0).split(":")(1).toInt

        if(authorCount > maxAuthorCount)
          {
            maxTitles = value.toString.split("#")(1).split(":")(1)
            maxAuthorCount=authorCount
          }
        if(minAuthorCount > authorCount)
          {
            minAuthorCount = authorCount
            minTitles = value.toString.split("#")(1).split(":")(1)
          }
      }
      )
    context.write(new Text(maxAuthorPublicationTitlePrefix+"#"+venueDetails),new Text("Titles: ["+maxTitles+"]"))
    context.write(new Text(leastAuthorPublicationTitlePrefix+"#"+venueDetails),new Text("Titles: ["+minTitles+"]"))
  }

  /**
   * @return the encoded form of year string according to the configuration.
   *         which equal to the number of one in continuation uptill they are one less than the
   *         numberOfContinuousYears config key
   */
  def getPatternForYearEncoding():String={

    var targetPattern:StringBuilder = new StringBuilder()
    1 to numberOfContinuousYears-1 foreach {
    year =>
    targetPattern.append("1")
  }
  targetPattern.toString()
  }
}