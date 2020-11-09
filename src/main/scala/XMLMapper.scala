import Utils.MyConfig
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory

import scala.xml.{Elem, NodeSeq}


class XMLMapper extends Mapper[LongWritable, Text, Text, Text] {

  // loading the key prefixes from the config
  val venueAuthorCountPrefix = MyConfig.getString("keyPrefixes.venueAuthorCount")
  val authorCoauthorPrefix =  MyConfig.getString("keyPrefixes.authorCoauthor")
  val authorPublishedYearPrefix = MyConfig.getString("keyPrefixes.authorPublishedYear")
  val venuePublicationAuthorCountsPrefix = MyConfig.getString("keyPrefixes.venuePublicationAuthorCounts")

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * @param key  starting byte position of record in input file
   * @param value record value in the text format
   * @param context will carry the key value generated to combiner for further processing
   */
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context) = {

    logger.info("map function is called....")

//    loading the string passed as a xml element. along with its .dtd for proper parsing
    val xml = scala.xml.XML.loadString("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<!DOCTYPE dblp SYSTEM \""+getClass.getClassLoader.getResource("dblp.dtd").toURI.toString+"\">" + value.toString)

    val venue = xml.label

//    getting the value of tags
    val year:Option[String] = getTagText(xml, "year")
    val title:Option[String] = getTagText(xml, "title")

//    getting all the editors
    val allAuthors:Array[String]=getAuthorOrEditorSortedList(xml)

//    getting all the author and co-author pairs
    val authorCoAuthorList:Array[Array[String]] = getAuthorCoAuthorList(allAuthors)
    authorCoAuthorList.foreach((i: Array[String]) => context.write(new Text(authorCoauthorPrefix + "#Author:" + i(0)), new Text(i(1))))


    title match {
      case Some(s) => context.write(new Text(venuePublicationAuthorCountsPrefix + "#Venue:" + venue + "#AuthorCount:" + allAuthors.length.toString), new Text(s))
      case None =>
    }


    for (author <- allAuthors) {
      context.write(new Text(venueAuthorCountPrefix+"#Venue:"+venue+"#Author:"+author),new Text(1.toString))
      //    inserting the year key and value only when publication has that entry in record
      year match {
        case Some(y) => context.write(new Text(authorPublishedYearPrefix + "#Author:" + author), new Text(y))
        case None =>
      }
    }
  }

  /**
   * extract the value of passed tag from passed xml element
   * @param xml  xml from which to fetch the value of tag
   * @param tagName name of tag whose value has to be extracted
   * @return
   */
  def getTagText(xml: Elem, tagName: String):Option[String] = {
    val tagSeq: NodeSeq = xml \ tagName
    if (tagSeq.length > 0)
      Some(tagSeq(0).text)
    else
      None
  }

  /**
   * Given the sorted Array of authors created all the possible pairs sorted of authors
   * @param authors  Array of authors
   * @return Array of array of author and co-author
   */
  def getAuthorCoAuthorList(authors: Array[String]): Array[Array[String]] = {

    var authorCoauthor: Array[Array[String]] = Array()
    0 to authors.length - 2 foreach {
      i =>
        authorCoauthor = authorCoauthor ++ authors.slice(i + 1, authors.length).map(Array[String](authors(i), _))
    }
    authorCoauthor
  }

  /**
   * given the xml extracts the Array of authors/Editor from it
   * @param xml  xml from which to extract the author names
   * @return array if Authors
   */
  def getAuthorOrEditorSortedList(xml:Elem):Array[String] = {

    val authorNodes: NodeSeq = xml \ "author"
    val editorNodes: NodeSeq = xml \ "editor"
    var allAuthors: Array[String] = (authorNodes ++ editorNodes).map(_.text).toArray[String].sorted
    return allAuthors
  }
}