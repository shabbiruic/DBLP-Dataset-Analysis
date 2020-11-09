import Utils.XMLInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object MapReduceDriver {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * main driver class which will execute the whole map reduce job.
   * @param args
   * first argument is qualified name of main method
   * second argument is path to input folder
   * third argument is path where you want output folder to be generated.
   */
  def main(args: Array[String]): Unit = {

    logger.info("Executing the main method MapReduceDriver class....")
    val conf = new Configuration
    val job = Job.getInstance(conf, "DBLP Statistics")
    job.setJarByClass(this.getClass)

//    setting formatter class which will split the content of input file into predefined records
    job.setInputFormatClass(classOf[XMLInputFormat])

//    setting the mapper class which will convert the record passed by above formatter into the key value pair
    job.setMapperClass(classOf[XMLMapper])

//  aggregates the key value pair and generates the intermediate key value pair according to the
//    end results
    job.setCombinerClass(classOf[XMLCombiner])
//    generates the key value pair which are the needed results for the task.
    job.setReducerClass(classOf[XMLReducer])

//    setting the data type for output key
    job.setOutputKeyClass(classOf[Text])

//    setting the data type for output key.
    job.setOutputValueClass(classOf[Text])

//    setting the path of Input folder i.e. second argument of passed input array.
    FileInputFormat.addInputPath(job, new Path(args(1)))

//    setting the path of output folder i.e. third argument of passed input array.
    FileOutputFormat.setOutputPath(job, new Path(args(2)))

    System.exit(if (job.waitForCompletion(true)) 0
    else 1)

    logger.info("MapReduce job got executed successfully!!!!")
  }

}
