package Utils

import java.util

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

/**
 * singleton class to read the configurations
 */
object MyConfig {
  val logger:Logger = LoggerFactory.getLogger(MyConfig.getClass)

val config = ConfigFactory.load("application");

  def getString(key: String): String = try
    {
    logger.info("fetching String " + key +" ...." )
    config.getString(key)

  }
  catch {
    case w: ConfigException.WrongType => logger.error("\""+key + "\" key contains invalid string");throw new ConfigKeyException()
    case e: ConfigException.Missing => logger.error("\""+key + "\" key is not present in Config");throw new ConfigKeyException()
  }

  def getStringList(key: String): util.List[String] = try {
    logger.info("fetching String List" + key +" ...." )
    config.getStringList(key)
  }
  catch {
    case w: ConfigException.WrongType => logger.error("\"" + key + "\" key is not a List of String type"); throw new ConfigKeyException()
    case m: ConfigException.Missing => logger.error("\"" + key + "\" key is not present in Config"); throw new ConfigKeyException()
  }

  def getInt(key:String): Int =
  {try {
    logger.info("fetching Double " + key +" ...." )
    config.getInt(key)
  }
  catch {
    case w: ConfigException.WrongType => logger.error("\""+key + "\" key not contains the Number");throw new ConfigKeyException()
    case m: ConfigException.Missing => logger.error("\""+key + "\" key is not present in Config");throw new ConfigKeyException()
  }
  }
}
