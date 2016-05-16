package spark.jobserver

import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.cassandra.CassandraSQLContext

trait CassandraQuery extends SparkCassandraJob {
  def validate(cc: CassandraSQLContext, config: Config): SparkJobValidation = SparkJobValid
}

object GetSubreddits extends CassandraQuery {

  override def runJob(cc: CassandraSQLContext, config: Config): Any = {
    cc.sql("SELECT DISTINCT subreddit FROM reddit.comments")
      .map(_.getValuesMap[Any](List("subreddit"))).collect
  }
}

object GetSubredditsDistinct extends CassandraQuery {

  override def runJob(cc: CassandraSQLContext, config: Config): Any = {
    cc.sql("SELECT subreddit FROM reddit.comments").distinct()
      .map(_.getValuesMap[Any](List("subreddit"))).collect
  }
}

object GetThreads extends CassandraQuery {

  override def runJob(cc: CassandraSQLContext, config: Config): Any = {
    var rows = 20
    var query: String = "SELECT link_id FROM reddit.comments "
    try {
      val subreddit: String = config.getString("subreddit")
      query += " WHERE subreddit = '" + subreddit + "'"
    } catch { case e: ConfigException => }
    try {
      val page: Int = config.getString("page").toInt
      rows *= page
    } catch { case e: ConfigException => }
    query += " LIMIT " + rows.toString

    cc.sql(query).distinct()
      .map(_.getValuesMap[Any](List("link_id"))).collect
  }
}

object GetComments extends CassandraQuery {

  override def runJob(cc: CassandraSQLContext, config: Config): Any = {
    var query: String = "SELECT created_utc, subreddit, parent_id, name, author, body, ups, downs, score FROM reddit.comments"
    try {
      val link_id: String = config.getString("link_id")
      query += " WHERE link_id = '" + link_id + "'"
    } catch { case e: ConfigException => }

    cc.sql(query)
      .map(_.getValuesMap[Any](List("created_utc", "subreddit", "parent_id", "name",
        "author", "body", "ups", "downs", "score"))).collect
  }
}

object GetUser extends CassandraQuery {

  override def runJob(cc: CassandraSQLContext, config: Config): Any = {
    var query: String = "SELECT created_utc, subreddit, link_id, parent_id, name, author, body, ups, downs, score FROM reddit.comments"
    try {
      val author: String = config.getString("author")
      query += " WHERE author = '" + author + "'"
    } catch { case e: ConfigException => }

    cc.sql(query)
      .map(_.getValuesMap[Any](List("created_utc", "subreddit", "link_id", "parent_id", "name",
        "author", "body", "ups", "downs", "score"))).collect
  }
}