import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions

import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.amazonaws.services.glue.log.GlueLogger

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.services.secretsmanager._
import com.amazonaws.services.secretsmanager.model._

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
   
    val logger = new GlueLogger
   
    val client = AWSSecretsManagerClientBuilder.standard().build()
   
    val secretName = "secretName"
    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName)
    val getSecretValueResult = client.getSecretValue(getSecretValueRequest)
    val secret = JsonOptions(getSecretValueResult.getSecretString()).toMap
   
    val jdbc_driver : String = "org.postgresql.Driver"
    val jdbc_url : String = "jdbc:postgresql://" + secret("host") + ":" + secret("port") + "/" + secret("dbname")
   
    import java.sql.{Connection, DriverManager, ResultSet}
    classOf[org.postgresql.Driver]
    val connection_string = "jdbc:postgresql://"+secret("host")+":"+secret("port")+"/"+secret("dbname")+"?user="+secret("username").toString+"&password="+secret("password").toString
    val conn = DriverManager.getConnection(connection_string)
    try {
        val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        val rs = stm.execute("call pssetl.test()")
        println(rs)
    }
    catch {
        case ex: Exception => println(ex)
    }
    finally {
          conn.close()
    }
    Job.commit()
    println("DONE.")
  }
}
