import java.sql.{Connection, DriverManager, ResultSet}

object GlueApp {
  def main(sysArgs: Array[String]) {
    classOf[org.postgresql.Driver]
    val con_str = "jdbc:postgresql://172.30.1.6:49153/postgres?user=postgres&password=postgrespw"
    val conn = DriverManager.getConnection(con_str)
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = stm.executeQuery("select 'aaaa' as ttt")
      //stm.execute("call public.test_proc()")
      while(rs.next) {
        println(rs.getString("ttt"))
        }
      } finally {
          conn.close()
        }
  }
}
