
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCBatch {

	String url = "jdbc:postgresql://000:1234/db";    			
	String user = "user";
	String password = "passwd";
    Connection conn = null;
    Statement selectStatement = null;
    PreparedStatement insertStatement = null;
    ResultSet resultSet  = null; 
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		(new JDBCBatch()).doTask();
	}
	public void doTask()
	{
		try {
			conn=getConnectionPSSRE();
			
			String sqlTruncateETL_TEMP = " TRUNCATE TABLE SCHEMATEST.TEST ";
            System.out.println(sqlTruncateETL_TEMP);
            selectStatement = conn.createStatement();
            
            selectStatement.executeUpdate(sqlTruncateETL_TEMP);
            
            String sqlSelectXX_TEMP = " SELECT     ";
            sqlSelectXX_TEMP += "         P,T,B ";
            sqlSelectXX_TEMP += "   FROM  WEEE     ";
            sqlSelectXX_TEMP += "  WHERE GGGGGG " ;
            resultSet = selectStatement.executeQuery(sqlSelectXX_TEMP);
            
            int recordPerCommit=0;
            String sqlInsertETL_TEMP = "insert into CCCC.GGGG values( ?,?,?,XXXX.F_ENCRYPT('BAS',?))";
        	insertStatement = conn.prepareStatement(sqlInsertETL_TEMP);
        	
            while(resultSet.next()) {
            	insertStatement.setString(1, resultSet.getString("BBB"));
            	insertStatement.setLong(2, resultSet.getLong("BBBB"));
            	insertStatement.setString(3, resultSet.getString("XXX"));
            	
            	String plainText = null;
            	
            	insertStatement.setString(4, plainText);
            	insertStatement.addBatch();
            	insertStatement.clearParameters();
            	
            	if((recordPerCommit % 1000) == 0) {
            		int commitCount=getBatchCnt(insertStatement.executeBatch());
            		insertStatement.clearBatch();
                    conn.commit();
                    System.out.println(recordPerCommit + " ,Commit Record = " + commitCount);
            	}
            	recordPerCommit++;
            }
            
            int commitCount=getBatchCnt(insertStatement.executeBatch());
            conn.commit();
            System.out.println("Commit Record = " + commitCount);
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			closeConnectionPSSRE();
		}
	}
	public Connection getConnectionPSSRE() throws SQLException, ClassNotFoundException 
	{
		Class.forName("org.postgresql.Driver");
  	  	conn = DriverManager.getConnection(url, user, password);
  	  	conn.setAutoCommit(false);
  	  	System.out.println("Connect POSTGRESQL.");

		return conn;
	}
	public void closeConnectionPSSRE()
	{
		try
		{
			if(resultSet != null) resultSet.close();
			if(selectStatement != null) selectStatement.close();
			if(insertStatement != null) insertStatement.close();
			if(conn != null)   conn.close();
			System.out.println("Close all instance.");
		}catch(SQLException e1) {
            e1.printStackTrace();
        }
	}
	public int getBatchCnt(int[] updates){
	    int tmpCnt = 0;
	    for(int i=0; i < updates.length; i++){
	        if( updates[i] == Statement.SUCCESS_NO_INFO ){         
	            tmpCnt = tmpCnt + 1;
	        }else if( updates[i] == Statement.EXECUTE_FAILED){     
	            tmpCnt = tmpCnt + 0;
	        }else{
	            tmpCnt = tmpCnt + updates[i];
	        }
	    }
	    return tmpCnt;
	}
}