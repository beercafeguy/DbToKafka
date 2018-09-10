package com.beercafeguy.data.model;

import com.beercafeguy.data.db.dao.DataSourceFactory;
import com.beercafeguy.data.exception.InvalidDBTypeException;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    private String tableName;
    private String dbType;

    public TableInfo(String tableName, String dbType) {
        this.tableName = tableName;
        if(dbType.equals("mysql") || dbType.equals("oracle")) {
            this.dbType = dbType;
        }else{
            throw new InvalidDBTypeException(dbType+" is not a supported DBType");
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbType() {
        return dbType;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public List<String> getColumnList() throws SQLException {
        List<String> columns=new ArrayList<>();
        DataSource ds=null;
        ResultSetMetaData resultSetMetaData=null;
        if("mysql".equals(dbType)){
            ds = DataSourceFactory.getMySQLDataSource();
        }else if("oracle".equals(dbType)){
            ds = DataSourceFactory.getOracleDataSource();
        }
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = ds.getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(getMetaQuery());
            resultSetMetaData=rs.getMetaData();
            int columnCount=resultSetMetaData.getColumnCount();
            int columnCounter=1;
            while(columnCounter<=columnCount){
                columns.add(resultSetMetaData.getColumnName(columnCounter));
                columnCounter++;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try {
                if(rs != null) rs.close();
                if(stmt != null) stmt.close();
                if(con != null) con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return columns;
    }

    private String getMetaQuery(){
        StringBuffer buffer=new StringBuffer();
        buffer.append("select * from ");
        buffer.append(tableName);
        buffer.append(" where (1=0)");
        return buffer.toString();
    }
}
