package com.beercafeguy.data.db.dao;

import com.mysql.jdbc.Driver;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

public class ConnectionFactory {

    private static Properties dbProperties;

    static{
        try {
            dbProperties=new Properties();
            dbProperties.load(new FileInputStream("src/main/resources/jdbc.properties"));
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
    private ConnectionFactory(){

    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(Driver.class.getName());
        return DriverManager.getConnection(dbProperties.getProperty("db_url"),dbProperties);
    }

    public static void closeConnection(Connection con, Statement st, ResultSet rs) throws Exception{
        if(con!=null)
            con.close();
        if(st!=null)
            st.close();
        if(rs!=null)
            rs.close();
    }
}
