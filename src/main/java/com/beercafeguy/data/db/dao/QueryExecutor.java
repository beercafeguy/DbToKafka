package com.beercafeguy.data.db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutor {
    public static ResultSet getResult(String query) throws SQLException, ClassNotFoundException {
        Statement statement = ConnectionFactory.getConnection().createStatement();
        return statement.executeQuery(query);
    }
}
