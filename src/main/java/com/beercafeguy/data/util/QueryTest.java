package com.beercafeguy.data.util;

import com.beercafeguy.data.model.TableInfo;

import java.sql.SQLException;

public class QueryTest {
    public static void main(String[] args) throws SQLException {
        System.out.println("Generate full load query");
        TableInfo tableInfo=new TableInfo("user_master","mysql");
        QueryBuilder queryBuilder=new QueryBuilder(tableInfo);
        System.out.println(queryBuilder.getSelectQuery());
    }
}
