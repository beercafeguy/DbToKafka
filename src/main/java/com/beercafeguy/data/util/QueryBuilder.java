package com.beercafeguy.data.util;

import com.beercafeguy.data.model.TableInfo;
import com.beercafeguy.data.stream.kafka.DbKafkaProducerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class QueryBuilder {

    private final Logger logger = LoggerFactory.getLogger(QueryBuilder.class);
    private TableInfo tableInfo;

    public QueryBuilder(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public String getSelectQuery() throws SQLException {
        StringBuffer buffer=new StringBuffer();
        List<String> columnList=tableInfo.getColumnList();
        int columnCount=columnList.size();
        logger.info("Number of columns in Column List: "+columnCount);
        buffer.append("select ");
        int columnCounter=0;
        while(columnCounter<columnCount){
            buffer.append(columnList.get(columnCounter));
            if(columnCounter<(columnCount-1)){
                buffer.append(",");
            }
            columnCounter++;
        }

        buffer.append(" from ");
        buffer.append(tableInfo.getTableName());
        return buffer.toString();
    }
}
