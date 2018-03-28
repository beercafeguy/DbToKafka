package com.beercafeguy.data.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyFactory {
    private PropertyFactory(){
    }
    public static Properties getProperties(String propertyFileName) throws IOException {
        Properties properties=new Properties();
        properties.load(new FileInputStream(propertyFileName));
        return properties;
    }
}
