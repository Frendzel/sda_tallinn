package com.sda;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.lang.Integer.valueOf;

public class PropertyLoader {

    private static String FILE_NAME = "connection.properties";

    Properties properties = new Properties();

    public PropertyLoader() throws IOException {
        this.init();
    }

    public void init() throws IOException {
        InputStream input = getClass().getClassLoader().getResourceAsStream("connection.properties");
        properties.load(input);
    }

    public String getAddress() {
        return properties.getProperty("address");
    }

    public Integer getPort() {
        return valueOf(properties.getProperty("port"));
    }

    public String getUser() {
        return properties.getProperty("user");
    }

    public char[] getPassword() {
        return properties.getProperty("password").toCharArray();
    }

    public String getDB() {
        return properties.getProperty("db");
    }
}
