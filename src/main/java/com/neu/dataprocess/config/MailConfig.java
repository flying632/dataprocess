package com.neu.dataprocess.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author fengyuluo
 * @createTime 10:05 2019/4/26
 */
public class MailConfig {
    private static final String PROPERTIES_DEFAULT = "mailConfig.properties";
    public static String host;
    public static Integer port;
    public static String userName;
    public static String passWord;
    public static String emailForm;
    public static String timeout;
    public static String personal;
    public static Properties properties;
    static{
        init();
    }

    /**
     * 初始化
     */
    private static void init() {
        properties = new Properties();
        try{
            InputStream inputStream = MailConfig.class.getClassLoader().getResourceAsStream(PROPERTIES_DEFAULT);
            properties.load(inputStream);
            inputStream.close();
            host = properties.getProperty("spring.mail.host");
            port = Integer.parseInt(properties.getProperty("spring.mail.port"));
            userName = properties.getProperty("spring.mail.username");
            passWord = properties.getProperty("spring.mail.password");
            emailForm = properties.getProperty("mailFrom");
            timeout = properties.getProperty("mailTimeout");
            personal = "风雨落";
        } catch(IOException e){
            e.printStackTrace();
        }
    }
}

