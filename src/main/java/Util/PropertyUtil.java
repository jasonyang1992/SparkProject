package Util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {

    private static final Properties PROPS = new Properties();

    static{
        try {
            load();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void load() throws IOException{
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try(InputStream inputStream = loader.getResourceAsStream(System.getProperty("config.file.name"))) {
            PROPS.load(inputStream);
        }
    }

    public static String getProperty(final String name){
        return PROPS.getProperty(name);
    }
}
