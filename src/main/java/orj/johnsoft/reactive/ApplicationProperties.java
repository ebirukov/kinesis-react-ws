package orj.johnsoft.reactive;

import java.io.InputStream;
import java.util.Properties;

public final class ApplicationProperties {

    public static Properties loadProperties(String fileName) {
        Properties properties = new Properties();
        String profile = System.getProperty("profile", "dev");
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(profile + "/" + fileName)) {
            properties.load(is);
        } catch (Exception e) {
            System.out.println("can't load configuration from " + profile + "/" + fileName);
        }
        return properties;
    }
}
