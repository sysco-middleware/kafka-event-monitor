package no.sysco.middleware.kafka.eventmonitor;

/**
 * Application entry-point.
 */
public class EventMonitorMain {
    public static void main(String[] args) {
        try {
            EventMonitorConfig config = EventMonitorConfig.load();
            EventMonitorApplication app = new EventMonitorApplication(config);
            app.run();
            Runtime.getRuntime().addShutdownHook(new Thread(app::close));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
