package ma.enset.kafkaspringcloudstream.handlers;

import ma.enset.kafkaspringcloudstream.events.PageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class PageEventHandler {

    private static final Logger log = LoggerFactory.getLogger(PageEventHandler.class);

    /**
     * Consumer Kafka : lit les événements du topic T2.
     */
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return event -> log.info("Événement reçu : {}", event);
    }
}
