package ma.enset.kafkaspringcloudstream.handlers;

import ma.enset.kafkaspringcloudstream.events.PageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    /**
     * Supplier Kafka : publie périodiquement un événement aléatoire dans le topic T3.
     */
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            return new PageEvent(
                    random.nextBoolean() ? "P1" : "P2",
                    random.nextBoolean() ? "U1" : "U2",
                    new Date(),
                    10 + random.nextInt(10_000)
            );
        };
    }
}
