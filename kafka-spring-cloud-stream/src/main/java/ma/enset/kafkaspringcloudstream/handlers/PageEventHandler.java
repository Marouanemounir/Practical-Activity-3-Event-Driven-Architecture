package ma.enset.kafkaspringcloudstream.handlers;

import ma.enset.kafkaspringcloudstream.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    public static final String COUNT_STORE = "count-store";
    public static final Duration WINDOW_SIZE = Duration.ofSeconds(5);
    private static final long MIN_VISIT_DURATION_MS = 100;

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

    /**
     * Kafka Streams : compte les visites de plus de 100 ms par page
     * sur des fenêtres de 5 secondes, du topic T3 vers le topic T4.
     */
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
        return input -> input
                .filter((key, event) -> event.duration() > MIN_VISIT_DURATION_MS)
                .map((key, event) -> new KeyValue<>(event.name(), event.duration()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
                .count(Materialized.as(COUNT_STORE))
                .toStream()
                .map((windowedKey, count) -> new KeyValue<>(windowedKey.key(), count));
    }
}
