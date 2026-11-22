package ma.enset.kafkaspringcloudstream.controllers;

import ma.enset.kafkaspringcloudstream.events.PageEvent;
import ma.enset.kafkaspringcloudstream.handlers.PageEventHandler;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class PageEventController {

    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;

    public PageEventController(StreamBridge streamBridge, InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    /**
     * Producer Kafka : génère un événement et le publie dans le topic demandé.
     */
    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name,
                             @RequestParam(defaultValue = "T2") String topic) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        PageEvent event = new PageEvent(
                name,
                random.nextBoolean() ? "U1" : "U2",
                new Date(),
                10 + random.nextInt(10_000)
        );
        if (!streamBridge.send(topic, event)) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Impossible de publier l'événement dans le topic " + topic);
        }
        return event;
    }

    /**
     * Diffuse chaque seconde, en Server-Sent Events, le nombre de visites par page
     * dans la fenêtre courante du state store Kafka Streams.
     */
    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1), Schedulers.boundedElastic())
                .onBackpressureDrop()
                .map(tick -> currentWindowCounts());
    }

    private Map<String, Long> currentWindowCounts() {
        Map<String, Long> counts = new HashMap<>();
        Map<String, Long> latestWindowStarts = new HashMap<>();
        Instant now = Instant.now();
        try {
            ReadOnlyWindowStore<String, Long> store = interactiveQueryService.getQueryableStore(
                    PageEventHandler.COUNT_STORE, QueryableStoreTypes.windowStore());
            try (KeyValueIterator<Windowed<String>, Long> windows =
                         store.fetchAll(now.minus(PageEventHandler.WINDOW_SIZE), now)) {
                while (windows.hasNext()) {
                    KeyValue<Windowed<String>, Long> window = windows.next();
                    String page = window.key.key();
                    long windowStart = window.key.window().start();
                    if (windowStart >= latestWindowStarts.getOrDefault(page, Long.MIN_VALUE)) {
                        latestWindowStarts.put(page, windowStart);
                        counts.put(page, window.value);
                    }
                }
            }
        } catch (InvalidStateStoreException | IllegalStateException storeNotReady) {
            return Map.of();
        }
        return counts;
    }
}
