package ma.enset.kafkaspringcloudstream.controllers;

import ma.enset.kafkaspringcloudstream.events.PageEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class PageEventController {

    private final StreamBridge streamBridge;

    public PageEventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
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
}
