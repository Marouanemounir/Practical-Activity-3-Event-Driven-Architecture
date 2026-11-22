package ma.enset.kafkaspringcloudstream.handlers;

import ma.enset.kafkaspringcloudstream.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class PageEventStreamTest {

    private static final Instant WINDOW_START = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, PageEvent> input;
    private TestOutputTopic<String, Long> output;

    @BeforeEach
    void setUp() {
        JsonSerde<PageEvent> eventSerde = new JsonSerde<>(PageEvent.class).noTypeInfo();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PageEvent> stream = builder.stream("T3", Consumed.with(Serdes.String(), eventSerde));
        new PageEventHandler().kStreamFunction().apply(stream)
                .to("T4", Produced.with(Serdes.String(), Serdes.Long()));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "page-analytics-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(builder.build(), config);
        input = driver.createInputTopic("T3", Serdes.String().serializer(), eventSerde.serializer());
        output = driver.createOutputTopic("T4", Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void ignoresVisitsOfAtMost100Ms() {
        send("P1", 100, 0);
        send("P1", 101, 1);

        assertThat(output.readKeyValuesToList()).containsExactly(KeyValue.pair("P1", 1L));
    }

    @Test
    void countsVisitsPerPageInTheSameWindow() {
        send("P1", 500, 0);
        send("P2", 500, 1_000);
        send("P1", 500, 4_999);

        List<KeyValue<String, Long>> results = output.readKeyValuesToList();

        assertThat(results).containsExactly(
                KeyValue.pair("P1", 1L),
                KeyValue.pair("P2", 1L),
                KeyValue.pair("P1", 2L));
    }

    @Test
    void restartsCountingInTheNextWindow() {
        send("P1", 500, 0);
        send("P1", 500, 1_000);
        send("P1", 500, 5_000);

        assertThat(output.readValuesToList()).containsExactly(1L, 2L, 1L);
    }

    private void send(String page, long duration, long offsetMs) {
        Instant timestamp = WINDOW_START.plusMillis(offsetMs);
        input.pipeInput(null, new PageEvent(page, "U1", Date.from(timestamp), duration), timestamp);
    }
}
