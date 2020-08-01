package learning.kafka.streams;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONArray;
import org.json.JSONObject;

@Slf4j
public class MusicRecordsEnricher {

  public void enrichArtistRecords() {
    final Properties streamsProperties = getStreamsProperties();

    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final KStream<String, String> songsStream = streamsBuilder.stream("songs");
    final KStream<String, String> artistsStream = streamsBuilder.stream("artists");

    final KTable<String, String> songsTable = songsStream.groupBy((x, y) -> new JSONObject(y).
        getString("artist")).aggregate(
        () -> new JSONObject().put("songs", new JSONArray()).put("listenersCount", 0L).toString(),
        (k, v, agg) -> {
          JSONObject jsonObject = new JSONObject(agg);
          JSONObject jsonObject1 = new JSONObject(v);
          jsonObject.getJSONArray("songs").put(jsonObject1.getString("songName"));
          jsonObject.put("listenersCount", jsonObject.getLong("listenersCount") + Long
              .parseLong(jsonObject1.getString("listeners")));
          jsonObject.put("artist", k);

          return jsonObject.toString();
        });

    final KTable<String, String> artistTable = artistsStream.selectKey((x, y) -> y).toTable();

    final KTable<String, String> result = songsTable.join(artistTable, (x, y) -> x);

    result.toStream().to("result");

    final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);
    kafkaStreams.cleanUp();
    kafkaStreams.start();

    log.info("Describe topology = {}", streamsBuilder.build().describe());
  }

  private Properties getStreamsProperties() {
    final Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    streamsProperties
        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsProperties
        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    streamsProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    log.info("Streams properties = {}", streamsProperties);
    return streamsProperties;
  }

}
