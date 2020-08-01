package learning.kafka.producer;

import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

@Slf4j
public class MusicRecordsProducer {

  public static final String API_KEY = "";//Add API Key here
  public static final String COUNTRY = "";//Add country here
  private static final String SONGS_API_ENDPOINT =
      "https://ws.audioscrobbler.com/2.0/?method=geo.gettoptracks&country=" + COUNTRY + "&api_key="
          + API_KEY + "&format=json&limit=1000";

  private static final String ARTIST_API_ENDPOINT =
      "https://ws.audioscrobbler.com/2.0/?method=geo.gettopartists&country=" + COUNTRY
          + "&api_key=" + API_KEY + "&format=json&limit=1000";

  @SneakyThrows
  public void produceSongRecords() {
    final KafkaProducer<String, String> kafkaProducer = getKafkaProducer();

    final JsonNode jsonBody = Unirest.get(SONGS_API_ENDPOINT).asJson().getBody();
    final JSONArray tracksArray = jsonBody.getObject().getJSONObject("tracks")
        .getJSONArray("track");

    for (int i = 0; i < tracksArray.length(); i++) {
      final JSONObject songRecord = new JSONObject();
      songRecord.put("songName", tracksArray.getJSONObject(i).get("name"));
      songRecord.put("listeners", tracksArray.getJSONObject(i).get("listeners"));
      songRecord.put("url", tracksArray.getJSONObject(i).get("url"));
      songRecord.put("artist", tracksArray.getJSONObject(i).getJSONObject("artist").get("name"));
      songRecord.put("rank", tracksArray.getJSONObject(i).getJSONObject("@attr").get("rank"));

      kafkaProducer.send(new ProducerRecord<>("songs", songRecord.toString()));
    }
  }

  @SneakyThrows
  public void produceArtistRecords() {
    final KafkaProducer<String, String> kafkaProducer = getKafkaProducer();
    final JsonNode jsonNode = Unirest.get(ARTIST_API_ENDPOINT).asJson().getBody();

    final JSONArray artistsArray = jsonNode.getObject().getJSONObject("topartists")
        .getJSONArray("artist");

    for (int i = 0; i < artistsArray.length(); i++) {
      final String artistName = artistsArray.getJSONObject(i).getString("name");

      kafkaProducer.send(new ProducerRecord<>("artists", artistName));
    }
  }

  private KafkaProducer<String, String> getKafkaProducer() {
    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProperties
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    log.info("Producer properties = {}", producerProperties);
    return new KafkaProducer<>(producerProperties);
  }
}
