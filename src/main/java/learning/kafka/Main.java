package learning.kafka;

import learning.kafka.producer.MusicRecordsProducer;
import learning.kafka.streams.MusicRecordsEnricher;

public class Main {

  public static void main(String[] args) {
    final MusicRecordsProducer musicRecordsProducer = new MusicRecordsProducer();
    musicRecordsProducer.produceSongRecords();
    musicRecordsProducer.produceArtistRecords();
    final MusicRecordsEnricher musicRecordsEnricher = new MusicRecordsEnricher();
    musicRecordsEnricher.enrichArtistRecords();
  }

}
