package learning.kafka;

import learning.kafka.producer.MusicRecordsProducer;

public class Main {

  public static void main(String[] args) {
    final MusicRecordsProducer musicRecordsProducer = new MusicRecordsProducer();
    musicRecordsProducer.produceSongRecords();
    musicRecordsProducer.produceArtistRecords();
  }

}
