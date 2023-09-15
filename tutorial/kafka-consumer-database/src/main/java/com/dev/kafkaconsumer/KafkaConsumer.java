package com.dev.kafkaconsumer;

import com.dev.kafkaconsumer.entity.WikimediaData;
import com.dev.kafkaconsumer.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    private static final Logger LOGGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private WikimediaDataRepository wikimediaDataRepository;

    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consume(String eventMessage) {

        LOGGGER.info(String.format("Event Message Received -> %s", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikimediaData(eventMessage);
        this.wikimediaDataRepository.save(wikimediaData);
    }
}
