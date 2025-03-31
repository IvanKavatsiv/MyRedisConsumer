package com.kaiv.redis.config;

import com.kaiv.redis.service.UserStreamListener;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;

@Configuration
@RequiredArgsConstructor
public class redisSubscription {

    private static final String STREAM_NAME_TOPIC = "user_stream";
    public static final String CONSUMER_GROUP = "user_consumer_group1";
    String consumerName = "consumer-1";

    private final StringRedisTemplate stringRedisTemplate;

    @Autowired
    private UserStreamListener userStreamListener;

    @Bean
    public Subscription redisStreamSubscription(RedisConnectionFactory factory) {

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(1))
                .build();

        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                StreamMessageListenerContainer.create(factory, options);

        // createConsumerGroup
        if (!stringRedisTemplate.hasKey(STREAM_NAME_TOPIC)) {
            stringRedisTemplate.opsForStream().createGroup(STREAM_NAME_TOPIC, ReadOffset.latest(), CONSUMER_GROUP);
        } else {
            stringRedisTemplate.opsForStream().groups(STREAM_NAME_TOPIC).stream()
                    .filter(infoGroup -> infoGroup.groupName().equals(CONSUMER_GROUP)).findFirst().ifPresentOrElse(
                            infoGroup -> {
                            },
                            () -> stringRedisTemplate.opsForStream().createGroup(STREAM_NAME_TOPIC, CONSUMER_GROUP));
        }

        final StreamOffset<String> streamOffSet = StreamOffset.create(STREAM_NAME_TOPIC, ReadOffset.lastConsumed());

        final Consumer consumer = Consumer.from(CONSUMER_GROUP, consumerName);

        // create Subscription
        Subscription subscription = container.receive(consumer, streamOffSet, userStreamListener);

        container.start();
        return subscription;
    }

}
