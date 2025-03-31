package com.kaiv.redis.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kaiv.redis.model.User;
import lombok.RequiredArgsConstructor;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;


import static com.kaiv.redis.config.redisSubscription.CONSUMER_GROUP;

@Component
@RequiredArgsConstructor
public class UserStreamListener implements StreamListener<String, MapRecord<String, String, String>> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(UserStreamListener.class);

    private final StringRedisTemplate stringRedisTemplate;

    private final ObjectMapper objectMapper;

    @Override
    public void onMessage(MapRecord<String, String, String> message) {

        final User user = objectMapper.convertValue(message.getValue(), User.class);

        String id = user.getId();
        String name = user.getName();
        int age = user.getAge();

        stringRedisTemplate.opsForStream().acknowledge(CONSUMER_GROUP, message);

        LOG.info("Received user event from stream with info: " + id + ", " + name + ", " + age);
    }
}
