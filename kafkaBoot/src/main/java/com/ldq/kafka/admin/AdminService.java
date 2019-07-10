package com.ldq.kafka.admin;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class AdminService {

    @Autowired
    private AdminClient adminClient;


    public String createTopic(String topicName, int partitions, int replicationFactor) {
        adminClient.createTopics(Arrays.asList(new NewTopic(topicName, partitions, (short) replicationFactor)));
        return "create success";
    }


    public String findAllTopic() throws ExecutionException, InterruptedException {
        ListTopicsResult result = adminClient.listTopics();
        Collection<TopicListing> list = result.listings().get();
        List<String> resultList = new ArrayList<>();
        for (TopicListing topicListing : list) {
            resultList.add(topicListing.name());
        }
        return JSONObject.toJSONString(resultList);
    }


    public String topicInfo(String topicName) throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topicName));
        Map<String, String> resultMap = new HashMap<>();
        result.all().get().forEach((k, v) -> {
            log.info("k: " + k + " ,v: " + v.toString());
            resultMap.put(k, v.toString());
        });

        result.all().get().forEach((k, v) -> System.out.println("k: " + k + " ,v: " + v.toString() + "\n\r"));
        return JSONObject.toJSONString(resultMap);
    }


    public String deleteTopic(String topicName) {
        DeleteTopicsResult result = adminClient.deleteTopics(Arrays.asList(topicName));
        return JSONObject.toJSONString(result.values());
    }

    public String findAllConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = listConsumerGroupsResult.all().get();
        List<String> resultList = new ArrayList<>();
        for (ConsumerGroupListing topicListing : consumerGroupListings) {
            resultList.add(topicListing.groupId());
        }
        return JSONObject.toJSONString(resultList);

    }

    public String consumerGroupInfo(String groupId) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult groupsResult = adminClient.describeConsumerGroups(Arrays.asList(groupId));
        Map<String, String> resultMap = new HashMap<>();
        groupsResult.all().get().forEach((k, v) -> {
            log.info("k: " + k + " ,v: " + v.toString());
            resultMap.put(k, v.toString());
        });

        resultMap.forEach((k, v) -> System.out.println("k: " + k + " ,v: " + v + "\n\r"));
        return JSONObject.toJSONString(resultMap);
    }

    public String controllerInfo() throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        return describeClusterResult.controller().get().toString();
    }

    public void clusterInfo() throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        describeClusterResult.nodes().get().forEach(System.out::println);

    }

    public void config() throws ExecutionException, InterruptedException {
        DescribeConfigsResult describeConfigsResult =
                adminClient.describeConfigs(Collections.singleton(
                        new ConfigResource(ConfigResource.Type.BROKER, "")));
        describeConfigsResult.all().get().forEach((x, y) -> System.out.println(x.toString()
                + Arrays.toString(y.entries().toArray())));

    }

    public String topicConfig(String topicName) throws ExecutionException, InterruptedException {
        DescribeConfigsResult describeConfigsResult =
                adminClient.describeConfigs(Collections.singleton(
                        new ConfigResource(ConfigResource.Type.TOPIC, topicName)));

        Map<String, Object> config = new HashMap<>();

        config.put("topic", topicName);
        describeConfigsResult.all().get().forEach((x, y) -> {
            y.entries().forEach(cfg -> config.put(cfg.name(), cfg.value()));
        });

        return Strings.join(config.entrySet().iterator(), '\n');
    }

}
