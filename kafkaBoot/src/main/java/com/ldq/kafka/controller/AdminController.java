package com.ldq.kafka.controller;

import com.ldq.kafka.admin.AdminService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class AdminController {

    @Autowired
    private AdminService adminService;

    @ApiOperation(value = "创建topic")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topicName", value = "topic名称",
                    defaultValue = "test", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "partitions", value = "分区数", defaultValue = "3",
                    required = true, dataType = "int", paramType = "query"),
            @ApiImplicitParam(name = "replicationFactor", value = "副本数", defaultValue = "3",
                    required = true, dataType = "int", paramType = "query")
    })
    @GetMapping("/createTopic")
    public String createTopic(String topicName,int partitions,int replicationFactor){
        adminService.createTopic( topicName, partitions, replicationFactor);
        return "create success";
    }

    @ApiOperation(value = "查看所有的topic")
    @GetMapping("/findAllTopic")
    public String findAllTopic() throws ExecutionException, InterruptedException {
        return adminService.findAllTopic();
    }

    @ApiOperation(value = "查看topic详情")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topicName", value = "topic名称",defaultValue = "test",
                    required = true, dataType = "string", paramType = "query")
    })
    @GetMapping("/info")
    public String topicInfo(String topicName) throws ExecutionException, InterruptedException {
        return adminService.topicInfo(topicName);
    }

    @ApiOperation(value = "删除topic")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topicName", value = "topic名称",defaultValue = "test",
                    required = true, dataType = "string", paramType = "query")
    })
    @GetMapping("/delete")
    public String deleteTopic(String topicName){
        return  adminService.deleteTopic(topicName);
    }

    @ApiOperation(value = "查看所有的消费者组信息")
    @GetMapping("/findAllCG")
    public String findAllCG() throws ExecutionException, InterruptedException {
        return adminService.findAllConsumerGroups();
    }

    @ApiOperation(value = "查看消费者组详情")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "groupId", value = "groupId名称",defaultValue = "test",
                    required = true, dataType = "string", paramType = "query")
    })
    @GetMapping("/cgInfo")
    public String cgInfo(String groupId) throws ExecutionException, InterruptedException {
        return adminService.consumerGroupInfo(groupId);
    }

    @GetMapping("/clusterInfo")
    public String clusterInfo() throws ExecutionException, InterruptedException {
         adminService.clusterInfo();
        return "print cluster info";

    }

    @GetMapping("/ctrlInfo")
    public String controllerInfo() throws ExecutionException, InterruptedException {
        return adminService.controllerInfo();

    }

    @GetMapping("/topicConfig")
    public String topicConfig(String topicName) throws ExecutionException, InterruptedException {
        return adminService.topicConfig(topicName);
    }

    @GetMapping("/groupIdOffsets")
    public String groupIdOffsets(String groupId) throws ExecutionException, InterruptedException {
         adminService.groupIdOffsets(groupId);
         return "OK ";
    }
}
