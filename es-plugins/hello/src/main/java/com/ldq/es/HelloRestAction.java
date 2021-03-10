package com.ldq.es;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Date;


public class HelloRestAction extends BaseRestHandler {
    private String printName = "helloPluginTest";

    public HelloRestAction(Settings settings, RestController controller) {
        super(settings);
//        注册路径
        controller.registerHandler(RestRequest.Method.GET, "/_hello", this);
    }

    @Override
    public String getName() {
        return printName;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient nodeClient) throws IOException {
        // 接收的参数
        System.out.println("params==" + request.params());

        long t1 = System.currentTimeMillis();

        String name = request.param("name");

        long cost = System.currentTimeMillis() - t1;
        // 返回内容，这里返回消耗时间 请求参数 插件名称
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("cost", cost);
            builder.field("name", name);
            builder.field("time", new Date());
            builder.field("pluginName", printName);
            builder.field("print", "this is print plugin test");
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
