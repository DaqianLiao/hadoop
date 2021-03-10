package com.ldq.es;


import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class AppendLuceneRequestBuilder extends ActionRequestBuilder<AppendLuceneRequest, AppendLuceneResponse, AppendLuceneRequestBuilder> {
    public AppendLuceneRequestBuilder(ElasticsearchClient client, Action<AppendLuceneRequest, AppendLuceneResponse, AppendLuceneRequestBuilder> action) {
        super(client, action, new AppendLuceneRequest());
    }
}
