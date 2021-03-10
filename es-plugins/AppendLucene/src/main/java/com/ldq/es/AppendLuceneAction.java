package com.ldq.es;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class AppendLuceneAction extends Action<AppendLuceneRequest, AppendLuceneResponse, AppendLuceneRequestBuilder> {
    public static final AppendLuceneAction INSTANCE = new AppendLuceneAction();
    public static final String NAME = "indices:append/lucene";

    private AppendLuceneAction() {
        super(NAME);
    }

    @Override
    public AppendLuceneRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new AppendLuceneRequestBuilder(client, this);
    }

    @Override
    public AppendLuceneResponse newResponse() {
        return new AppendLuceneResponse();
    }
}
