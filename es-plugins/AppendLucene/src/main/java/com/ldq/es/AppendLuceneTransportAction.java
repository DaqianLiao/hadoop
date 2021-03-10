package com.ldq.es;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.FsDirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/* 负责将制定目录的lucene文件加载到制定的shard中 */
public class AppendLuceneTransportAction extends TransportAction<AppendLuceneRequest, AppendLuceneResponse> {

    private static  Logger logger =  Logger.getLogger("AppendLuceneTransportAction");
    private ThreadPool threadPool;
    private IndicesService indicesService;

    @Inject
    public AppendLuceneTransportAction(Settings settings,
                                       ThreadPool threadPool,
                                       ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       TransportService transportService,
                                       IndicesService indexServices) {

        super(settings, AppendLuceneAction.NAME, threadPool, actionFilters, indexNameExpressionResolver,
                transportService.getTaskManager());
        this.threadPool = threadPool;
        this.indicesService = indexServices;
    }

    @Override
    protected void doExecute(AppendLuceneRequest request, ActionListener<AppendLuceneResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> {
            doExecuteCore(request, listener);
        });
    }

    private void doExecuteCore(AppendLuceneRequest request, ActionListener<AppendLuceneResponse> listener) {
        try {
            // 对请求做check
            request.check();

            // 获得shard信息
            ShardId shardId = new ShardId(request.indexName, request.uuid, request.shardId);
            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard == null) {
                throw new Exception("shard not found, indexName:" + request.indexName + ", shardId:" + request.shardId);
            }
//            shard.close("",true);
//            shard.flushAndCloseEngine();



//            shard.sync();
//            shard.flush()
            Store store = shard.store();
            logger.info("store.dir =" + store.directory().toString());

            Directory directory = store.directory();
            shard.flushAndCloseEngine();

            IndexWriterConfig iwc = new IndexWriterConfig();
//            iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
//            IndexWriter indexWriter = new IndexWriter(store.directory(), iwc);


//            // 获得lucene的IndexWriter对象
//            /* FIXME 这里需要修改es的代码, 将lucene的IndexWriter对象暴露给plugin使用  */
//            InternalEngine engine = (InternalEngine) shard.getEngine();
//            IndexWriter indexWriter = engine.getIndexWriter();

            // 处理主键冲突情况
            long deleteCount = -1;
            List<String> appendDirs = request.getAppendDirs();
//            if (request.primeKey != null && request.primeKey.length() > 0) {
//                deleteCount = doPrimerKey(appendDirs, indexWriter, request.primeKey);
//            }
//
            // 将新的lucene文件加入到shard中
            Directory[] indexes = new Directory[appendDirs.size()];
            for (int i = 0; i < appendDirs.size(); i++) {
                indexes[i] = FSDirectory.open(Paths.get(appendDirs.get(i)));
            }




            IndexWriter indexWriter = new IndexWriter(directory, iwc);

            indexWriter.addIndexes(indexes);
            indexWriter.commit();
            indexWriter.flush();
            indexWriter.close();
            // 构建response
            AppendLuceneResponse response = new AppendLuceneResponse();
            response.deleteCount = deleteCount;
//            response.
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /*
     * 对比新加的lucene文件和现有shard数据是否有主键冲突的情况
     * 如果存在冲突，则删除shard数据中对应的主键数据
     */
    public long doPrimerKey(List<String> dirs, IndexWriter dstIndexWriter, String primeKey) throws IOException {
        if (dstIndexWriter.numDocs() == 0) {
            return -1;
        }

        long count = 0;
        for (String appendDir : dirs) {
            IndexWriter srcIndexWriter = null;
            StandardDirectoryReader srcReader = null;
            StandardDirectoryReader dstReader = null;
            try {
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig(null);
                indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
                srcIndexWriter = new IndexWriter(FSDirectory.open(Paths.get(appendDir)), indexWriterConfig);

                srcReader = (StandardDirectoryReader) DirectoryReader.open(srcIndexWriter);
                dstReader = (StandardDirectoryReader) DirectoryReader.open(dstIndexWriter);

                // 遍历src中各个segment
                for (LeafReaderContext srcLeafReaderContext : srcReader.leaves()) {
                    LeafReader srcLeafReader = srcLeafReaderContext.reader();

                    Terms srcTerms = srcLeafReader.terms(primeKey);
                    if (srcTerms == null) {
                        continue;
                    }
                    TermsEnum srcTermsEnum = srcTerms.iterator();

                    // 遍历单个segment中各个主键
                    while (srcTermsEnum.next() != null) {
                        Term srcTerm = new Term(primeKey, srcTermsEnum.term());

                        // 判断dst中是否存在相同的主键
                        for (LeafReaderContext dstleafReaderContext : dstReader.leaves()) {
                            LeafReader dstLeafReader = dstleafReaderContext.reader();
                            if (dstLeafReader.postings(srcTerm) != null) {
                                // 如果碰到相同主键，则删除dst中对应的主键
                                dstIndexWriter.deleteDocuments(srcTerm);
                                count++;
                                break;
                            }
                        }
                    }
                }

            } finally {
                if (srcReader != null) {
                    srcReader.close();
                }

                if (dstReader != null) {
                    dstReader.close();
                }

                if (srcIndexWriter != null) {
                    srcIndexWriter.close();
                }
            }
        }

        return count;
    }
}
