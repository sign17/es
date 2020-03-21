package cn.sign.elasticsearch;

import cn.sign.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

@Configuration
public class ElasticClient implements Closeable {

    private static final String INDEX_KEY = "index";
    private static final String INDEX = "test_index";
    private static final String TYPE = "_doc";
    private static final String TIMESTAMP = "timestamp";
    private static final String ANALYZER = "standard";

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticClient.class);

    @Autowired
    private RestHighLevelClient client;

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(client)) {
            client.close();
        }
    }

    /**
     * 创建索引(默认分片数为5和副本数为1)
     * @param indexName
     * @throws IOException
     */
    public void createIndex(String indexName) throws IOException {
        if (checkIndexExists(indexName)) {
            LOGGER.error("\"index={}\"索引已经存在！", indexName);
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        // 指示是否所有节点都已确认请求
        boolean acknowledged = response.isAcknowledged();
        // 指示是否在超时之前为索引中的每个分片启动了必需的分片副本数
        boolean shardsAcknowledged = response.isShardsAcknowledged();
        if (acknowledged || shardsAcknowledged) {
            LOGGER.info("创建索引成功！索引名称为{}", indexName);
        }
    }

    /**
     * 创建索引
     * @param indexName
     * @param mapping setting配置
     * @param setting mapping配置
     * @throws IOException
     */
    public void createIndex(String indexName, String mapping, String setting) throws IOException {
        if (checkIndexExists(indexName)) {
            LOGGER.error("\"index={}\"索引已经存在！", indexName);
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        if (!Utils.validate(mapping)) {
            LOGGER.error("非法的json字符串！");
            return;
        }
        if (!Utils.validate(setting)) {
            LOGGER.error("非法的json字符串！");
            return;
        }
        request.mapping(mapping,XContentType.JSON);
        request.settings(setting,XContentType.JSON);

//        request.mapping(generateMappingProperties(map));

        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        // 是否在超时之前为索引中的每个分片启动了必需的分片副本数
        boolean shardsAcknowledged = response.isShardsAcknowledged();
        if (acknowledged || shardsAcknowledged) {
            LOGGER.info("创建索引成功！索引名称为{}", indexName);
        }
    }

    /**
     * 创建索引
     * @param indexName
     * @param shards
     * @param replicas
     * @throws IOException
     */
    public void createIndex(String indexName, int shards, int replicas) throws IOException {
        if (checkIndexExists(indexName)) {
            LOGGER.error("\"index={}\"索引已存在！", indexName);
            return;
        }
        Settings.Builder builder = Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", replicas);
        CreateIndexRequest request = new CreateIndexRequest(indexName).settings(builder);
//        request.mapping(generateBuilder());
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        if (response.isAcknowledged() || response.isShardsAcknowledged()) {
            LOGGER.info("创建索引成功！索引名称为{}", indexName);
        }
    }

    /**
     * 删除索引
     * @param indexName
     * @throws IOException
     */
    public void deleteIndex(String indexName) throws IOException {
        try {
            AcknowledgedResponse response = client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                LOGGER.info("{} 索引删除成功！", indexName);
            }
        } catch (ElasticsearchException ex) {
            if (ex.status() == RestStatus.NOT_FOUND) {
                LOGGER.error("{} 索引名不存在", indexName);
            }
            LOGGER.error("删除失败！");
        }
    }

    /**
     * 判断索引是否存在
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean checkIndexExists(String indexName) {
        GetIndexRequest request = new GetIndexRequest(indexName);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOGGER.error("操作异常！");
        }
        return false;
    }

    /**
     * 开启索引
     * @param indexName
     * @throws IOException
     */
    public void openIndex(String indexName) throws IOException{
        if (!checkIndexExists(indexName)) {
            LOGGER.error("索引不存在！");
            return;
        }
        OpenIndexRequest request = new OpenIndexRequest(indexName);
        OpenIndexResponse response = client.indices().open(request, RequestOptions.DEFAULT);
        if (response.isAcknowledged() || response.isShardsAcknowledged()) {
            LOGGER.info("{} 索引开启成功！", indexName);
        }
    }

    /**
     * 关闭索引
     * @param indexName
     * @throws IOException
     */
    public void closeIndex(String indexName) throws IOException {
        if (!checkIndexExists(indexName)) {
            LOGGER.error("索引不存在！");
            return;
        }
        CloseIndexRequest request = new CloseIndexRequest(indexName);
        CloseIndexResponse response = client.indices().close(request, RequestOptions.DEFAULT);
        if (response.isAcknowledged()) {
            LOGGER.info("{} 索引已关闭！", indexName);
        }
    }



    /**
     * 设置文档映射(设置字段的数据类型)
     * @param index
     * @throws IOException
     */
    public void setMapping(String index, Map<String, Object> fields) {
        if (!checkIndexExists(index)) {
            LOGGER.error("索引不存在！");
            return;
        }
        PutMappingRequest request = new PutMappingRequest(index);
        try {
//            request.source(source, XContentType.JSON);
//            request.source(generateMappingProperties(fields));
            request.source(fields);
            AcknowledgedResponse response = client.indices().putMapping(request, RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                LOGGER.info("已成功对\"index={}\"的文档设置类型映射！", index);
            }
        } catch (IOException e) {
            LOGGER.error("\"index={}\"的文档设置类型映射失败！", index);
        }
    }

    /**
     * 创建mapping语句
     * @param fields
     * @return
     * @throws IOException
     */
//    private XContentBuilder generateMappingProperties(Map<String, Map<String, String>> fields) throws IOException {
//
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        builder.startObject("properties");
//
//        if (!fields.isEmpty()){
//            for (Map.Entry<String, Map<String, String>> field: fields.entrySet()) {
//                if (null != field.getKey()){
//                    builder.startObject(field.getKey());
//                    if (!field.getValue().isEmpty()){
//                        for (Map.Entry<String, String> filed : field.getValue().entrySet()){
//                            String pro = filed.getKey();
//                            String value = filed.getValue();
//                            if( null != pro && null != value) {
//                                builder.field(pro,value);
//                            }
//                        }
//                    }
//                    builder.endObject();
//                }
//            }
//        }
//        builder.endObject();
//        builder.endObject();
//        return builder;
//    }

    /**
     * 增加文档
     * @param indexName
     * @param id
     * @param jsonString
     */
    public void addDocByJson(String indexName, String id, String jsonString) throws IOException{
        if (!Utils.validate(jsonString)) {
            LOGGER.error("非法的json字符串！");
            return;
        }
        if (!checkIndexExists(indexName)) {
            createIndex(indexName);
        }

        IndexRequest request=new IndexRequest(indexName);
        request.id(id).opType("create").source(jsonString, XContentType.JSON);
        // request的opType默认是INDEX(传入相同id会覆盖原document，CREATE则会将旧的删除)
        // request.opType(DocWriteRequest.OpType.CREATE)
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);

            String index = response.getIndex();
            String documentId = response.getId();
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                LOGGER.info("新增文档成功！ index: {}, id: {}", index , documentId);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                LOGGER.info("修改文档成功！ index: {}, id: {}", index , documentId);
            }
            // 分片处理信息
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                LOGGER.error("文档未写入全部分片副本！");
            }
            // 如果有分片副本失败，可以获得失败原因信息
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    LOGGER.error("副本失败原因：{}", reason);
                }
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本异常！");
            }
            LOGGER.error("文档新增失败！");
        }
    }

    /**
     * 查找文档
     * @param index
     * @param id
     * @return
     * @throws IOException
     */
    public Map<String, Object> getDocument(String index, String id) throws IOException{
        Map<String, Object> resultMap = new HashMap<>();
        GetRequest request = new GetRequest(index, id);
        // 实时(否)
        request.realtime(false);
        // 检索之前执行刷新(是)
        request.refresh(true);

        GetResponse response = null;
        try {
            response = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                LOGGER.error("文档未找到！" );
            }
            if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本冲突！" );
            }
            LOGGER.error("查找失败！");
        }

        if(Objects.nonNull(response)) {
            if (response.isExists()) { // 文档存在
                resultMap = response.getSourceAsMap();
            } else {
                LOGGER.error("文档未找到！" );
            }
        }
        return resultMap;
    }

    /**
     * 文档查询
     * @param index 索引
     * @param query query构造器
     * @param sort sort构造器
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> search(String index, QueryBuilder query, FieldSortBuilder sort) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(TimeValue.timeValueMinutes(2L));
        searchBuilder.query(query);
        if (Objects.isNull(sort)) {
            //默认id倒序
            searchBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.DESC));
        }

        SearchRequest request = new SearchRequest(index);
        request.source(searchBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        int failedShards = response.getFailedShards();
        if (failedShards > 0) {
            LOGGER.error("部分分片副本处理失败！");
            for (ShardSearchFailure failure : response.getShardFailures()) {
                String reason = failure.reason();
                LOGGER.error("分片处理失败原因：{}", reason);
            }
        }
        List<Map<String, Object>> list = parseSearchResponse(response);
        return list;
    }

    /**
     * 文档查询
     * @param index 索引
     * @param query query构造器
     * @param sort sort构造器
     * @param pageNum 页码
     * @param pageSize 每页条数
     * @return
     * @throws IOException
     */
    public EsPage<Map<String, Object>> search(String index, QueryBuilder query, FieldSortBuilder sort, Integer pageNum, Integer pageSize) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(TimeValue.timeValueMinutes(2L));
        searchBuilder.query(query);
        if (Objects.isNull(sort)) {
            //默认id倒序
            searchBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.DESC));
        }

        if (Objects.isNull(pageNum)) {
            pageNum = 0;
        }
        if (Objects.isNull(pageSize)) {
            pageSize = 10;
        }
        searchBuilder.from(pageNum).size(pageSize);
        SearchRequest request = new SearchRequest(index);
        request.source(searchBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        int failedShards = response.getFailedShards();
        if (failedShards > 0) {
            LOGGER.error("部分分片副本处理失败！");
            for (ShardSearchFailure failure : response.getShardFailures()) {
                String reason = failure.reason();
                LOGGER.error("分片处理失败原因：{}", reason);
            }
        }

        List<Map<String, Object>> list = parseSearchResponse(response);
        long totalRecord = response.getHits().getTotalHits().value;

        EsPage<Map<String, Object>> page = new EsPage<>();
        page.setPageNum(pageNum);
        page.setPageSize(pageSize);
        page.setData(list);
        page.setTotal(totalRecord);
        return page;
    }

    /**
     * 删除文档
     * @param index
     * @param id
     * @throws IOException
     */
    public void deleteDocument(String index, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, id);
        DeleteResponse response = null;
        try {
            response = client.delete(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            LOGGER.error("删除失败!");
        }
        if (Objects.nonNull(response)) {
            if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                LOGGER.error("不存在该文档！");
            }
            LOGGER.info("文档已删除！");
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                LOGGER.error("部分分片副本未处理");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    LOGGER.error("失败原因：{}", reason);
                }
            }
        }
    }

    /**
     * 脚本语句更新文档
     * @param index
     * @param id
     * @param script
     */
    public void updateDocByScript(String index, String id, String script) throws IOException{
        Script inline = new Script(script);
        UpdateRequest request = new UpdateRequest(index, id).script(inline);
        try {
            UpdateResponse response  = client.update(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                LOGGER.info("文档更新成功！");
            } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
                LOGGER.error("\"index={},id={}\"的文档已被删除！", response.getIndex(), response.getId());
            } else if(response.getResult() == DocWriteResponse.Result.NOOP) {
                LOGGER.error("操作没有被执行！");
            }

            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                LOGGER.error("部分分片副本未处理");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    LOGGER.error("未处理原因：{}", reason);
                }
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                LOGGER.error("不存在这个文档！" );
            } else if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本冲突异常！" );
            }
            LOGGER.error("更新失败！");
        }
    }

    /**
     * 通过一个JSON字符串更新文档(如果该文档不存在，则创建这个文档)
     * @param index
     * @param id
     * @param jsonString
     * @throws IOException
     */
    public void updateDocByJson(String index, String id, String jsonString) throws IOException {
        if (!Utils.validate(jsonString)) {
            LOGGER.error("非法的json字符串！");
            return;
        }
        if (!checkIndexExists(index)) {
            createIndex(index);
        }
        UpdateRequest request = new UpdateRequest(index, id);
        request.doc(jsonString, XContentType.JSON);
        // 如果要更新的文档不存在，则根据传入的参数新建一个文档
        request.docAsUpsert(true);
        try {
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
            String indexName = response.getIndex();
            String documentId = response.getId();
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                LOGGER.info("文档新增成功！index: {}, id: {}", indexName, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                LOGGER.info("文档更新成功！");
            } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
                LOGGER.error("\"index={},id={}\"的文档已被删除！", indexName, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                LOGGER.error("操作没有被执行！");
            }

            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                LOGGER.error("分片副本未全部处理");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    LOGGER.error("未处理原因：{}", reason);
                }
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                LOGGER.error("不存在这个文档！" );
            } else if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本冲突异常！" );
            }
            LOGGER.error("更新失败！");
        }
    }

    /**
     * 批量增加文档
     * @param params
     * @throws IOException
     */
    public void bulkAdd(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            String jsonString = dataMap.get("json");
            if (StringUtils.isNotBlank(id) && Utils.validate(jsonString)) {
                IndexRequest request=new IndexRequest(index).id(id).opType("create").source(jsonString, XContentType.JSON);
                bulkRequest.add(request);
            }
        }
        // 超时时间(2分钟)
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        // 刷新策略
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        if (bulkRequest.numberOfActions() == 0) {
            LOGGER.error("批量增加操作失败！");
            return;
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 全部操作成功
        if (!bulkResponse.hasFailures()) {
            LOGGER.info("批量增加操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    LOGGER.error("\"index={}, id={}\"的文档增加失败！", failure.getIndex(), failure.getId());
                    LOGGER.error("增加失败详情: {}", failure.getMessage());
                } else {
                    LOGGER.info("\"index={}, id={}\"的文档增加成功！", bulkItemResponse.getIndex(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量更新文档
     * @param params
     * @throws IOException
     */
    public void bulkUpdate(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            String jsonString = dataMap.get("json");
            if (StringUtils.isNotBlank(id) && !Utils.validate(jsonString)) {
                UpdateRequest request = new UpdateRequest(index, id).doc(jsonString, XContentType.JSON);
                request.docAsUpsert(true);
                bulkRequest.add(request);
            }
        }
        if (bulkRequest.numberOfActions() == 0) {
            LOGGER.error("批量更新操作失败！");
            return;
        }
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (!bulkResponse.hasFailures()) {
            LOGGER.info("批量更新操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    LOGGER.error("\"index={}, id={}\"的文档更新失败！", failure.getIndex(), failure.getId());
                    LOGGER.error("更新失败: {}", failure.getMessage());
                } else {
                    LOGGER.info("\"index={}, id={}\"的文档更新成功！", bulkItemResponse.getIndex(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量删除文档
     * @param params
     * @throws IOException
     */
    public void bulkDelete(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            if (StringUtils.isNotBlank(id)){
                DeleteRequest request = new DeleteRequest(index, id);
                bulkRequest.add(request);
            }
        }
        if (bulkRequest.numberOfActions() == 0) {
            LOGGER.error("操作失败！");
            return;
        }
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (!bulkResponse.hasFailures()) {
            LOGGER.info("批量删除操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    LOGGER.error("\"index={}, id={}\"的文档删除失败！", failure.getIndex(), failure.getId());
                    LOGGER.error("删除失败: {}", failure.getMessage());
                } else {
                    LOGGER.info("\"index={}, id={}\"的文档删除成功！", bulkItemResponse.getIndex(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量查找文档
     * @param params
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> multiGet(List<Map<String, String>> params) throws IOException {
        List<Map<String, Object>> resultList = new ArrayList<>();

        MultiGetRequest request = new MultiGetRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            if (StringUtils.isNotBlank(id)) {
                request.add(new MultiGetRequest.Item(index, id));
            }
        }
        request.realtime(false);
        request.refresh(true);
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        List<Map<String, Object>> list = parseMGetResponse(response);
        if (!list.isEmpty()) {
            resultList.addAll(list);
        }
        return resultList;
    }

    private List<Map<String, Object>> parseMGetResponse(MultiGetResponse response) {
        List<Map<String, Object>> list = new ArrayList<>();
        MultiGetItemResponse[] responses = response.getResponses();
        for (MultiGetItemResponse item : responses) {
            GetResponse getResponse = item.getResponse();
            if (Objects.nonNull(getResponse)) {
                if (!getResponse.isExists()) {
                    LOGGER.error("\"index={}, id={}\"的文档查找失败！", getResponse.getIndex(), getResponse.getId());
                } else {
                    list.add(getResponse.getSourceAsMap());
                }
            } else {
                MultiGetResponse.Failure failure = item.getFailure();
                ElasticsearchException e = (ElasticsearchException) failure.getFailure();
                if (e.status() == RestStatus.NOT_FOUND) {
                    LOGGER.error("\"index={}, id={}\"的文档不存在！", failure.getIndex(), failure.getId());
                } else if (e.status() == RestStatus.CONFLICT) {
                    LOGGER.error("\"index={}, id={}\"的文档版本冲突！", failure.getIndex(), failure.getId());
                }
            }
        }
        return list;
    }

    private List<Map<String, Object>> parseSearchResponse(SearchResponse response){
        List<Map<String, Object>> resultList = new ArrayList<>();
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            resultList.add(hit.getSourceAsMap());
        }
        return resultList;
    }

}