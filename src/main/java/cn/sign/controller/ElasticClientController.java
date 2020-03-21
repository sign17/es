package cn.sign.controller;

import cn.sign.elasticsearch.ElasticClient;
import cn.sign.elasticsearch.EsPage;
import cn.sign.utils.ResultMap;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("es")
public class ElasticClientController {

    @Autowired
    private ElasticClient elasticClient;

    @RequestMapping("createIndex")
    public ResultMap createIndex() throws IOException {
        String setting = "{\"analysis\": {\"char_filter\": {\"n_to_l\": {\"type\": \"mapping\",\"mappings\": [\"n=> l \"]}},\"filter\": {\"my_stopwords\": {\"type\": \"stop\",\"stopwords\": [\"the\"]}},\"analyzer\": {\"my_analyzer\": {\"type\": \"custom\",\"char_filter\": [\"n_to_l\"],\"tokenizer\": \"standard\",\"filter\": [\"my_stopwords\"]}}}}";
        String mapping = "{\"properties\": {\"id\": {\"type\": \"long\"}}}";
        elasticClient.createIndex("test_index",mapping, setting);
        return ResultMap.success();
    }

    @RequestMapping("deleteIndex")
    public ResultMap deleteIndex() throws IOException {
        elasticClient.deleteIndex("test_index");
        return ResultMap.success();
    }

    @RequestMapping("addDocByJson")
    public ResultMap addDocByJson(String id) throws IOException {
        elasticClient.addDocByJson("test_index",id,"{\"name\":\"nAME\",\"count\":"+id+"}");
        return ResultMap.success();
    }

    @RequestMapping("getDocument")
    public ResultMap getDocument(String id) throws IOException {
        Map<String, Object> res = elasticClient.getDocument("test_index",id);
        return ResultMap.success();
    }

    @RequestMapping("deleteDocument")
    public ResultMap deleteDocument(String id) throws IOException {
        elasticClient.deleteDocument("test_index",id);
        return ResultMap.success();
    }

    @RequestMapping("updateDocByScript")
    public ResultMap updateDocByScript(String id, String script) throws IOException {
        elasticClient.updateDocByScript("test_index",id,script);
        return ResultMap.success();
    }

    @RequestMapping("updateDocByJson")
    public ResultMap updateDocByJson(String id, String jsonString) throws IOException {
        elasticClient.updateDocByJson("test_index",id,jsonString);
        return ResultMap.success();
    }

    @RequestMapping("bulkAdd")
    public ResultMap bulkAdd() throws IOException {
        List<Map<String, String>> params = new ArrayList<>();
        HashMap map = new HashMap();
        map.put("index","test_index");
        map.put("id","5");
        map.put("json","{\"name\":\"nAME\",\"count\":1}");
        params.add(map);
        elasticClient.bulkAdd(params);
        return ResultMap.success();
    }

    @RequestMapping("bulkUpdate")
    public ResultMap bulkUpdate() throws IOException {
        List<Map<String, String>> params = new ArrayList<>();
        HashMap map = new HashMap();
        map.put("index","test_index");
        map.put("id","5");
        map.put("json","{\"name\":\"nAME\",\"count\":2}");
        params.add(map);
        elasticClient.bulkUpdate(params);
        return ResultMap.success();
    }

    @RequestMapping("bulkDelete")
    public ResultMap bulkDelete() throws IOException {
        List<Map<String, String>> params = new ArrayList<>();
        HashMap map = new HashMap();
        map.put("index","test_index");
        map.put("id","5");
        params.add(map);
        elasticClient.bulkDelete(params);
        return ResultMap.success();
    }

    @RequestMapping("multiGet")
    public ResultMap multiGet() throws IOException {
        List<Map<String, String>> params = new ArrayList<>();
        HashMap map = new HashMap();
        map.put("index","test_index");
        map.put("id","1");
        params.add(map);
        elasticClient.multiGet(params);
        return ResultMap.success();
    }

    @RequestMapping("search")
    public ResultMap search() throws IOException {
        SearchRequest request = new SearchRequest("test_index");
        //构造bool查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", "name1"));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("count", "6"));
        boolQueryBuilder.should(QueryBuilders.rangeQuery("count").gte("3"));
        //对应filter
//        boolQueryBuilder.filter(QueryBuilders.rangeQuery("count").from(1).to(3));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //排序
//        searchSourceBuilder.sort(SortBuilders.fieldSort("count").order(SortOrder.DESC));
        List<Map<String, Object>> list = elasticClient.search("test_index",boolQueryBuilder, null);
        return ResultMap.success();
    }

    @RequestMapping("searchPage")
    public ResultMap searchPage() throws IOException {
        SearchRequest request = new SearchRequest("test_index");
        //构造bool查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", "name1"));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("count", "6"));
        boolQueryBuilder.should(QueryBuilders.rangeQuery("count").gte("3"));
        //对应filter
//        boolQueryBuilder.filter(QueryBuilders.rangeQuery("count").from(1).to(3));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //排序
//        searchSourceBuilder.sort(SortBuilders.fieldSort("count").order(SortOrder.DESC));
        EsPage<Map<String, Object>> list = elasticClient.search("test_index",boolQueryBuilder, null,0,10);
        return ResultMap.success();
    }

    @RequestMapping("setMapping")
    public ResultMap setMapping() throws IOException {

//        String source = "{\"properties\": {\"id\": {\"type\": \"text\"},\"name\": {\"type\": \"text\", \"analyzer\": \"standard\"}}}";
//        Map<String, Map<String, String>> map = new HashMap<>();
//        Map<String, String> map2 = new HashMap<>();
//        map2.put("type","long");
//        map.put("id",map2);

        Map<String, Object> map = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        Map<String, Object> map3 = new HashMap<>();
        map3.put("type","text");
        map2.put("message",map3);
        map.put("properties",map2);

        elasticClient.setMapping("test_index", map);

        return ResultMap.success();
    }
}
