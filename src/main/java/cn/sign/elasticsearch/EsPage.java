package cn.sign.elasticsearch;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class EsPage<T> {
    private Integer pageNum;
    private Integer pageSize;
    private Long total;
    private List<Map<String, Object>> data;
}
