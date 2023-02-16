# -*- coding: utf-8 -*-
################
# Es 操作篇
# 1. 结果过滤 , 返回es结果做过滤,以及优化内容
# 2. es 处理索引信息
# 3. Indices 关于索引的细节操作, 自定义mappings
# 4. Cluster 集群操作
# 5. Nodes 节点操作
# 6. Cat API , 一般返回结果都是json类型,cat更简洁
# 7. Snapshot 快照(es数据备份),单个索引/整个集群的快照
# 8. Task Management Api 任务管理API
################


from elasticsearch import Elasticsearch
from es_config import ES_HOST, ES_PORT

es_server = Elasticsearch([{"host": ES_HOST, "port": ES_PORT}])

# 1. 结果过滤 , filter_path

# {'took': 13, 'timed_out': False, '_shards': {'total': 5, 'successful': 5, 'skipped': 0, 'failed': 0}, 'hits': {'total': 1, 'max_score': 1.0, 'hits': [{'_index': 's23', '_type': 'doc', '_id': '1', '_score': 1.0, '_source': {'name': 'ds', 'age': 18}}]}}
print(es_server.search(index='s23', doc_type='doc'))  # 指定类型

#  # filter_path , 省略type类型
print(es_server.search(index='s23', filter_path=['hits.total', 'hits.hits._source']))
result1 = "{'hits': {'total': 1, 'hits': [{'_source': {'name': 'ds', 'age': 18}}]}}"

# 通配符 *
print(es_server.search(index='s23', filter_path=['hits.hits.*']))
result2 = {'hits': {'hits': [
    {'_index': 's23', '_type': 'doc', '_id': '1', '_score': 1.0, '_source': {'name': 'ds', 'age': 18}}
]}}

# 2. Es 对象
# es.index 向指定的索引添加文档/更新文档,如果索引不存在则会新建一个
print(es_server.index(index='w2', doc_type='doc', id='4', body={"name": "coco", "age": 12}))
print(es_server.index(index='w2', doc_type='doc', id='5', body={"name": "kaka", "age": 12}))
print(es_server.index(index='w2', doc_type='doc', body={"name": "鸣人", "age": 22}))  # 可以不指定id，默认生成一个id

# es.get 查询索引中的文档
print(es_server.get(index='w2', doc_type='doc', id=4))

# es.search 搜索查询
# - index : 指定索引, 使用 `_all` 会从全部索引中查询
# - doc_type : 搜索文档的类型 , 高版本es 已移除
# - body : Query DSL(查询特定语言)
# - _source : 返回 _source字段的true 或者 false, 或者返回字段列表
# - _source_exclude : 排除哪些字段
# - _source_include : 包含哪些字段

print(es_server.search(index='w2', doc_type='doc', body={"query": {"match": {"age": 22}}}))
print(es_server.search(index='w2', doc_type='doc', body={"query": {"match": {"age": 22}}}, _source=['name', 'age']))
print(es_server.search(index='w2', doc_type='doc', body={"query": {"match": {"age": 22}}}, _source_includes=['age']))
print(es_server.search(index='w2', doc_type='doc', body={"query": {"match": {"age": 22}}}, _source_excludes=['age']))

# es.get_source 通过索引,类型和id获取文档来源,返回结果是字典
print(es_server.get_source(index='w2', doc_type='doc', id=4))

# es.count 执行查询,并返回统计数
count_body = {
    "query": {
        "match": {
            "age": 22
        }
    }
}
print(es_server.count(index='w2', doc_type='doc', body=count_body))
print(es_server.count(index='w2', doc_type='doc', body=count_body)['count'])
print(es_server.count(index='w2'))
print(es_server.count(index='w2', doc_type='doc'))

# es.create 创建索引,并新增一条记录,重复使用会报错
# print(es_server.create(index='w3', doc_type='doc', id=1, body={"name": "lisi", "age": 18}))
# print(es_server.get(index='w3', doc_type='doc', id=1))

# es.delete 删除指定文档,但是不能删除索引(删除db失败,需要使用es.indices.delete)
print(es_server.delete(index='w2', doc_type='doc', id=4))

# es.delete_by_query 删除与查询匹配的所有文档
print(es_server.delete_by_query(index='w2', doc_type='doc', body={
    "query": {"match": {"age": "22"}}
}))

# es.exists 查询es中是否存在指定文档,返回布尔值
print(es_server.exists(index='w2', doc_type='doc', id='1'))

# es.info 获取集群信息
print(es_server.info())

# es.ping 查询集群是否启动,布尔值
print(es_server.ping())
