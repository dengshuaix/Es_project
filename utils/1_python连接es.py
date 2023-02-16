# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch

################
# 连接 es 服务
################
from es_config import ES_HOST, ES_PORT

# es_server = Elasticsearch() # 本地连接
es_server = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])

es_server = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}],
                          # sniff_on_start=True,  # 连接前测试 (造成本地连接出现卡的现象，暂时不清楚问题原因！！todo)
                          sniff_on_connection_fail=True,  # 节点无响应时，刷新节点
                          sniff_timeout=60  # 设置超时时间
                          )
# 创建一个索引(索引==db),并插入一条id为1的记录
es_server.index(index='s23', doc_type='doc', id=1, body={"name": "ds", "age": 18})

# 查询 id为1记录
data1 = es_server.get(index='s23', doc_type='doc', id=1)
print(data1)

################
# 状态码配置
################

es_conn = Elasticsearch({'host': ES_HOST, 'port': ES_PORT}, ignore=400)  # 忽略返回的400状态码
es_conn = Elasticsearch({'host': ES_HOST, 'port': ES_PORT}, ignore=[400, 405, 502])  # 以列表的形式忽略多个状态码
