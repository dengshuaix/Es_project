# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch

es = Elasticsearch()

es.get(index='s23', id=1)
