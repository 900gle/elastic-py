from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import pprint as ppr
import json


def deleteIndex():
    # Elasticsearch에 있는 모든 Index 조회
    print(client.indices.delete(index=INDEX_NAME, ignore=[400, 404]))

def dataInsert():
    # ===============
    # 데이터 삽입
    # ===============
    count=0
    docs = []
    with open(DATA_FILE) as data_file:
        for line in data_file:
            line = line.strip()


            doc = json.loads(line)
            # if doc["type"] != "question":
            #     continue

            docs.append(doc)
            count += 1

            if count % BATCH_SIZE == 0:
                index_batch(docs)
                docs = []
                print("Indexed {} documents.".format(count))

        if docs:
            index_batch(docs)
            print("Indexed {} documents.".format(count))

        client.indices.refresh(index=INDEX_NAME)
        print("Done indexing.")

def index_batch(docs):
    names = [doc["name"] for doc in docs]
    name_vectors = names

    requests = []
    for i, doc in enumerate(docs):
        request = doc
        request["_op_type"] = "index"
        request["_index"] = INDEX_NAME

        print(request)
        requests.append(request)
    bulk(client, requests)
    client.indices.update_aliases({
        "actions": [
            { "add":    { "index": "homeplus", "alias": "item" }}
        ]
    })

def searchAll(cls, indx=None):
    # ===============
    # 데이터 조회 [전체]
    # ===============
    res = cls.es.search(
        index="homeplus", doc_type="_doc",
        body={
            "query": {"match_all": {}}
        }
    )
    print(json.dumps(res, ensure_ascii=False, indent=4))

def searchFilter():
    # ===============
    # 데이터 조회 []
    # ===============
    res = client.search(
        index=INDEX_NAME, doc_type="_doc",
        body={
            "query" : {
                "term": {
                    "name": {
                        "value": "brown"
                    }
                }
            },
            "sort" : {
                "_script" : {
                    "type" : "number",
                    "script": {
                        "source": "payload_sort",
                        "lang" : "sort_script",
                        "params": {
                            "field": "name",
                            "value": "brown"
                        }
                    },
                    "order" : "desc"
                }
            }
        }
    )



    ppr.pprint(res)

def createIndex():
    # ===============
    # 인덱스 생성
    # ===============
    client.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "name": {
                        "type": "text",
                        "term_vector": "with_positions_offsets_payloads",
                        "analyzer": "payload_analyzer"
                    }
                }
            },
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "analysis": {
                    "analyzer": {
                        "payload_analyzer": {
                            "type": "custom",
                            "tokenizer": "payload_tokenizer",
                            "filter": ["payload_filter"]
                        }
                    },
                    "tokenizer": {
                        "payload_tokenizer": {
                            "type": "whitespace",
                            "max_token_length": "64"
                        }
                    },
                    "filter": {
                        "payload_filter": {
                            "type": "delimited_payload",
                            "encoding": "float",
                            "delimiter" : "|"
                        }
                    }
                }
            }
        }
    )



if __name__ == '__main__':
    INDEX_NAME = "homeplus"
    DATA_FILE = "../data/delimiter_data.json"
    BATCH_SIZE = 1000
    # ElaAPI.allIndex()
    # ElaAPI.srvHealthCheck()
    client = Elasticsearch(hosts='localhost', port='9200', http_auth=('elastic', 'dlengus'))

    deleteIndex()
    createIndex()
    dataInsert()
    # searchFilter()
    # ElaAPI.searchAll()
    # ElaAPI.searchFilter()
