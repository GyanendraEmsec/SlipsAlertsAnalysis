import elasticsearch
from elasticsearch import Elasticsearch

es5 = Elasticsearch(
    cloud_id="xxxxxxxxxxxxxxx",
    basic_auth=("elastic", "xxxxxxxxxxxxxxx"),
    request_timeout=600
)

index_name = "slips_backup_test"

query1 = {
    "size": 0, 
    "aggs": {
        "threat_levels": {
            "terms": {
                "script": {
                    "source": """
                        if (doc['data._source.Attach.Content.keyword'].size() > 0) {
                            def content = doc['data._source.Attach.Content.keyword'].value;
                            if (content.contains('threat level: high')) {
                                return 'high';
                            } else if (content.contains('threat level: medium')) {
                                return 'medium';
                            } else if (content.contains('threat level: info')) {
                                return 'info';
                            } else if (content.contains('threat level: critical')) {
                                return 'critical';
                            } else if (content.contains('threat level: low')) {
                                return 'low';
                            } else {
                                return 'other';
                            }
                        } else {
                            return 'other';
                        }
                    """,
                    "lang": "painless"
                },
                "size": 100  # Adjust as necessary to get more distinct threat levels
            }
        }
    }
}

result = es5.search(index=index_name, body=query1)

threat_levels = result['aggregations']['threat_levels']['buckets']

for bucket in threat_levels:
    print(f"Threat_level: {bucket['key']}, Count: {bucket['doc_count']}")
