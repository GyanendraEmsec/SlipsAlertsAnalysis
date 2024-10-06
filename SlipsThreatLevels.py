import elasticsearch
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt

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
threatLevels = []
counts = []
for bucket in threat_levels:
    print(f"Threat_level: {bucket['key']}, Count: {bucket['doc_count']}")
    threatLevels.append(bucket['key'])
    counts.append(bucket['doc_count'])

plt.figure(figsize=(13,10))
bars = plt.bar(threatLevels, counts, color=['blue', 'green', 'orange', 'red'])

# Adding counts on top of bars
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 500, int(yval), ha='center', va='bottom')

# Title and labels
plt.title('Distribution of All Slips Alerts Over Threat Levels')
plt.xlabel('Threat Levels')
plt.ylabel('Alerts Count')

# Display the plot
plt.show()
