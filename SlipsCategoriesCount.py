from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt

# Elasticsearch connection
es5 = Elasticsearch(
    cloud_id="xxxxxxxxxxxxxxx",
    basic_auth=("elastic", "xxxxxxxxxxxxxxx"),
    request_timeout=600
)

index_name = "slips_backup_test"

# Aggregation query to count records and fetch an example 'Attach.Content' for each category
agg_query = {
    "size": 0,  # No need to return individual documents
    "aggs": {
        "category_count": {
            "terms": {
                "field": "data._source.Category.keyword",  # Correct field path for category
                "size": 10  # Adjust size to capture all categories
            },
            "aggs": {
                "first_example": {
                    "top_hits": {
                        "size": 1,  # Get only the first document
                        "_source": ["data._source.Attach.Content"]  # Limit fields to 'Attach.Content'
                    }
                }
            }
        }
    }
}

# Execute the aggregation query
response = es5.search(index=index_name, body=agg_query)

# Extract and print category counts and first record example
categories = response['aggregations']['category_count']['buckets']
categoryNames = []
counts = []
for category in categories:
    category_name = category['key']
    category_count = category['doc_count']
    
    # Get the example alert description
    top_hits = category['first_example']['hits']['hits']
    
    if top_hits:
        attach_content = top_hits[0]['_source']['data']['_source']['Attach'][0]['Content'] if 'data' in top_hits[0]['_source'] and 'Attach' in top_hits[0]['_source']['data']['_source'] and top_hits[0]['_source']['data']['_source']['Attach'] else 'No description available'
    else:
        attach_content = 'No description available'
    
    print(f"Category: {category_name}, Count: {category_count}, Example Alert Description: {attach_content}")
    categoryNames.append(category_name)
    counts.append(category_count)

plt.figure(figsize=(13,10))
bars = plt.bar(categoryNames, counts, color=['blue', 'green', 'orange', 'red'])

# Adding counts on top of bars
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 500, int(yval), ha='center', va='bottom')

# Title and labels
plt.title('Distribution of All Slips Alerts Over Categories')
plt.xlabel('Categories')
plt.ylabel('Alerts Count')

# Display the plot
plt.show()
