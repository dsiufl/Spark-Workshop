import ijson
import json
import sys

fn = sys.argv[1]

i = 1
with open(fn) as f:
    hits = ijson.items(f, "item.hits.item")
    for h in hits:
        print(json.dumps(h))
#        if i >= 1000000:
#            break
#        else:
#            i = i + 1

#with open("data/HackerNews/small.json") as f: 
#    parser = ijson.parse(f)
#    for prefix, event, value in parser:
#        print(json.dumps(value))
