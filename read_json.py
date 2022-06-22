import json

unique_terms = set()

with open('raw_data.json', 'r') as f:
    js = json.load(f)

    for record in js['response']['docs']:
        if 'species' in record:
            term = tuple(record['species'])

            if term not in unique_terms:
                unique_terms.add(term)
                print(term)
