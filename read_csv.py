import csv

unique_terms = set()

with open('vb_scan_2020_11_12.csv', 'r') as f:
    reader = csv.DictReader(f)

    for line in reader:
        string = line['identificationRemarks']
        terms = string.split(';')

        for term in terms:
            if term not in unique_terms:
                unique_terms.add(term)
                print(term)
