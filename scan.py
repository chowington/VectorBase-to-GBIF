################
# Python 3.7.2 #
################

import argparse
import csv
import os
import random
import requests
import json


def parse_args():
    """Parse the command line arguments and return an args namespace."""
    parser = argparse.ArgumentParser(description='Transforms a VectorBase search export file to '
                                                 'SCAN DwC format.')

    parser.add_argument('output', help='The name of the output file.')
    parser.add_argument('-s', '--sample', type=float,
                        help='Output a sample of the input data. Provide a number from 0-100 '
                             'representing the approximate size of the sample as a percentage of '
                             'the whole dataset.')
    parser.add_argument('-c', '--use-cached', action='store_true')
    args = parser.parse_args()

    if args.sample and not (0 <= args.sample <= 100):
        raise ValueError('Sampling percentage must be between 0 and 100.')

    return args


def make_scan(output, use_cached=False, sample=None):
    if sample:
        random.seed()

    output_rows = [
        'occurrenceID', 'catalogNumber', 'dataGeneralizations', 'basisOfRecord', 'individualCount',
        'sex', 'lifeStage', 'references', 'recordedBy', 'eventDate', 'verbatimEventDate',
        'samplingProtocol', 'country', 'stateProvince', 'locality', 'decimalLatitude',
        'decimalLongitude', 'identificationRemarks', 'scientificName', 'identificationQualifier',
        'occurrenceRemarks'
    ]

    solr_fields = [
        'sample_id_s',
        'projects',
        'species',
        'geo_coords',
        'country_s',
        'adm1_s',
        'adm2_s',
        'collection_protocols',
        'collection_day_s',
        'collection_date_range',
        'protocols',
        'sample_size_i',
        'exp_citations_ss',
        'tags_ss',
        'sex_s',
        'dev_stages_ss',
        'project_authors_txt'
    ]

    valid_first_species_terms = (
        'Aedeomyia', 'Aedes', 'Aedimorphus', 'Anopheles', 'Catageiomyia', 'Ceratopogonidae',
        'Chironomidae', 'Coquillettidia', 'Culex', 'Culicidae', 'Culicinae', 'Culiciomyia',
        'Culicoides', 'Culiseta', 'Eumelanomyia', 'Lophoceraomyia', 'Mansonia', 'Mimomyia',
        'Oculeomyia', 'Orthopodomyia', 'Phlebotomus', 'Psorophora', 'Sergentomyia', 'Simuliidae',
        'Toxorhynchites', 'Uranotaenia', 'Wyeomyia', 'Avaritia'
    )

    subspecies_terms = ('japonicus', 'arabiensis', 'vexans', 'S', 'T')

    group_terms = ('morphological', 'group', 'complex', 'sensu', 'lato', 'AD', 'BCE', 'subgroup')

    valid_provider_tags = (
        'Anastasia Mosquito Control District Florida',
        'Marion County Public Health Department Indiana',
        'Hernando County Florida Mosquito Control', 'Biogents Mosquito Surveillance',
        'Northwest Mosquito and Vector Control District', 'Collier Mosquito Control District',
        'Iowa State Mosquito Surveillance', 'Manatee County Florida Mosquito Control',
        'Cass County Vector Control District', 'Salt Lake City Mosquito Abatement District',
        'Southern Nevada Health District', 'Entomology Group Pirbright',
        'Orange County Florida Mosquito Control', 'ICEMR',
        'Rhode Island Department of Environmental Management',
        'South Walton County Florida Mosquito Control', 'Toledo Area Sanitary District',
        'Ada County Weed Pest and Mosquito Abatement',
        'Marion County Public Health Department, Indiana',
        'Lee County Mosquito Control',
        'Desplaines Valley Mosquito Abatement',
        'Anastasia Mosquito Control District, Florida',
        'Entomology Group, Pirbright',
        'Ada County Weed, Pest, and Mosquito Abatement'
    )

    skip_provider_tags = (
        'Lee County Mosquito Control',
        'Desplaines Valley Mosquito Abatement'
    )

    remove_tags = ('abundance', 'viral surveillance')

    temp = output + '.temp'

    try:
        if not use_cached:
            rows = 1000000

            url = 'http://localhost:8200/solr/collection1/select'
            url += '?fl=' + ','.join(solr_fields)
            url += ('&fq=bundle:pop_sample' +
                    '&fq=has_abundance_data_b:true' +
                    '&fq=has_geodata:true' +
                    '&fq=sample_size_i:%5B1%20TO%20*%5D' +
                    '&fq=site:%22Population%20Biology%22'
                    )
            url += '&q=*:*'
            url += '&rows=' + str(rows)

            print('Sending request...')
            response = requests.get(url)
            print('Done.')
            response.raise_for_status()

            with open('raw_data.json', 'w', newline='') as raw_data_file:
                raw_data_file.write(response.text)

            response = response.json()

        else:
            with open('raw_data.json') as raw_data_file:
                response = json.load(raw_data_file)

        with open(temp, 'w', newline='') as temp_file:
            temp_csv = csv.DictWriter(temp_file, output_rows)

            temp_csv.writeheader()

            i = 0

            for record in response['response']['docs']:
                defaults = {
                    'projects': [''],
                    'country_s': '',
                    'adm1_s': '',
                    'adm2_s': '',
                    'collection_protocols': [''],
                    'collection_day_s': '',
                    'collection_date_range': [''],
                    'protocols': [''],
                    'exp_citations_ss': [''],
                    'sex_s': '',
                    'dev_stages_ss': [''],
                    'project_authors_txt': ['']
                }

                defaults.update(record)
                record = defaults

                i += 1

                if not i % 1000:
                    print(i)

                # If we're sampling, drop the row with some probability
                if sample is not None and random.random() >= sample/100:
                    continue

                tags = []

                if 'tags_ss' in record:
                    tags = [tag for tag in record['tags_ss'] if tag not in remove_tags]

                    if len(tags) > 1 or tags[0] not in valid_provider_tags:
                        raise ValueError('Unexpected tag(s) {} at {}'
                                         .format(tags, record['sample_id_s']))

                # Discard the row based on certain conditions
                if (record['collection_protocols'] == 'BG-Counter trap catch'
                        or (tags and tags[0] in skip_provider_tags)):
                    continue

                # Directly set the fields that need little processing
                output_row = {
                    'occurrenceID': record['sample_id_s'],
                    'catalogNumber': record['sample_id_s'],
                    'dataGeneralizations': ';'.join(record['projects']),
                    'basisOfRecord': 'HumanObservation',
                    'individualCount': record['sample_size_i'],
                    'sex': record['sex_s'],
                    'lifeStage': ';'.join(record['dev_stages_ss']),
                    'references': ';'.join(record['exp_citations_ss']),
                    'recordedBy': ';'.join(record['project_authors_txt']),
                    'verbatimEventDate': ';'.join(record['collection_date_range']),
                    'samplingProtocol': ';'.join(record['collection_protocols']),
                    'identificationRemarks': ';'.join(record['protocols']),
                }

                # Species
                species_terms = record['species'][0].split()

                if species_terms[0] == 'genus' or species_terms[0] == 'subgenus':
                    del species_terms[0]

                if species_terms[0] not in valid_first_species_terms:
                    raise ValueError('Unknown first species term "{}" at {}'
                                     .format(species_terms[0], record['sample_id_s']))

                output_row['scientificName'] = species_terms[0]

                if len(species_terms) >= 2:
                    output_row['scientificName'] += ' ' + species_terms[1]

                    if len(species_terms) >= 3:
                        if species_terms[2] in subspecies_terms:
                            output_row['scientificName'] += ' ' + species_terms[2]
                        elif species_terms[2] in group_terms:
                            output_row['identificationQualifier'] = species_terms[2]
                        else:
                            ValueError('Unknown third species term "{}" at {}'
                                       .format(species_terms[2], record['sample_id_s']))

                        if len(species_terms) == 4:
                            if 'identificationQualifier' not in output_row:
                                output_row['identificationQualifier'] = ''

                            output_row['identificationQualifier'] += ' ' + species_terms[3]

                # Coordinates
                coordinates = record['geo_coords'].split(',')
                output_row['decimalLatitude'] = coordinates[0]
                output_row['decimalLongitude'] = coordinates[1]

                # Location
                output_row['country'] = record['country_s'].split(' (')[0]
                output_row['stateProvince'] = record['adm1_s'].split(' (')[0]
                output_row['locality'] = record['adm2_s'].split(' (')[0]

                # Date
                output_row['eventDate'] = record['collection_day_s'][:10]

                # occurrenceRemarks
                author_text = generator_text = ''
                link = 'https://www.vectorbase.org/popbio/map/?view=abnd&zoom_level=11'
                link += '&center=' + record['geo_coords']

                if tags:
                    author_text += ' authored by ' + tags[0]
                    link += '&tag=' + tags[0].replace(' ', '+')
                else:
                    for project in record['projects']:
                        link += '&projectID[]=' + project

                if record['exp_citations_ss']:
                    generator_text += (', including '
                                       + '; '.join(record['exp_citations_ss']))

                output_row['occurrenceRemarks'] = (
                    'This record has been curated by VectorBase.org as part of a larger data set{}'
                    ' which can be seen in context at {}. Please cite VectorBase and the original '
                    'data generator(s){}.'.format(author_text, link, generator_text)
                )

                # Write to output
                temp_csv.writerow(output_row)

        os.replace(temp, output)

    except ValueError:
        os.remove(temp)
        raise


if __name__ == '__main__':
    args = vars(parse_args())
    make_scan(**args)
