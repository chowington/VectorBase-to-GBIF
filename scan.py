################
# Python 3.7.2 #
################

import argparse
import csv
import os
import random


def parse_args():
    """Parse the command line arguments and return an args namespace."""
    parser = argparse.ArgumentParser(description='Transforms a VectorBase search export file to '
                                                 'SCAN DwC format.')

    parser.add_argument('input', help='The CSV input file.')
    parser.add_argument('output', help='The name of the output file.')
    parser.add_argument('-s', '--sample', type=float,
                        help='Output a sample of the input data. Provide a number from 0-100 '
                             'representing the approximate size of the sample as a percentage of '
                             'the whole dataset.')
    args = parser.parse_args()

    if args.sample and not (0 <= args.sample <= 100):
        raise ValueError('Sampling percentage must be between 0 and 100.')

    return args


def make_scan(input, output, sample=None):
    if sample:
        random.seed()

    output_rows = [
        'occurrenceID', 'catalogNumber', 'dataGeneralizations', 'basisOfRecord', 'individualCount',
        'sex', 'lifeStage', 'references', 'eventDate', 'verbatimEventDate', 'samplingProtocol',
        'country', 'stateProvince', 'locality', 'decimalLatitude', 'decimalLongitude',
        'identificationRemarks', 'scientificName', 'identificationQualifier', 'occurrenceRemarks'
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

    countries = ('United States', 'United Kingdom', 'Uganda')

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
        'Ada County Weed Pest and Mosquito Abatement'
    )

    remove_tags = ('abundance', 'viral surveillance')

    temp = output + '.temp'

    try:
        with open(input) as input_file, open(temp, 'w', newline='') as temp_file:
            input_csv = csv.DictReader(input_file)
            temp_csv = csv.DictWriter(temp_file, output_rows)

            temp_csv.writeheader()

            for row in input_csv:
                # If we're sampling, drop the row with some probability
                if sample is not None and random.random() >= sample/100:
                    continue

                # Discard the row based on certain conditions
                if (not row['has_geodata']
                        or row['collection_protocols'] == 'BG-Counter trap catch'):
                    continue

                # Directly set the fields that need no processing
                output_row = {
                    'occurrenceID': row['accession'],
                    'catalogNumber': row['accession'],
                    'dataGeneralizations': row['projects'],
                    'basisOfRecord': 'HumanObservation',
                    'individualCount': row['sample_size_i'],
                    'sex': row['sex_s'],
                    'lifeStage': row['dev_stages_ss'],
                    'references': row['exp_citations_ss'],
                    'verbatimEventDate': row['collection_date_range'],
                    'samplingProtocol': row['collection_protocols'],
                    'identificationRemarks': row['protocols'],
                }

                # Species
                species_terms = row['species'].split()

                if species_terms[0] == 'genus' or species_terms[0] == 'subgenus':
                    del species_terms[0]

                if species_terms[0] not in valid_first_species_terms:
                    raise ValueError('Unknown first species term "{}" at {}'
                                     .format(species_terms[0], row['accession']))

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
                                       .format(species_terms[2], row['accession']))

                        if len(species_terms) == 4:
                            if 'identificationQualifier' not in output_row:
                                output_row['identificationQualifier'] = ''

                            output_row['identificationQualifier'] += ' ' + species_terms[3]

                # Coordinates
                coordinates = row['geo_coords'].split(',')
                output_row['decimalLatitude'] = coordinates[0]
                output_row['decimalLongitude'] = coordinates[1]

                # Location
                location_terms = row['geolocations'].strip(')').split(' (')

                output_row['locality'] = location_terms[0]

                if len(location_terms) >= 2:
                    if location_terms[1] in countries:
                        output_row['country'] = location_terms[1]
                    else:
                        output_row['stateProvince'] = location_terms[1]

                        if len(location_terms) == 3:
                            if location_terms[2] in countries:
                                output_row['country'] = location_terms[2]
                            else:
                                raise ValueError('Unknown third location term "{}" at {}'
                                                 .format(location_terms[2], row['accession']))

                # Date
                output_row['eventDate'] = row['collection_date'][:10]

                # occurrenceRemarks
                author_text = generator_text = ''
                link = 'https://www.vectorbase.org/popbio/map/?view=abnd&zoom_level=11'
                link += '&center=' + row['geo_coords']

                if row['tags_ss']:
                    tags = row['tags_ss'].split(';')
                    tags = [tag for tag in tags if tag not in remove_tags]

                    if len(tags) > 1 or tags[0] not in valid_provider_tags:
                        raise ValueError('Unexpected tag(s) {} at {}'
                                         .format(tags, row['accession']))

                    author_text += ' authored by ' + tags[0]
                    link += '&tag=' + tags[0].replace(' ', '+')
                else:
                    link += '&projectID=' + row['projects']

                if row['exp_citations_ss']:
                    generator_text += (', which includes '
                                       + '; '.join(row['exp_citations_ss'].split(';')))

                output_row['occurrenceRemarks'] = (
                    'This record has been curated by VectorBase.org as part of a larger data set{}'
                    ' which can be seen in context at {}. Please cite VectorBase and the original '
                    'data generator{}.'.format(author_text, link, generator_text)
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
