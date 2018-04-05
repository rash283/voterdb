import usaddress
import pandas as pd
from collections import OrderedDict

voter_dict = OrderedDict({
    'CountyCode': (0, 2),
    'PrecinctCode': (3, 6),
    'CityCode': (7, 7),
    'Last': (8, 32),
    'First': (33, 47),
    'Middle': (48, 57),
    'Sex': (58, 58),
    'Party': (59, 59),
    'OtherCode': (60, 62),
    'ResidenceStreet': (63, 102),
    'ResidenceCity': (103, 122),
    'ResidenceState': (123, 124),
    'ResidenceZip': (125, 133),
    'MailingStreet': (134, 173),
    'MailingCity': (174, 193),
    'MailingState': (194, 195),
    'MailingZip': (196, 204),
    'DateOfBirth': (205, 212),
    'DateOfRegistration': (213, 220),
    'VotingHistory': (221, 240)
})

original_file = '/home/richard/Documents/rash0396.txt'
good_file = '/home/richard/Documents/good_lines.txt'

original = open(original_file, 'r')
good = open(good_file, 'w')

good.write(','.join(voter_dict.keys()))
good.write('\n')
for line in original:
    good.write(','.join([f'"{line[v1:v2+1].strip()}"' for _, (v1, v2) in voter_dict.items()]))
    good.write('\n')

original.close()
good.close()


def parse_line(line):
    voter = {k: line[v1:v2+1].strip() for k, (v1, v2) in voter_dict.items()}
    residence_address = parse_address("{voter['ResidenceStreet']}, {voter['ResidenceCity']}, {voter['ResidenceState']}, {voter['ResidenceZip'][:5]}")
    mailing_address = parse_address("{voter['MailingStreet']}, {voter['MailingCity']}, {voter['MailingState']}, {voter['MailingZip'][:5]}")

def parse_address(address_str):
    tag_map = {
        'Recipient': 'recipient',
        'AddressNumber': 'house_number',
        'AddressNumberPrefix': 'house_number',
        'AddressNumberSuffix': 'house_number',
        'StreetName': 'street',
        'StreetNamePreDirectional': 'street',
        'StreetNamePreModifier': 'street',
        'StreetNamePreType': 'street',
        'StreetNamePostDirectional': 'street',
        'StreetNamePostModifier': 'street',
        'StreetNamePostType': 'street',
        'CornerOf': 'street',
        'IntersectionSeparator': 'street',
        'LandmarkName': 'street',
        'USPSBoxGroupID': 'street',
        'USPSBoxGroupType': 'street',
        'USPSBoxID': 'street',
        'USPSBoxType': 'street',
        'BuildingName': 'address2',
        'OccupancyType': 'address2',
        'OccupancyIdentifier': 'address2',
        'SubaddressIdentifier': 'address2',
        'SubaddressType': 'address2',
        'PlaceName': 'city',
        'StateName': 'state',
        'ZipCode': 'zip_code',
    }
    address, _ = usaddress.tag(address_str, tag_mapping=tag_map)
    return address

def parse_voting_history(vh_string: str):
    index = list(range(0,len(vh_string)+1,4))
    vh  = [vh_string[index[i]:index[i+1]] for i in range(len(index)-1)]


df = pd.read_csv('/home/richard/Documents/good_lines.txt',
                 header=0, names=[
                                    'CountyCode',
                                    'PrecinctCode',
                                    'CityCode',
                                    'Last',
                                    'First',
                                    'Middle',
                                    'Sex',
                                    'Party',
                                    'OtherCode',
                                    'ResidenceStreet',
                                    'ResidenceCity',
                                    'ResidenceState',
                                    'ResidenceZip',
                                    'MailingStreet',
                                    'MailingCity',
                                    'MailingState',
                                    'MailingZip',
                                    'DateOfBirth',
                                    'DateOfRegistration',
                                    'VotingHistory',
                                ],
                 dtype=object,
                 parse_dates=[17, 18],
                 infer_datetime_format=True
                 )


class Address(dict):
    house_number: int


class VotingHistory(object):
    year: int
    primary: bool
    general: bool

    def __init__(self, vh_string: str = None, year: int = None, general: bool = None, primary: bool = None):
        if vh_string:
            if len(vh_string) != 4:
                raise ValueError('Voter History String should be 4 characters')
            else:
                self.year = 2000 + int(vh_string[:2])
                self.primary = bool(int(vh_string[2:3]))
                self.general = bool(int(vh_string[3:]))
        elif (primary is not None) & (general is not None) & (year is not None):
            self.year = year
            self.general = general
            self.primary = primary

    def __repr__(self):
        return(f'<Voting History: {self.year}>')

    def __str__(self):
        return(f'{self.year}: Primary {self.primary}, General {self.general}')


class Voter(object):

    def __init__(self, county_code=None, precinct_code=None, city_code=None, last_name=None, first_name=None,
                 middle_name=None, sex=None, party=None, other_code=None, residence_street=None,
                 residence_city=None, residence_state=None, residence_zip=None, mailing_street=None,
                 mailing_city=None, mailing_state=None, mailing_zip=None, dob=None, dor=None,
                 voting_history=None):
        if county_code:
            self.county_code = county_code
        if precinct_code:
            self.precinct_code = precinct_code
        if city_code:
            self.city_code = city_code
        if last_name:
            self.last_name = last_name
        if first_name:
            self.first_name = first_name
        if middle_name:
            self.middle_name = middle_name
        if sex:
            self.sex = sex
        if party:
            self.party = party
        if other_code:
            self.other_code = other_code
        if residence_street:
            self.residence_street = residence_street
        if residence_city:
            self.residence_city = residence_city
        if residence_state:
            self.residence_state = residence_state
        if residence_zip:
            self.residence_zip = residence_zip
        if mailing_street:
            self.mailing_street = mailing_street
        if mailing_city:
            self.mailing_city = mailing_city
        if mailing_state:
            self.mailing_state = mailing_state
        if mailing_zip:
            self.mailing_zip = mailing_zip
        if dob:
            self.dob = dob
        if dor:
            self.dor = dor
        if voting_history:
            self.voting_history = voting_history
