import usaddress
from collections import OrderedDict
from datetime import datetime
import pandas as pd


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
        return f'<Voting History: {self.year}>'

    def __str__(self):
        return f'{self.year}: Primary {self.primary}, General {self.general}'


def parse_address(street, city, state, zip_code):
    address_str = f'{street}, {city}, {state}, {zip_code}'
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
    try:
        address, _ = usaddress.tag(address_str, tag_mapping=tag_map)
    except usaddress.RepeatedLabelError:
        address = OrderedDict(house_num='NaN',street=street, city=city, state=state, zip_code=zip_code)
    return address


def parse_voting_history(vh_string: str):
    index = list(range(0, len(vh_string)+1, 4))
    vh = [vh_string[index[i]:index[i+1]] for i in range(len(index)-1)]
    return vh


def parse_line(line):

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
        'DateOfBirth': (205, 208),
        'DateOfRegistration': (209, 216),
        'VotingHistory': (217, 237)
    })

    voter = {k: line[v1:v2+1].strip() for k, (v1, v2) in voter_dict.items()}
    residence_address = parse_address(
        f"{voter['ResidenceStreet']}, {voter['ResidenceCity']}, {voter['ResidenceState']}, {voter['ResidenceZip'][:5]}")
    mailing_address = parse_address(
        f"{voter['MailingStreet']}, {voter['MailingCity']}, {voter['MailingState']}, {voter['MailingZip'][:5]}")

    voter_map = dict(county_code=voter['CountyCode'],
                     precinct_code=voter['PrecinctCode'],
                     city_code=voter['CityCode'],
                     last=voter['Last'],
                     first=voter['First'],
                     middle=voter['Middle'],
                     sex=voter['Sex'],
                     party=voter['Party'],
                     other_code=voter['OtherCode'],
                     dob=datetime.strptime(voter['DateOfBirth'], '%Y').date(),
                     dor=datetime.strptime(voter['DateOfRegistration'], '%m%d%Y').date())

    voter_history = [VotingHistory(vh) for vh in parse_voting_history(voter['VotingHistory'])]

    return voter_map, residence_address, mailing_address, voter_history


def change_typo(df, field, typo, correct_word):
    df.loc[df[field] == typo, field] = correct_word
