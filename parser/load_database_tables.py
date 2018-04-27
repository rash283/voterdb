import pandas as pd
from datetime import date
from collections import OrderedDict
from core.database import engine
from core.funcs import load_reference_cities, load_reference_street, clean_cities


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

    def as_dict(self):
        return dict(year=self.year,
                    primary=self.primary,
                    general=self.general
                    )


def parse_voting_history(vh_string: str):
    index = list(range(0, len(vh_string) + 1, 4))
    vh = [vh_string[index[i]:index[i + 1]] for i in range(len(index) - 1)]
    return vh


def change_typo(dataframe, field, typo, correct_word):
    dataframe.loc[dataframe[field] == typo, field] = correct_word


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
    'VotingHistory': (217, 236)
})

with open('data/rash0396.txt', 'r') as file:
    default_date = date(1900, 1, 1)
    voters = []
    voting_history = []
    i = 0
    for line in file:
        # If The line is the correct length then process it
        if len(line) == 238:
            # parse the line using the fix width format provided
            # This creates a dictionary out of the line with entries define by voter_dict

            voter = {k: line[v1:v2 + 1].strip() for k, (v1, v2) in voter_dict.items()}
            voters.append(voter)
            voter_history = [VotingHistory(vh).as_dict() for vh in
                             parse_voting_history(voter.get('VotingHistory', '9900'))]
            vh = pd.DataFrame(voter_history)
            vh['id'] = i
            voting_history.append(vh)
            #            [voting_history.append(f'{vh.year},{vh.primary},{vh.general},{i}\n') for vh in voter_history]
            i += 1

df = pd.DataFrame(voters, columns=voters[0].keys())
df.ResidenceCity = df.ResidenceCity.str.upper()
df.MailingCity = df.MailingCity.str.upper()
cities = set(df.ResidenceCity.unique())
cities.update(df.MailingCity.unique())
ref_cities = load_reference_cities()
ref_streets = load_reference_street()
scrubbed, needs_work = clean_cities(cities, ref_cities)
for fix in scrubbed:
    change_typo(df, 'ResidenceCity', fix['original'], fix['best_match'])
    change_typo(df, 'MailingCity', fix['original'], fix['best_match'])
df.loc[df.MailingStreet == '3710 BALLARD VISTA CT', ['ResidenceCity', 'MailingCity']] = 'SMITHFIELD'
df.loc[df.MailingStreet == '7910 OREGON CREEK RD', ['ResidenceCity', 'MailingCity']] = 'PENDLETON'
df.loc[df.DateOfRegistration.apply(lambda x: len(x)) != 8, ['DateOfRegistration']] = '01011900'
pd.to_datetime(df.DateOfRegistration, format='%m%d%Y')
df1 = pd.concat(voting_history, ignore_index=True)
vh_df = df1.loc[df1.year <= 2017]
# vh_df.to_sql('voting_history', engine, schema='public', index=False, chunksize=2000)
voter = df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 8, 17, 18]]
voter.loc[:, 'DateOfRegistration'] = pd.to_datetime(voter.DateOfRegistration, format='%m%d%Y')
# voter.to_sql('voters', engine, schema='public', chunksize=2000)
mailing_address = df.loc[:, ['First', 'Last', 'MailingStreet', 'MailingCity', 'MailingState', 'MailingZip']]
mailing_address = mailing_address.assign(macro=lambda x: (x['MailingCity'] + ', ' + x['MailingState'] + ', ' + x['MailingZip']))
mailing_address = mailing_address.assign(full_address=lambda x: x['MailingStreet'] + ', ' + x['MailingCity'] + ', ' + x['MailingState'] + ', ' + x['MailingZip'])
residence_address = df.loc[:, ['First', 'Last', 'ResidenceStreet', 'ResidenceCity', 'ResidenceState', 'ResidenceZip']]
residence_address = residence_address.assign(macro=lambda x: (
        x['ResidenceCity'] + ', ' + x['ResidenceState'] + ', ' + x['ResidenceZip']))
residence_address = residence_address.assign(full_address=lambda x: (x['ResidenceStreet'] + ', ' +
        x['ResidenceCity'] + ', ' + x['ResidenceState'] + ', ' + x['ResidenceZip']))
# mailing_address.to_sql('mailing_address_in_process', engine, schema='public', chunksize=2000)
# residence_address.to_sql('residence_address_in_process', engine, schema='public', chunksize=2000)

