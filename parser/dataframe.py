from app.parser import parse_voting_history, parse_address, VotingHistory
from datetime import datetime, date
import pandas as pd
from collections import OrderedDict

# class Voter(dict):
#
#     def __init__(self, *args, **kwargs):
#         self.update(*args, **kwargs)
#
#     def __getitem__(self, key):
#         val = dict.__getitem__(self, key)
#         print('GET', key)
#         return val
#
#     def __setitem__(self, key, val):
#         print('SET', key, val)
#         dict.__setitem__(self, key, val)
#
#     def __repr__(self):
#         dictrepr = dict.__repr__(self)
#         return '%s(%s)' % (type(self).__name__, dictrepr)
#
#     def update(self, *args, **kwargs):
#         print('update', args, kwargs)
#         for k, v in dict(*args, **kwargs).items():
#             self[k] = v


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

with open('rash0396.txt', 'r') as file:
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

            voter_history = [VotingHistory(vh) for vh in parse_voting_history(voter.get('VotingHistory', '9900'))]
            [voting_history.append(f'{vh.year},{vh.primary},{vh.general},{i}\n') for vh in voter_history]
            i += 1


for i in range(len(voting_history)):
    voting_history[i] = f'{i},{voting_history[i]}'

with open('vh.csv', 'w') as file:
    for line in voting_history:
        file.write(line)




with open('rash0396.txt', 'r') as file:
    with open('Mailing_address.csv', 'w') as newfile:
        i = 0
        j = 51754
        for line in file:
            if len(line) == 238:
                voter = {k: line[v1:v2 + 1].strip() for k, (v1, v2) in voter_dict.items()}
                newfile.write(
                    f'{j},"{voter.get("MailingStreet")}, {voter.get("MailingCity")}, {voter.get("MailingState")}, {voter.get("MailingZip")}",mailing,{i}\n')
                i += 1
                j += 1

# ','.join([f'"{line[v1:v2+1].strip()}"' for _, (v1, v2) in voter_dict.items()])

vh = df[['']]
