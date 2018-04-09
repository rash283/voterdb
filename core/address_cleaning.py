from core.database import engine
from core.tables import normal_address_mailing, voters, mailing_addresses, voter_in_process
from sqlalchemy import select, alias, text, distinct, and_
from sqlalchemy.sql import func
import difflib
import pandas as pd

j = voters.join(mailing_addresses)
j1 = voter_in_process.join(voters)
stmt = select([voters.c.id, voters.c.first, voters.c.last, mailing_addresses.c.num, mailing_addresses.c.street,
               mailing_addresses.c.city, mailing_addresses.c.state, mailing_addresses.c.zip]).select_from(j).where(
    and_(voters.c.party == 'R', voters.c.primary_sum >= 2))
stmt1 = select([voter_in_process.c.index, voter_in_process.c.First, voter_in_process.c.Last,
                voter_in_process.c.MailingStreet, voter_in_process.c.MailingCity, voter_in_process.c.MailingState,
                voter_in_process.c.MailingZip, voter_in_process.c.ResidenceStreet, voter_in_process.c.ResidenceCity,
                voter_in_process.c.ResidenceState, voter_in_process.c.ResidenceZip]).select_from(j1).where(
    and_(voters.c.party == 'R', voters.c.primary_sum >= 2))
with engine.connect() as con:
    results = con.execute(stmt).fetchall()
    results1 = con.execute(stmt1).fetchall()
df1 = pd.DataFrame(results, columns=['id', 'first_name', 'last_name', 'num', 'street', 'city', 'state', 'zip'])
df1.index = df1['id']
del(df1['id'])
df1['address'] = ''
df2 = pd.DataFrame(results1, columns=results1[0].keys())
df2.index = df2['index']
del(df2['index'])
df = pd.merge(df1, df2, left_index=True, right_index=True)
df.loc[df.street.isnull(), ['street']] = df.MailingStreet
df.loc[df.street == '', ['street']] = None
df.loc[:, 'street'] = df['street'].str.replace(r'P\.?\s?O\.?\s?BOX\s+(\d+)', r'P.O. BOX \1')
df = df.dropna(subset=['street'])
df.loc[df.state.isna(), ['state']] = 'KY'

pattern = r' I{2,3}| JR| SR| IV'
df['last_name_stripped'] = df.last_name.str.rstrip(pattern)

with open('mailing_addys.csv', 'w') as file:
    for addy in df.address.unique():
        family = df[df.address == addy]
        num = len(family)
        if num == 1:
            name = ' '.join(family[['first_name', 'last_name']].values[0])
            line = f'"{addy}",{name}\n'
        elif len(family.last_name_stripped.unique()) == 1:
            last_name = family.last_name_stripped.values[0]
            first_names = ', '.join(family.first_name.values)
            line = f'"{addy}",,"{first_names}","{last_name}"\n'
        else:
            names = []
            for person in family[['first_name', 'last_name']].values:
                names.append(' '.join(person))
            names = ', '.join(names)
            line = f'"{addy}",{names},,\n'
        file.writelines(line)