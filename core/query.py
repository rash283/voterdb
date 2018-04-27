from core.database import engine
from core.tables import voters, mailing_in_process, residence_in_process, oldham_roads, voting_history, mailing_std, \
    mailing_parsed
from core.funcs import load_reference_street, clean_streets
from sqlalchemy import select, alias, text, and_, bindparam
from sqlalchemy.sql import text
from sqlalchemy.sql import func
import pandas as pd
import numpy as np


def get_mailing_addresses_rep_prim():
    j = voters.join(mailing_in_process)
    j1 = voters.join(mailing_std)
    stmt = select([mailing_in_process.c.index, mailing_in_process.c.First, mailing_in_process.c.Last,
                   mailing_in_process.c.MailingStreet, mailing_in_process.c.MailingCity,
                   mailing_in_process.c.MailingState, mailing_in_process.c.MailingZip, voters.c.PrecinctCode,
                   voters.c.Party, voters.c.Sex, voters.c.DateOfBirth, voters.c.DateOfRegistration,
                   voters.c.primary_sum]).select_from(j).where(and_(voters.c.Party == 'R', voters.c.primary_sum >= 2))
    stmt1 = select([mailing_std]).select_from(j1).where(
        and_(voters.c.Party == 'R', voters.c.primary_sum >= 2))

    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
        results1 = con.execute(stmt1).fetchall()

    df = pd.DataFrame(results, columns=['id', 'first_name', 'last_name', 'street', 'city',
                                        'state', 'zip', 'precinct', 'party', 'sex', 'yob', 'dor', 'p_sum'])
    df.index = df['id']
    del (df['id'])
    df['street'] = df['street'].str.replace(r'P\.?\s?O\.?\s?BOX\s+(\d+)', r'P.O. BOX \1')
    df['street'] = df['street'].str.strip()
    df.yob = pd.to_datetime(df.yob, format='%Y')
    # df['zip'] = df['zip'].str.slice(0, 5)

    df1 = pd.DataFrame(results1, columns=results1[0].keys())
    df1.index = df1['index']
    del (df1['index'])
    for k in df1.columns:
        if df1[k].isnull().all():
            del(df1[k])

    df2 = pd.merge(df, df1, left_index=True, right_index=True)
    df2.loc[df2['street'] == '0', 'street'] = None
    df2 = df2.dropna(subset=['street'])
    pg = df2.groupby('precinct')



    # I left off here
    df2['address'] = ''
    df2['street1'] = ''
    parts = ['pre', 'streetname', 'streettype', 'post']
    for row in df2.itertuples():
        rowdict = row._asdict()
        df2.loc[row.Index, 'address'] = f'{row.street}, {row.city}, {row.state}, {row.zip[:5]}'
        df2.loc[row.Index, 'street1'] = ' '.join([str(rowdict[part]) for part in parts if rowdict[part]])
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


def clean_mailing_streets():
    with engine.connect() as con:
        results = con.execute(select([mailing_parsed])).fetchall()
    df = pd.DataFrame(results, columns=results[0].keys())
    streets = [street for street in df.name.unique() if street and not street.startswith('1/2')]
    ref_street = distinct_streets_oldham()
    errors = [street for street in streets if street not in ref_street]
    scrubbed, needs_work = clean_streets(errors, ref_street)


def update_voter_score():
    stmt = select([voting_history])
    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
    df = pd.DataFrame(results, columns=results[0].keys())
    groups = df.groupby('id')
    primary_sum = groups.primary.sum()
    general_sum = groups.general.sum()
    ps = [dict(id=k, primary_sum=v) for k, v in primary_sum.items()]
    gs = [dict(id=k, general_sum=v) for k, v in general_sum.items()]
    stmt = voters.update().where(voters.c.index == bindparam('id')) \
        .values(primary_sum=bindparam('primary_sum'))
    with engine.connect() as con:
        results = con.execute(stmt, ps)
    stmt = voters.update().where(voters.c.index == bindparam('id')) \
        .values(general_sum=bindparam('general_sum'))
    with engine.connect() as con:
        results = con.execute(stmt, gs)


def update_full_address():
    stmt = mailing_in_process.update().where(mailing_in_process.c.index == bindparam('voter_id')) \
        .values(full_address=bindparam('full_address'))


def distinct_streets_oldham():
    with engine.connect() as con:
        stmt = select([oldham_roads.c.fullname], distinct=True)
        results = con.execute(stmt).fetchall()
        streets = [street[0].upper() for street in results if street[0]]
    return streets


def get_mailing_addys_to_clean():
    j = voters.join(mailing_in_process)
    stmt = select([mailing_in_process]).select_from(j).where(
        and_(voters.c.Party == 'R', voters.c.primary_sum >= 2))
    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
    df = pd.DataFrame(results, columns=['id', 'first_name', 'last_name', 'street', 'city',
                                        'state', 'zip', 'macro', 'full_address'])
    df.index = df['id']
    del (df['id'])
    df['street'] = df['street'].str.replace(r'P\.?\s?O\.?\s?BOX\s+(\d+)', r'P.O. BOX \1')
