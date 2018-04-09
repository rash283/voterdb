from core.database import engine
from core.tables import voters_in_process, address_in_process, normal_address, oldham_roads, normal_address_mailing, \
     voters, mailing_addresses
from core.funcs import load_reference_street, clean_streets
from sqlalchemy import select, alias, text, distinct, and_
from sqlalchemy.sql import func
import difflib
import pandas as pd


# Populate the residential address strings in address_in_process table.


def load_residential_address_strings():
    con = engine.connect()
    v = alias(voters_in_process)
    stmt = select([v.c.id, v.c.r_street, v.c.r_city, v.c.r_state, v.c.r_zip])
    results = con.execute(stmt)
    residence_addresses = []
    for result in results:
        residence_addresses.append(dict(voter_id=result.id,
                                        address_type='residence',
                                        address_str=f'{result.r_street}, {result.r_city}, {result.r_state}, {result.r_zip}')
                                   )
    con.execute(address_in_process.insert(), residence_addresses)


def load_mailing_address_strings():
    con = engine.connect()
    v = alias(voters_in_process)
    stmt = select([v.c.id, v.c.m_street, v.c.m_city, v.c.m_state, v.c.m_zip])
    results = con.execute(stmt)
    mailing_addresses = []
    for result in results:
        mailing_addresses.append(dict(voter_id=result.id,
                                      address_type='mailing',
                                      address_str=f'{result.m_street}, {result.m_city}, {result.m_state}, {result.m_zip}')
                                 )
    con.execute(address_in_process.insert(), mailing_addresses)


def geocode():
    s = text(
        """SELECT g.rating, g.geomout
    FROM geocode(:addy, 1) AS g;""")


def distinct_streets_oldham():
    with engine.connect() as con:
        stmt = select([oldham_roads.c.fullname], distinct=True)
        results = con.execute(stmt).fetchall()
        streets = [street[0] for street in results if street[0]]
    return streets


def current_streets():
    with engine.connect() as con:
        stmt = select([normal_address.c.streetname, normal_address.c.streettypeabbrev], distinct=True)
        results = con.execute(stmt).fetchall()
        streets = [f'{street[0]} {street[1]}' for street in results if (street[0] and street[1])]
        streets += [street[0] for street in results if (street[0] and not street[1])]
    return streets


def get_mailing_addresses(engine):
    with engine.connect() as con:
        j = voters.join(normal_address_mailing)
        stmt = select([voters, normal_address_mailing]).select_from(j).where(and_(
            voters.c.party == 'R', voters.c.primary_sum >= 3
        ))
        results = con.execute(stmt).fetchall()
        voters_list = {}
        for result in results:
            r = dict()
            r['street_address'] = ' '.join([str(result[x]) for x in ['address', 'predirabbrev', 'streetname', 'streettypeabbrev', 'postdirabbrev', 'internal'] if result[x]])
            r['city_state_zip'] = ', '.join([str(result[x]) for x in ['location', 'stateabbrev', 'zip'] if result[x]])
            r['name'] = ' '.join([result['first'], result['last']])
            voters_list[result['id']] = r


def get_mailing_addresses_rep_prim(engine):
    j = voters.join(mailing_addresses)
    j1 = voters.join(normal_address_mailing)
    stmt = select([mailing_addresses]).select_from(j).where(
        and_(voters.c.party == 'R', voters.c.primary_sum >= 2))
    stmt1 = select([normal_address_mailing]).select_from(j1).where(
        and_(voters.c.party == 'R', voters.c.primary_sum >= 2))
    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
        results1 = con.execute(stmt1).fetchall()
    df = pd.DataFrame(results, columns=['id', 'first_name', 'last_name', 'street', 'city', 'state', 'zip'])
    df.index = df['id']
    del(df['id'])
    df['street'] = df['street'].str.replace(r'P\.?\s?O\.?\s?BOX\s+(\d+)', r'P.O. BOX \1')
    df1 = pd.DataFrame(results1, columns=['addr_num', 'pre', 'streetname', 'streettype', 'post', 'internal',
                                          'city1', 'state1', 'zip1', 'parsed', 'zip4', 'aplhanum', 'voter_id'])
    df1 = df1.dropna(subset=['addr_num'])
    df1['addr_num'] = df1['addr_num'].astype('int')
    df1.index = df1['voter_id']
    del(df1['voter_id'])
    df1.loc[df1.streettype == 'HWY', ['streetname', 'streettype']] = df1.loc[df1.streettype == 'HWY', ['streettype', 'streetname']].values
    df2 = pd.merge(df, df1, left_index=True, right_index=True)
    df2.loc[df2['street'] == '0', 'street'] = None
    df2 = df2.dropna(subset=['street'])
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
        results = con.execute(select([mailing_addresses])).fetchall()
    df = pd.DataFrame(results, columns=results[0].keys())
    streets = [street for street in df.name.unique() if street]
    ref_street = load_reference_street()


