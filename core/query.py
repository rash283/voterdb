from core.database import engine
from core.tables import voters_in_process, address_in_process, normal_address, oldham_roads, normal_address_mailing, voters, mailing_addresses
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


def distinct_streets_oldham(engine):
    with engine.connect() as con:
        stmt = select([oldham_roads.c.fullname], distinct=True)
        results = con.execute(stmt).fetchall()
        streets = [street[0] for street in results if street[0]]
    return streets


def current_streets(engine):
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
    with engine.connect() as con:
        j = voters.join(mailing_addresses)
        stmt = select([mailing_addresses]).select_from(j).where(and_(voters.c.party == 'R', voters.c.primary_sum >= 2))
        results = con.execute(stmt).fetchall()
        df = pd.DataFrame(results, columns=['id', 'first_name', 'last_name', 'street', 'city', 'state', 'zip'])
        df.index = df['id']
        del(df['id'])
        df['address'] = ''
        for row in df.itertuples():
            df.loc[row.Index, 'address'] = f'{row.street}, {row.city}, {row.state}, {row.zip[:5]}'
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

