from core.database import engine
from core.tables import voters, mailing_in_process, oldham_roads, voting_history, mailing_std, tiger_zip_state_loc, \
    tiger_addr, tiger_addrfeat
from core.funcs import load_reference_street
from sqlalchemy import select, alias, text, and_, bindparam
from sqlalchemy.sql import text
from sqlalchemy.sql import func
import pandas as pd
import numpy as np


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


def get_addressee(group):
    group['occupant'] = len(group)
    if (group.sex == 'M').any():
        oldest = group.yob.idxmin()
        oldest_male = group.loc[group.sex == 'M', 'yob'].idxmin()
        if oldest == oldest_male:
            addressee = group.loc[oldest]
        else:
            oldest_yob = group.loc[oldest, 'yob']
            oldest_male_yob = group.loc[oldest_male, 'yob']
            if (oldest_yob - oldest_male_yob) / np.timedelta64(365, 'D') > 16:
                addressee = group.loc[oldest]
            else:
                addressee = group.loc[oldest_male]
    else:
        oldest = group.yob.idxmin()
        addressee = group.loc[oldest]
    return addressee


def get_mailing_addresses_rep_prim():
    j = voters.join(mailing_in_process)
    j1 = voters.join(mailing_std)
    stmt = select([mailing_in_process, voters.c.PrecinctCode, voters.c.Sex,
                   voters.c.DateOfBirth, voters.c.primary_sum]).select_from(j).where(
        and_(voters.c.Party == 'R', voters.c.primary_sum >= 2))
    stmt1 = select([mailing_std]).select_from(j1).where(
        and_(voters.c.Party == 'R', voters.c.primary_sum >= 2))
    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
        results1 = con.execute(stmt1).fetchall()
    df = pd.DataFrame(results, columns=['id', 'first_name', 'last_name', 'street', 'city',
                                        'state', 'zip', 'macro', 'precinct', 'sex', 'yob', 'p_sum'])
    df.index = df['id']
    del(df['id'])
    df.yob = pd.to_datetime(df.yob)
    df['street'] = df['street'].str.replace(r'P\.?\s?O\.?\s?BOX\s+(\d+)', r'P.O. BOX \1')
    df['street'] = df['street'].str.replace(r'\s+', r' ')
    df['zip'] = df['zip'].str.slice(0, 5)
    for col in df.columns:
        if df[col].dtype == np.object_:
            df[col] = df[col].str.strip()
    df = df.assign(
        address=lambda x: (x['street'] + ', ' + x['city'] + ', ' + x['state'] + ', ' + x['zip']))
    pattern = r' I{2,3}| JR| SR| IV'
    df['last_name_stripped'] = df.last_name.str.rstrip(pattern)
    df1 = pd.DataFrame(results1, columns=results1[0].keys())
    df1.index = df1['index']
    del(df1['index'])
    for k in df1.columns:
        if df1[k].isnull().all():
            del (df1[k])
    df2 = pd.merge(df, df1, left_index=True, right_index=True)
    df2.loc[df2['street'] == '0', 'street'] = None
    df2 = df2.dropna(subset=['street'])
    df2.loc[df2.state_y == 'KENTUCKY', 'state_y'] = 'KY'
    pg = df2.groupby('precinct')

    for name, group1 in pg.groups:
        addys = group1.groupby(['house_num', 'name'])
        for (hn, street), group in addys:
            print(get_addressee(group))
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
        results = con.execute(select([mailing_in_process])).fetchall()
    df = pd.DataFrame(results, columns=results[0].keys())
    streets = [street for street in df.name.unique() if street]
    ref_street = load_reference_street()


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


def get_ky_zips():
    stmt = select([tiger_zip_state_loc.c.zip, tiger_zip_state_loc.c.place]) \
        .where(tiger_zip_state_loc.c.statefp == '21')
    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
    zips = pd.DataFrame(results, columns=results[0].keys())
    return zips


def compare_city_zip(df, zip_df):
        z = df.city.unique()
        for i in z:
            for city in df.loc[df.zip5 == i, 'city'].unique():
                zip_cities = zip_df.loc[zip_df.zip == i, 'place'].values
                if city not in zip_cities:
                    print(f'Zip code:{i}, listed city: {city}')
                    print(zip_cities)


def get_oldham_zips():
    j = tiger_addr.join(tiger_addrfeat)


    a101 = pg.get_group('A103')
    a101_addys = a101.groupby(['house_num', 'name'])
    addresses = [get_addressee(group) for (hn, street), group in a101_addys]



        # if len(group.address.unique()) > 1:
        #     men = len(group[group.sex == 'M'])
        #     if men == 1:
        #
        #         print(group.loc[group.sex == 'M', 'address'].values[0])
        #     elif men > 1:
        #         group.address = group[group.index == group.loc[group.sex == 'M', 'yob'].idxmin()].address
        #     else:
        #         group.address = group[group.index == group.loc[:, 'yob'].idxmin()].address
        #     print(f'Group:{hn}, {street}\nUnique addresses: {len(group.address.unique())}\n')
