from core.database import engine, meta
from core.query import distinct_streets_oldham
from core.tables import residence_in_process, normal_address, standard_address, tiger_place, tiger_county, tiger_addrfeat
from sqlalchemy import select, alias, text, distinct, and_
from sqlalchemy.sql import func
import geoalchemy2
import difflib
import pandas as pd

# results = con.execute(select([residence_in_process.c.street], distinct=True)).fetchall()
# streets = [street[0].lower() for street in results if street[0]]


con = engine.connect()
ref = distinct_streets_oldham(engine)
ref = [street.lower() for street in ref]
ref += ['cassandra lane', 'fredrick lane', 'oldham oaks road', 'griffin lane', 'christopher lane']

results = con.execute(select([standard_address.c.name, standard_address.c.suftype], distinct=True)).fetchall()
streets = [f'{street[0]} {street[1]}'.lower() for street in results if street[0] and street[1]]
streets += [street[0].lower() for street in results if street[0] and not street[1]]
n_streets = []
needs_more_work = []
for street in streets:
    if difflib.get_close_matches(street, ref, 1):
        best_match = difflib.SequenceMatcher(a=street, b=difflib.get_close_matches(street, ref, 1)[0])
        ratio = best_match.quick_ratio()
        n_streets.append(dict(original=best_match.a, best_match=best_match.b, ratio=ratio))
    else:
        needs_more_work.append(street)

df = pd.DataFrame(data=n_streets)


def load_distinct_cities(engine):
    with engine.connect() as con:
        stmt = select([standard_address.c.city], distinct=True)
        results = con.execute(stmt).fetchall()
        return [city[0].lower() for city in results if city[0]]


def residence_in_process_dataframe(engine):
    with engine.connect() as con:
        results = con.execute(select([residence_in_process])).fetchall()
        return pd.DataFrame(data=results, columns=results[0].keys())


def load_reference_cities(engine):
    with engine.connect() as con:
        result = con.execute(select([tiger_county.c.the_geom]).where(
            and_(tiger_county.c.statefp == '21', tiger_county.c.countyfp == '185'))).fetchall()
        oldham_geom = result[0].the_geom

        results = con.execute(select([tiger_place.c.name]).where(
            func.ST_dwithin(tiger_place.c.the_geom, oldham_geom, .1))).fetchall()
        cities = [result[0] for result in results if result[0]]
        cities[cities.index('Louisville/Jefferson County metro government (balance)')] = 'Louisville'
        cities.sort()
        cities = [city.upper() for city in cities]
        cities += ['PENDLETON']
        return cities


def clean_cities(cities, ref):
    good = []
    scrubbed = []
    cities_need_work = []
    for city in cities:
        if difflib.get_close_matches(city, ref, 1):
            best_match = difflib.SequenceMatcher(a=city, b=difflib.get_close_matches(city, ref, 1)[0])
            ratio = best_match.quick_ratio()
            if ratio == 1.0:
                good.append(dict(original=best_match.a, best_match=best_match.b, ratio=ratio))
            else:
                scrubbed.append(dict(original=best_match.a, best_match=best_match.b, ratio=ratio))
        else:
            cities_need_work.append(city)
    return scrubbed, cities_need_work


def change_typo(df, orig, replace, column):
    df.loc[df[column] == orig, [column]] = replace


def load_reference_street(engine):
    with engine.connect() as con:
        result = con.execute(select([tiger_county.c.the_geom]).where(
            and_(tiger_county.c.statefp == '21', tiger_county.c.countyfp == '185'))).fetchall()
        oldham_geom = result[0].the_geom

        results = con.execute(select([tiger_addrfeat.c.fullname], distinct=True).where(
            func.ST_dwithin(tiger_addrfeat.c.the_geom, oldham_geom, .1))).fetchall()

        streets = [street[0].upper() for street in results if street[0]]

        return streets


def clean_streets(streets, ref):
    good = []
    scrubbed = []
    need_work = []
    for street in streets:
        if difflib.get_close_matches(street, ref, 1):
            best_match = difflib.SequenceMatcher(a=street, b=difflib.get_close_matches(street, ref, 1)[0])
            ratio = best_match.quick_ratio()
            if ratio == 1.0:
                good.append(dict(original=best_match.a, best_match=best_match.b, ratio=ratio))
            else:
                scrubbed.append(dict(original=best_match.a, best_match=best_match.b, ratio=ratio))
        else:
            need_work.append(street)
    return scrubbed, need_work


def normal_address_dataframe(engine):
    with engine.connect() as con:
        results = con.execute(select([normal_address])).fetchall()
        return pd.DataFrame(data=results, columns=results[0].keys())


# for k, v in update_dict.items():
#    stmt = residence_in_process.update().where(residence_in_process.c.voter_id.in_(v)).values(city=k)
#    results = con.execute(stmt)
#    print(results)

for city in scrubbed:
    with engine.connect() as con:
        con.execute(normal_address.update().where(normal_address.c.location == city['original']).values(location=city['best_match']))



