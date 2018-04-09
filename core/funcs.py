from core.database import engine
from core.tables import tiger_place, tiger_county, tiger_addrfeat
from sqlalchemy import select, and_
from sqlalchemy.sql import func
import difflib


def load_reference_cities():
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
        if difflib.get_close_matches(city, ref, n=1, cutoff=.8):
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


def load_reference_street():
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
        if difflib.get_close_matches(street, ref, n=1, cutoff=.8):
            best_match = difflib.SequenceMatcher(a=street, b=difflib.get_close_matches(street, ref, 1)[0])
            ratio = best_match.quick_ratio()
            if ratio == 1.0:
                good.append(dict(original=best_match.a, best_match=best_match.b, ratio=ratio))
            else:
                scrubbed.append(dict(original=best_match.a, best_match=best_match.b, ratio=ratio))
        else:
            need_work.append(street)
    return scrubbed, need_work


# for k, v in update_dict.items():
#    stmt = residence_in_process.update().where(residence_in_process.c.voter_id.in_(v)).values(city=k)
#    results = con.execute(stmt)
#    print(results)

# for city in scrubbed:
#     with engine.connect() as con:
#         con.execute(normal_address.update().where(normal_address.c.location == city['original']).values(location=city['best_match']))
