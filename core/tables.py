from core.database import meta, engine
from sqlalchemy import Table
from geoalchemy2 import Geometry, Geography

meta.reflect(bind=engine, schema='public')
meta.reflect(bind=engine, schema='tiger')
# voters_in_process = Table('voters_in_process', meta, schema='public', autoload=True, autoload_with=engine)
# address_in_process = Table('address_in_process', meta, schema='public', autoload=True, autoload_with=engine)
# addresses = Table('addresses', meta, schema='public', autoload=True, autoload_with=engine)
# voters = Table('voters', meta, schema='public', autoload=True, autoload_with=engine)
# votinghistories = Table('votinghistories', meta, schema='public', autoload=True, autoload_with=engine)
# residence_in_process = Table('residence_in_process', meta, schema='public', autoload=True, autoload_with=engine)

mailing_addresses = meta.tables['public.mailing_addresses']
standard_address = meta.tables['public.standard_address']
oldham_roads = meta.tables['public.oldham_roads']
normal_address = meta.tables['public.normal_address']
normal_address_mailing = meta.tables['public.normal_address_mailing']
us_lex = meta.tables['public.us_lex']
us_gaz = meta.tables['public.us_gaz']
us_rules = meta.tables['public.us_rules']
address_in_process = meta.tables['public.address_in_process']
addresses = meta.tables['public.addresses']
votinghistories = meta.tables['public.votinghistories']
voters = meta.tables['public.voters']
voters_in_process = meta.tables['public.voters_in_process']
residence_in_process = meta.tables['public.residence_in_process']
tiger_geocode_settings = meta.tables['tiger.geocode_settings']
tiger_geocode_settings_default = meta.tables['tiger.geocode_settings_default']
tiger_direction_lookup = meta.tables['tiger.direction_lookup']
tiger_secondary_unit_lookup = meta.tables['tiger.secondary_unit_lookup']
tiger_state_lookup = meta.tables['tiger.state_lookup']
tiger_street_type_lookup = meta.tables['tiger.street_type_lookup']
tiger_place_lookup = meta.tables['tiger.place_lookup']
tiger_countysub_lookup = meta.tables['tiger.countysub_lookup']
tiger_zip_lookup_all = meta.tables['tiger.zip_lookup_all']
tiger_zip_lookup = meta.tables['tiger.zip_lookup']
tiger_zip_lookup_base = meta.tables['tiger.zip_lookup_base']
tiger_state = meta.tables['tiger.state']
tiger_edges = meta.tables['tiger.edges']
tiger_county = meta.tables['tiger.county']
tiger_addrfeat = meta.tables['tiger.addrfeat']
tiger_place = meta.tables['tiger.place']
tiger_zip_state = meta.tables['tiger.zip_state']
tiger_cousub = meta.tables['tiger.cousub']
tiger_zip_state_loc = meta.tables['tiger.zip_state_loc']
tiger_addr = meta.tables['tiger.addr']
tiger_featnames = meta.tables['tiger.featnames']
tiger_loader_platform = meta.tables['tiger.loader_platform']
tiger_zcta5 = meta.tables['tiger.zcta5']
tiger_faces = meta.tables['tiger.faces']
tiger_loader_variables = meta.tables['tiger.loader_variables']
tiger_loader_lookuptables = meta.tables['tiger.loader_lookuptables']
tiger_tabblock = meta.tables['tiger.tabblock']
tiger_bg = meta.tables['tiger.bg']
tiger_pagc_gaz = meta.tables['tiger.pagc_gaz']
tiger_pagc_lex = meta.tables['tiger.pagc_lex']
tiger_tract = meta.tables['tiger.tract']
tiger_pagc_rules = meta.tables['tiger.pagc_rules']
tiger_county_lookup = meta.tables['tiger.county_lookup']