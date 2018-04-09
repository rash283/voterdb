from core.database import engine
from core.tables import voter_in_process
from sqlalchemy import select
import pandas as pd


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


def parse_voting_history(vh_string: str):
    index = list(range(0, len(vh_string)+1, 4))
    vh = [vh_string[index[i]:index[i+1]] for i in range(len(index)-1)]
    return vh


def load_address_in_process():
    stmt = select(voter_in_process)
    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
    df = pd.DataFrame(results, columns=results[0].keys())
    df['mailing'] = ''
    df['residence'] = ''
    m_parts = ['MailingStreet', 'MailingCity', 'MailingState', 'MailingZip']
    r_parts = ['ResidenceStreet', 'ResidenceCity', 'ResidenceState', 'ResidenceZip']
    for row in df.itertuples():
        rowdict = row._asdict()
        df.loc[row.Index, 'mailing'] = ' '.join([str(rowdict[part]) for part in m_parts if rowdict[part]])
        df.loc[row.Index, 'residence'] = ' '.join([str(rowdict[part]) for part in r_parts if rowdict[part]])
    df[['mailing', 'residence']].to_sql('address_in_process', engine, schema='public', if_exists='replace',
                                        index_label='voter_id', chunksize=1000)


def load_voting_history():
    stmt = select([voter_in_process])
    with engine.connect() as con:
        results = con.execute(stmt).fetchall()
    df = pd.DataFrame(results, columns=results[0].keys())


