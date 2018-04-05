from sqlalchemy import Column, String, Integer, Float, ForeignKey, Date, Enum, Boolean
from sqlalchemy.dialects.postgresql import DATE
from sqlalchemy.orm import relationship, composite
from app.database import Base
import enum


class AddressType(enum.Enum):
    mailing = 1
    residence = 2


class Address(Base):
    __tablename__ = 'addresses'

    id = Column(Integer, primary_key=True)
    address_type = Column(Enum(AddressType))
    address_number = Column(Integer)
    street = Column(String)
    city = Column(String)
    state = Column(String)
    zip_code = Column(Integer)
    voter_id = Column(Integer, ForeignKey('voters.id'))

    voter = relationship('Voter', back_populates='addresses')

    def __repr__(self):
        return f'<Address {self.address_number} {self.street}, {self.city}, {self.state}, {self.zip_code}>'


class VotingHistory(Base):
    __tablename__ = 'votinghistories'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    primary = Column(Boolean)
    general = Column(Boolean)
    voter_id = Column(Integer, ForeignKey('voters.id'))

    voter = relationship('Voter', back_populates='voting_history')

    def __repr__(self):
        return f'<Voting History: {self.year}, Primary:{self.primary}, General:{self.general}>'


class Voter(Base):
    __tablename__ = 'voters'

    id = Column(Integer, primary_key=True)
    county_code = Column(String)
    precinct_code = Column(String)
    city_code = Column(String)
    last = Column(String)
    first = Column(String)
    middle = Column(String)
    sex = Column(String)
    party = Column(String)
    other_code = Column(String)
    dob = Column(Date)
    dor = Column(Date)

    addresses = relationship('Address', order_by=Address.address_type, back_populates='voter')
    voting_history = relationship('VotingHistory', order_by=VotingHistory.year, back_populates='voter')

class Address_in_process(Base):
    __tablename__ = 'address_in_process'

    id = Column(Integer, primary_key=True)
    voter_id = Column(Integer)
    address_type = Column(Enum(AddressType))
    address_str = Column(String)