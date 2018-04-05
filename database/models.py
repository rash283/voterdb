from sqlalchemy import Column, String, Integer, Float, ForeignKey, Date, Enum, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Address(Base):
    __tablename__ = 'addresses'

    id = Column(Integer, primary_key=True)
    address_type = Column(String)  # Todo:Change this to Enum type
    address_number = Column(Integer)
    street = Column(String)
    city = Column(String)
    state = Column(String)
    zip_code = Column(Integer)
    voter_id = Column(Integer, ForeignKey('voters.id'))

    voter = relationship('Voter', back_populates='addresses')

    def __repr__(self):
        return f'<Address {self.address_number} {self.street}, {self.city}, {self.state}, {self.zip_code}'


class VotingHistory(Base):
    __tablename__ = 'votinghistories'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    primary = Column(Boolean)
    general = Column(Boolean)
    voter_id = Column(Integer, ForeignKey('voter.id'))

    voter = relationship('Voter', back_populates='votinghistories')

    def __repr__(self):
        return f'<Voter History: {self.voter_id}, {self.year}, Primary: {self.primary}, General: {self.general}'

    def __str__(self):
        return f'{self.year}: Primary: {self.primary}, General: {self.general}'


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

    addresses = relationship('Address', order_by=Address.address_type, back_populates='voters')
    voting_history = relationship('VotingHistory', order_by=VotingHistory.year, back_populates='voters')

    
''' 'CountyCode': (0, 2),
    'PrecinctCode': (3, 6),
    'CityCode': (7, 7),
    'Last': (8, 32),
    'First': (33, 47),
    'Middle': (48, 57),
    'Sex': (58, 58),
    'Party': (59, 59),
    'OtherCode': (60, 62),
    'ResidenceStreet': (63, 102),
    'ResidenceCity': (103, 122),
    'ResidenceState': (123, 124),
    'ResidenceZip': (125, 133),
    'MailingStreet': (134, 173),
    'MailingCity': (174, 193),
    'MailingState': (194, 195),
    'MailingZip': (196, 204),
    'DateOfBirth': (205, 212),
    'DateOfRegistration': (213, 220),
    'VotingHistory': (221, 240)
'''