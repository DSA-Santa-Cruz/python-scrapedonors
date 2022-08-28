from sqlalchemy import Column, DateTime, String, Float
from sqlalchemy.ext.declarative import declarative_base

# from sqlalchemy.orm import relationship
# from sqlalchemy.sql import func

Base = declarative_base()


class Contribution(Base):
    __tablename__ = "contributions"
    id = Column(String, primary_key=True)
    amount = Column(Float)
    date = Column(DateTime)
    report_date = Column(DateTime)
    type = Column(String)
    code = Column(String)
    donor_l_name = Column(String)
    donor_f_name = Column(String)
    donor_city = Column(String)
    donor_state = Column(String)
    donor_zip = Column(String)
    donor_occupation = Column(String)
    donor_employer = Column(String)
    donor_id = Column(String)
    reporter_id = Column(String)
    reporter_name = Column(String)


class Committee(Base):
    __tablename__ = "committees"
    reporter_id = Column(String, primary_key=True)
    reporter_name = Column(String)


class Donor(Base):
    __tablename__ = "donors"
    donor_id = Column(String, primary_key=True)
    donor_l_name = Column(String)
    donor_f_name = Column(String)
    donor_city = Column(String)
    donor_state = Column(String)
    donor_zip = Column(String)
    donor_occupation = Column(String)
    donor_employer = Column(String)
