from datetime import datetime

from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import relationship

from db.base_class import Base


class Correlation(Base):
    id = Column(Integer, primary_key=True)


class Regression(Base):
    id = Column(Integer, primary_key=True)


class Clustering(Base):
    id = Column(Integer, primary_key=True)
