from datetime import datetime

from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Boolean, Numeric
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class GgsCmmn(Base):
    __tablename__ = 'ggs_cmmn'

    cmmn_cd = Column(String(7), primary_key=True)
    lclsf_cmmn_cd = Column(String(7), nullable=False)
    cmmn_cd_nm = Column(String(200))
    indct_orr = Column(Numeric(10))
    etc_cn_1 = Column(String(2000))
    etc_cn_2 = Column(String(2000))
    etc_cn_3 = Column(String(2000))
    etc_cn_4 = Column(String(2000))
    etc_cn_5 = Column(String(2000))
    cmmn_cd_rmrk = Column(String(4000))
    use_yn = Column(String(1))
    frst_reg_dt = Column(TIMESTAMP)
    last_mdfcn_dt = Column(TIMESTAMP)

    def __str__(self):
        return f"GgsCmmn(cmmn_cd={self.cmmn_cd}, lclsf_cmmn_cd={self.lclsf_cmmn_cd}, cmmn_cd_nm={self.cmmn_cd_nm})"


class GgsDataInfo(Base):
    __tablename__ = 'ggs_data_info'

    dat_no = Column(String(10), primary_key=True)
    clsf_cd = Column(String(7))
    dat_nm = Column(String(50))
    rgn_se = Column(String(7))
    pd_se = Column(String(7))
    rel_dat_list_nm = Column(String(200))
    rel_tbl_nm = Column(String(200))
    rel_fild_nm = Column(String(100))
    dat_src = Column(String(200))
    updt_cyle = Column(String(50))
    dat_scop_bgng = Column(String(50))
    dat_scop_end = Column(String(50))
    rmk = Column(Text)
    indct_orr = Column(Numeric(10))
    use_yn = Column(Boolean)
    dat_last_reg_ymd = Column(String(8))
    frst_reg_dt = Column(DateTime)
    last_mdfcn_dt = Column(DateTime)

    def __str__(self):
        return f"GgsDataInfo(dat_no={self.dat_no}, dat_nm={self.dat_nm}, clsf_cd={self.clsf_cd}, ...)"


class GgsStatis(Base):
    __tablename__ = 'ggs_statis'
    __table_args__ = {'schema': 'dipgbpr'}

    yr = Column(String(4), primary_key=True)
    stdg_cd = Column(String(10), primary_key=True)
    dat_no = Column(String(7), primary_key=True)
    jan = Column(Numeric(15))
    feb = Column(Numeric(15))
    mar = Column(Numeric(15))
    apr = Column(Numeric(15))
    may = Column(Numeric(15))
    jun = Column(Numeric(15))
    july = Column(Numeric(15))
    aug = Column(Numeric(15))
    sep = Column(Numeric(15))
    oct = Column(Numeric(15))
    nov = Column(Numeric(15))
    dec = Column(Numeric(15))
    qu_1 = Column(Numeric(15))
    qu_2 = Column(Numeric(15))
    qu_3 = Column(Numeric(15))
    qu_4 = Column(Numeric(15))
    ht_1 = Column(Numeric(15))
    ht_2 = Column(Numeric(15))
    yr_vl = Column(Numeric(15))
    # pd_se = Column(String(7))
    frst_reg_dt = Column(TIMESTAMP)
    last_mdfcn_dt = Column(TIMESTAMP)

    def __str__(self):
        field_values = ', '.join([f'{column.name}={getattr(self, column.name)}' for column in self.__table__.columns])
        return f'GgsStatis({field_values})'
