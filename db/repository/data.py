import datetime
import os
import uuid

from fastapi import HTTPException
from typing import Literal, List

from numpy import select
from sqlalchemy.orm import Session, aliased
from sqlalchemy import create_engine, text, func, and_, Integer, or_, bindparam, distinct
import pandas as pd
from starlette import status

from core.hashing import Hasher

from db.models.data import GgsStatis, GgsCmmn, GgsDataInfo
from db.session import get_db
from schemas.data import ShowVariableDetail, EChartBarOption, EChartPieOption, EChartXAxisOption, EChartYAxisOption, \
    EChartSeriesOption, TitleEChartOption


def get_period_unit_list(period_unit):
    """
    M030004 : 년
    M030003 : 반기
    M030002 : 분기
    M030001 : 월
    """
    # data = {
    #     "year": ["M030001", "M030002", "M030003", "M030004"],
    #     "half": ["M030001", "M030002", "M030003"],
    #     "quarter": ["M030001", "M030002"],
    #     "month": ["M030001"]
    # }

    data = {
        "year": ["M030004"],
        "half": ["M030003"],
        "quarter": ["M030002"],
        "month": ["M030001"]
    }

    if period_unit not in data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="기간 단위 조건이 맞지 않습니다.")

    return data.get(period_unit, [])


def get_region_unit_list(region_unit):
    """
    M040001 : 시도
    M040002 : 시군
    M040003 : 시군구
    M040004 : 읍면동
    """

    data = {
        "sido": ["M040001"],
        "sg": ["M040002"],
        "sgg": ["M040003"],
        "emd": ["M040004"]
    }

    if region_unit not in data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="지역 단위 조건이 맞지 않습니다.")

    return data.get(region_unit, [])


def get_value_period_list(period_unit):
    data = {
        "year": ["yr_vl"],
        "month": ["jan", "feb", "mar", "apr", "may", "jun", "july", "aug", "sep", "oct", "nov", "dec"],
        "quarter": ["qu_1", "qu_2", "qu_3", "qu_4"],
        "half": ["ht_1", "ht_2"]
    }
    return data[period_unit]


def get_detail_period_by_param(period_unit, detail_period):
    data = {
        "year": {"all": "yr_vl"},
        "month": {"1": "jan", "2": "feb", "3": "mar", "4": "apr", "5": "may", "6": "jun", "7": "july", "8": "aug",
                  "9": "sep", "10": "oct", "11": "nov", "12": "dec"},
        "quarter": {"1": "qu_1", "2": "qu_2", "3": "qu_3", "4": "qu_4"},
        "half": {"1": "ht_1", "2": "ht_2"}
    }

    if detail_period not in data[period_unit]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="기간 설정 조건이 맞지 않습니다.")

    return data[period_unit][detail_period]


def get_value_period_list(period_unit: str) -> List[str]:
    if period_unit == "year":
        return ["yr_vl"]
    elif period_unit == "month":
        return ["jan", "feb", "mar", "apr", "may", "jun", "july", "aug", "sep", "oct", "nov", "dec"]
    elif period_unit == "quarter":
        return ["qu_1", "qu_2", "qu_3", "qu_4"]
    elif period_unit == "half":
        return ["ht_1", "ht_2"]


def retrieve_variable_list(region: Literal["all", "gsbd"],
                           period_unit: Literal["year", "month", "quarter", "half"],
                           db: Session):
    result = {"data": []}

    depth1_result = db.query(GgsCmmn).filter(
        GgsCmmn.cmmn_cd.like('M01%'),
        GgsCmmn.use_yn == "Y"
    ).order_by(GgsCmmn.indct_orr).all()

    for row in depth1_result:
        result["data"].append({
            "value": row.cmmn_cd,
            "label": row.cmmn_cd_nm,
            "order_index": int(row.indct_orr),
            "children": []
        })

    period_unit_list = get_period_unit_list(period_unit)

    depth2_query_template = """
        select 
            distinct gdi.dat_no, gdi.dat_nm, gdi.rel_dat_list_nm, gdi.clsf_cd, gdi.indct_orr, gc.cmmn_cd_nm rgn_se_nm      
        from 
            ggs_data_info gdi
        left join 
            ggs_cmmn gc
        on 
            gdi.rgn_se = gc.cmmn_cd 
        where 1=1
        AND (
            (dat_src not in ('경상북도', '카드사', '경상북도 (KOSIS)', 'KT') AND :region = 'all')
            OR (dat_src in ('경상북도', '카드사', '경상북도 (KOSIS)', 'KT') AND :region = 'gsbd')
        )
        AND 
            pd_se in :period_unit_list
            
        UNION ALL

        SELECT 
            DISTINCT gui.dat_no, gui.dat_nm, gui.rel_dat_list_nm, gui.clsf_cd, gui.indct_orr, gc.cmmn_cd_nm rgn_se_nm      
        FROM 
            ggs_user_data_info gui
        LEFT JOIN 
            ggs_cmmn gc
        ON 
            gui.rgn_se = gc.cmmn_cd 
        WHERE 1=1
        AND (
            (dat_src not in ('경상북도', '카드사', '경상북도 (KOSIS)', 'KT') AND :region = 'all')
            OR (dat_src in ('경상북도', '카드사', '경상북도 (KOSIS)', 'KT') AND :region = 'gsbd')
        )
        AND 
            pd_se in :period_unit_list
    """
    depth2_query = text(depth2_query_template)

    depth2_query = depth2_query.bindparams(bindparam('region', expanding=False))
    depth2_query = depth2_query.bindparams(bindparam('period_unit_list', expanding=True))

    depth2_result = db.execute(depth2_query, {
        'region': region,
        'period_unit_list': period_unit_list
    })

    # o(n^3)이네.. 고쳐야 되긴 하겠는데
    for row in depth2_result:
        for data in result["data"]:
            # depth1 아이디 찾기
            if data["value"] == row.clsf_cd:
                is_new_ele = True
                # depth2에 대해서 loop
                for child in data["children"]:

                    # depth2 이름이 동일한 게 있으면
                    if child["label"] == row.rel_dat_list_nm:
                        # depth2 children에 새로 넣음
                        child["children"].append({
                            "value": row.dat_no,
                            "label": row.dat_nm,
                            "order_index": row.indct_orr,
                            "region_unit": row.rgn_se_nm
                        })
                        is_new_ele = False
                        break
                # 없으면
                if is_new_ele:
                    data["children"].append(
                        {
                            "value": uuid.uuid4(),
                            "label": row.rel_dat_list_nm,
                            "children": [{
                                "value": row.dat_no,
                                "label": row.dat_nm,
                                "order_index": row.indct_orr,
                                "region_unit": row.rgn_se_nm
                            }]
                        }
                    )

    return result


def retrieve_variable_detail(id: str, db: Session):
    query_template = """
        select
            a.dat_no,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where a.CLSF_CD = a1.cmmn_cd) as clsf_nm,
            a.dat_nm,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where a.RGN_SE = a1.cmmn_cd) as rgn_nm,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where a.PD_SE = a1.cmmn_cd) as pd_nm,
            a.REL_DAT_LIST_NM,
            a.REL_TBL_NM,   
            a.REL_FILD_NM,
            a.DAT_SRC,
            a.UPDT_CYLE,
            a.DAT_SCOP_BGNG,
            a.DAT_SCOP_END,
            a.last_mdfcn_dt
        from GGS_DATA_INFO a
        where
            USE_YN = 'Y'
            and dat_no='{id}'
        
        union all
        
        select
            b.dat_no,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where b.CLSF_CD = a1.cmmn_cd) as clsf_nm,
            b.dat_nm,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where b.RGN_SE = a1.cmmn_cd) as rgn_nm,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where b.PD_SE = a1.cmmn_cd) as pd_nm,
            b.REL_DAT_LIST_NM,
            b.REL_TBL_NM,   
            b.REL_FILD_NM,
            b.DAT_SRC,
            b.UPDT_CYLE,
            b.DAT_SCOP_BGNG,
            b.DAT_SCOP_END,
            b.last_mdfcn_dt
        from ggs_user_data_info b
        where
            USE_YN = 'Y'
            and dat_no='{id}'
    """.format(id=id)

    query = db.execute(text(query_template))
    return query.first()


def retrieve_stdg_data(db: Session):
    query_template = """
           select stdg_cd, stdg_nm 
           from ggs_stdg 
           where stdg_ctpv_up_cd is null 
           and stdg_nm not like '%직할시' 
           and stdg_nm != '전국' 
           and stdg_nm != '제주도'
           order by stdg_cd;
    """

    query = text(query_template)
    db_result = db.execute(query).fetchall()

    if len(db_result) == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="해당 조건의 데이터가 없습니다.")

    return [{"stdg_cd": ele[0], "stdg_nm": ele[1]} for ele in db_result]


def retrieve_chart_data(id: str, year: str, period_unit: str, detail_period: str, stdg_cd: str, limit, db: Session):
    column = get_detail_period_by_param(period_unit, detail_period)

    if stdg_cd:
        additional_condition_statg = " and stat.stdg_cd in (select stdg_cd from ggs_stdg where stdg_ctpv_up_cd = '{}' and stdg_sgg_up_cd is null) ".format(
            stdg_cd)
        additional_condition_ustatg = " and ustat.stdg_cd in (select stdg_cd from ggs_stdg where stdg_ctpv_up_cd = '{}' and stdg_sgg_up_cd is null) ".format(
            stdg_cd)
    else:
        additional_condition_statg = ""
        additional_condition_ustatg = ""

    query_template = """
        select 
            stat.{column}::integer,
            stdg.stdg_nm 
        from 
            ggs_statis stat
        left join 
            ggs_stdg stdg
        on 
            stat.stdg_cd = stdg.stdg_cd 
        where 
            dat_no=:id
        and
            stat.yr=:year
        and 
            stat.{column}::integer is not null
        and 
            stdg.stdg_nm  is not null
        {additional_condition_statg}
        
        union all
        
        select 
            ustat.yr_vl::integer,
            stdg.stdg_nm 
        from 
            ggs_user_statis ustat 
        left join 
            ggs_stdg stdg
        on 
            ustat.stdg_cd = stdg.stdg_cd 
        where 
            dat_no=:id
        and
            ustat.yr=:year
        and 
            ustat.yr_vl::integer is not null
        and 
            stdg.stdg_nm is not null
        {additional_condition_ustatg}
    """.format(column=column,
               additional_condition_statg=additional_condition_statg,
               additional_condition_ustatg=additional_condition_ustatg)

    params = {
        "year": year,
        "id": id
    }

    if limit is not None:
        query_template += "\nlimit :limit"
        params["limit"] = limit

    query = text(query_template)
    db_result = db.execute(query, params).fetchall()

    if len(db_result) == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="해당 조건의 데이터가 없습니다.")
    return db_result


def get_dat_nm_by_dat_no(id: str, db: Session):
    return db.execute(text("""
    select gdi.dat_nm from ggs_data_info gdi where dat_no=:id
    union all
    select gudi.dat_nm from ggs_user_data_info gudi where dat_no=:id
    """), {"id": id}).first()[0]


def get_chart_data_response_form(db_result, year: str, chart_type: str, dat_nm):
    if chart_type == "pie":
        series_option = EChartSeriesOption(data=[{"value": ele[0], "name": ele[1]} for ele in db_result], type="pie")
        pie_option = EChartPieOption(
            title=TitleEChartOption(text=f"{dat_nm} Pie Chart", subtext="", left="center"),
            series=series_option
        )
        return pie_option

    elif chart_type == "bar":
        x_axis_data = [ele[1] for ele in db_result]
        series_data = [ele[0] for ele in db_result]

        x_axis_option = EChartXAxisOption(type="category", data=x_axis_data)
        y_axis_option = EChartYAxisOption(type="value")
        series_option = EChartSeriesOption(data=series_data, type="bar")

        bar_option = EChartBarOption(
            title=TitleEChartOption(text=f"{dat_nm} Bar Chart", subtext="", left="center"),
            series=series_option,
            xAxis=x_axis_option,
            yAxis=y_axis_option
        )
        return bar_option

    elif chart_type == "histogram":
        histogram_data = get_histogram_data([db_result[i][0] for i in range(len(db_result))])
        x_axis_data = [str(ele["x_axis"]) for ele in histogram_data]
        series_data = [ele["count"] for ele in histogram_data]

        x_axis_option = EChartXAxisOption(type="category", data=x_axis_data)
        y_axis_option = EChartYAxisOption(type="value")
        series_option = EChartSeriesOption(data=series_data, type="bar")

        bar_option = EChartBarOption(
            title=TitleEChartOption(text=f"{dat_nm} Histogram", subtext="", left="center"),
            series=series_option,
            xAxis=x_axis_option,
            yAxis=y_axis_option
        )
        return bar_option

    else:
        raise Exception(f"잘못된 차트 형식 : {chart_type}")


def get_histogram_data(data):
    data = sorted(data)

    num_bins = 100
    data_min = min(data)
    data_max = max(data)
    bin_width = (data_max - data_min) // num_bins

    bins = [0] * num_bins

    for d in data:
        bin_index = min(int((d - data_min) // bin_width), num_bins - 1)
        bins[bin_index] += 1

    histogram_data = []
    for i in range(num_bins):
        x_position = data_min + (bin_width * i) + int(bin_width / 2)  # Use the midpoint of the bin
        histogram_data.append({
            "x_axis": x_position,
            "count": bins[i]
        })
    return histogram_data


def retrieve_filter_list():
    return {
        "period_unit": ["year", "half", "month"],
    }


def retrieve_filter_detail_list(id: str, db: Session):
    query_template = """
        select 
            pd_se
        from 
            ggs_data_info
        where
            dat_no = :id
        """

    params = {
        "id": id
    }

    query = text(query_template)
    pd_se = db.execute(query, params).fetchone()[0]

    detail_period_list = []
    period_unit = ""

    if pd_se == "M030004":
        detail_period_list = ["all"]
        period_unit = "year"
    elif pd_se == "M030003":
        detail_period_list = ["1", "2"]
        period_unit = "half"
    elif pd_se == "M030001":
        period_unit = "month"

        query_template = """
        SELECT 
            CASE 
                WHEN (SELECT COUNT(jan) FROM ggs_statis WHERE dat_no = :id AND jan IS NOT NULL) > 0 
                THEN '1' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(feb) FROM ggs_statis WHERE dat_no = :id AND feb IS NOT NULL) > 0 
                THEN '2' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(mar) FROM ggs_statis WHERE dat_no = :id AND mar IS NOT NULL) > 0 
                THEN '3' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(apr) FROM ggs_statis WHERE dat_no = :id AND apr IS NOT NULL) > 0 
                THEN '4' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(may) FROM ggs_statis WHERE dat_no = :id AND may IS NOT NULL) > 0 
                THEN '5' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(jun) FROM ggs_statis WHERE dat_no = :id AND jun IS NOT NULL) > 0 
                THEN '6' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(july) FROM ggs_statis WHERE dat_no = :id AND july IS NOT NULL) > 0 
                THEN '7' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(aug) FROM ggs_statis WHERE dat_no = :id AND aug IS NOT NULL) > 0 
                THEN '8' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(sep) FROM ggs_statis WHERE dat_no = :id AND sep IS NOT NULL) > 0 
                THEN '9' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(oct) FROM ggs_statis WHERE dat_no = :id AND oct IS NOT NULL) > 0 
                THEN '10' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(nov) FROM ggs_statis WHERE dat_no = :id AND nov IS NOT NULL) > 0 
                THEN '11' 
                ELSE NULL 
            END AS result
            UNION ALL
        SELECT 
            CASE 
                WHEN (SELECT COUNT(dec) FROM ggs_statis WHERE dat_no = :id AND dec IS NOT NULL) > 0 
                THEN '12' 
                ELSE NULL 
            END AS result;
            """

        params = {
            "id": id
        }

        detail_period_list = [ele[0] for ele in db.execute(text(query_template), params).fetchall()]

    query_template = """
        select distinct yr from ggs_statis gs where dat_no = :id
    """

    params = {
        "id": id
    }
    year_list = [ele[0] for ele in db.execute(text(query_template), params).fetchall()]
    return {
        "year_list": year_list,
        "period_unit": period_unit,
        "detail_period_list": detail_period_list
    }


def get_pivoted_df(variable_list: List[str],
                   period_unit: Literal["year", "month", "quarter", "half"],
                   db: Session
                   ):
    if len(variable_list) > 10:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="variable list의 최대 개수는 10개입니다.")

    value_period_list = get_value_period_list(period_unit)

    query_template = """
        SELECT
            stat.stdg_cd,
            stat.yr,
            stat.dat_no,
            info.dat_nm,
            stdg.stdg_nm,
            stat.jan,
            stat.feb,
            stat.mar,
            stat.apr,
            stat.may,
            stat.jun,
            stat.july,
            stat.aug,
            stat.sep,
            stat.oct,
            stat.nov,
            stat.dec,
            stat.qu_1,
            stat.qu_2,
            stat.qu_3,
            stat.qu_4,
            stat.ht_1,
            stat.ht_2,
            stat.yr_vl
        FROM ggs_statis stat
        JOIN ggs_data_info info ON stat.dat_no = info.dat_no
        JOIN ggs_stdg stdg ON stat.stdg_cd = stdg.stdg_cd 
        WHERE stat.dat_no IN ({lst})
            union all
        SELECT
            ustat.stdg_cd,
            ustat.yr,
            ustat.dat_no,
            uinfo.dat_nm,
            stdg.stdg_nm,
            ustat.jan,
            ustat.feb,
            ustat.mar,
            ustat.apr,
            ustat.may,
            ustat.jun,
            ustat.july,
            ustat.aug,
            ustat.sep,
            ustat.oct,
            ustat.nov,
            ustat.dec,
            ustat.qu_1,
            ustat.qu_2,
            ustat.qu_3,
            ustat.qu_4,
            ustat.ht_1,
            ustat.ht_2,
            ustat.yr_vl
        FROM ggs_user_statis ustat
        JOIN ggs_user_data_info uinfo ON ustat.dat_no = uinfo.dat_no
        JOIN ggs_stdg stdg ON ustat.stdg_cd = stdg.stdg_cd 
        WHERE ustat.dat_no IN ({lst})
    """
    lst = ""
    for variable in variable_list:
        lst += "'{}', ".format(variable)
    lst = lst[:-2]
    query = text(query_template.format(lst=lst))

    result = db.execute(query)

    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    melted_df = pd.melt(df, id_vars=['yr', 'stdg_nm', 'dat_no', 'dat_nm'], value_vars=value_period_list)
    pivoted_df = pd.pivot_table(melted_df, values='value', index=['yr', 'stdg_nm', 'variable'], columns='dat_no')
    dat_no_dat_nm_dict = df.set_index('dat_no')['dat_nm'].to_dict()

    _uuid = uuid.uuid4()

    pivoted_df.to_csv("./data{}.csv".format(_uuid), encoding="utf-8-sig")
    pivoted_df = pd.read_csv("./data{}.csv".format(_uuid))
    os.remove("./data{}.csv".format(_uuid))

    return pivoted_df, dat_no_dat_nm_dict