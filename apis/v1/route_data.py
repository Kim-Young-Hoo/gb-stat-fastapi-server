from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, Query
from schemas.data import *
from db.repository.data import *
from db.session import get_db
from core.crs_converter import ALLOWED_CRS

router = APIRouter()


class ChartType(str, Enum):
    PIE = "pie"
    HISTOGRAM = "histogram"
    BAR = "bar"


@router.get("/variable", status_code=status.HTTP_200_OK)
def get_variable_list(region: Literal["all", "gsbd"],
                      period_unit: Literal["year", "month", "quarter", "half"],
                      db: Session = Depends(get_db)):
    """
    통계업무지원 특화서비스 데이터 카탈로그 목록을 반환한다.
    :param db: db session
    :return: 1,2 depth 형태의 카테고리명 string value json
    """
    variable_list = retrieve_variable_list(region, period_unit, db)
    return variable_list


@router.get("/variable/{id}", response_model=ShowVariableDetail, status_code=status.HTTP_200_OK)
def get_variable_detail(id: str, db: Session = Depends(get_db)):
    """
    통계업무지원 특화서비스에서 2depth의 상세보기 아이콘을 클릭할 시 데이터 성질에 대한 결과를 반환한다.
    :param id: variable의 아이디 ex) M010001
    :param db: db session
    :return: json 데이터
    """
    variable_detail = retrieve_variable_detail(id, db)

    if not variable_detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"variable with ID {id} does not exist")

    return ShowVariableDetail(
        name=variable_detail.dat_nm,
        source=variable_detail.dat_src,
        category=variable_detail.rel_dat_list_nm,
        region_unit=variable_detail.rgn_nm,
        update_cycle=variable_detail.updt_cyle,
        last_update_date=variable_detail.last_mdfcn_dt,
        data_scope=variable_detail.dat_scop_bgng + "-" + variable_detail.dat_scop_end
    )


@router.get("/filter-list", response_model=ShowFilterData, status_code=status.HTTP_200_OK)
def get_filter_list():
    """
    메뉴 상단 필터에 들어가야 할 목록들을 반환한다.
    """
    filter_list = retrieve_filter_list()
    return filter_list


@router.get("/variable/{id}/chart-data", response_model=Union[EChartBarOption, EChartPieOption],
            status_code=status.HTTP_200_OK)
def get_variable_chart_data(id: str,
                            year: str,
                            period_unit: str,
                            detail_period: str,
                            stdg_cd: Optional[str] = None,
                            chart_type: ChartType = Query(...),
                            limit: Optional[int] = None,
                            db: Session = Depends(get_db)):
    print(stdg_cd)
    variable_data = retrieve_chart_data(id, year, period_unit, detail_period, stdg_cd, limit, db)
    variable_name = get_dat_nm_by_dat_no(id, db)
    result = get_chart_data_response_form(variable_data, year, chart_type, variable_name)

    if not result:
        raise HTTPException(detail=f"variable with ID {id} does not exist")

    return result


@router.get("/filter-list/{id}", status_code=status.HTTP_200_OK)
def get_filter_list(id: str, db: Session = Depends(get_db)):
    """
    메뉴 상단 필터에 들어가야 할 목록들을 반환한다.
    """
    filter_list = retrieve_filter_detail_list(id, db)
    return filter_list


@router.get("/epsg-list", status_code=status.HTTP_200_OK)
def get_epsg_list():
    return {"data": ALLOWED_CRS}


@router.get("/stdg-list", status_code=status.HTTP_200_OK)
def get_stdg_list(db: Session = Depends(get_db)):
    return retrieve_stdg_data(db)
