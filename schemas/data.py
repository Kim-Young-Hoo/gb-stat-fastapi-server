from datetime import date, datetime
from typing import List, Dict, Union, Literal, Any

from pydantic import EmailStr, BaseModel, Field


class ShowVariableDetail(BaseModel):
    """
    변수의 상세정보를 반환하기 위한 dto
    """
    name: str
    source: str
    category: str
    region_unit: str
    update_cycle: str
    last_update_date: datetime
    data_scope: str


class ShowVariableChartData(BaseModel):
    """
    변수의 기초적인 차트를 그리기 위한 data dto
    TODO: chart type에 대해서 enum화 필요
    """
    type: str
    data: List[Dict[str, Union[str, int, float]]]


class ShowFilterData(BaseModel):
    period_unit: List[str]


class ShowFilterDetailData(BaseModel):
    year_list: List[str]
    period_unit: List[str]
    detail_period: str


class TitleEChartOption(BaseModel):
    text: str
    subtext: str
    left: str


class EChartXAxisOption(BaseModel):
    type: Literal["category", "value"]
    data: List[Any]


class EChartYAxisOption(BaseModel):
    type: str


class EChartSeriesOption(BaseModel):
    data: List[Any]
    type: str


class EChartOption(BaseModel):
    title: Union[TitleEChartOption, None]
    series: EChartSeriesOption


class EChartBarOption(EChartOption):
    xAxis: EChartXAxisOption
    yAxis: EChartYAxisOption


class EChartPieOption(EChartOption):
    pass
