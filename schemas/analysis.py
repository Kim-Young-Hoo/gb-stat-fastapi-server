from datetime import date
from typing import Optional, List, Union, Any, Literal

from pydantic import BaseModel, Field, root_validator
from pydantic.v1 import validator


class AnalysisResult(BaseModel):
    title: str  # 결과물 이름
    format: str  # 결과물 포맷
    result: Any  # 결과물 (bas64 이미지, html 등의 string)


class ShowAnalysis(BaseModel):
    """
    상관분석 결과를 반환하는 dto
    모든 필드는 base64형 image
    """
    data: List[AnalysisResult]


class BaseAnalysisInput(BaseModel):
    period_unit: Literal["year", "month", "quarter", "half"]


class CreateCorrelation(BaseAnalysisInput):
    """
    상관분석 시행하기 위한 parameter dto

    {
        "variable_list": ["M026006", "M026002"],
        "year": "2021",
        "period_unit": "yr_vl",
        "testing_side": "both",
        "valid_pvalue_accent": true
    }
    """
    variable_list: List[str]
    test_side: Literal["two-sided", "greater"]
    valid_pvalue_accent: bool


class CreateRegression(BaseAnalysisInput):
    """
    회귀분석 시행하기 위한 parameter dto
    """
    dependent_variable: str
    independent_variable_list: List[str]


class CreateClustering(BaseAnalysisInput):
    """
    군집분석 시행하기 위한 parameter dto
    """
    variable_list: List[str]
    n_point: int

    @validator('n_point')
    def check_min_n_point(cls, v):
        if v < 2:
            raise ValueError("n은 최소 2 이상입니다.")
        return v


class CreateSpatialClustering(CreateClustering):
    crs: str

