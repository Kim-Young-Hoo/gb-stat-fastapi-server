import os
import uuid
from typing import List, Literal

import pandas as pd
from fastapi import Depends, HTTPException
from sqlalchemy import or_, inspect, text
from sqlalchemy.orm import Session
from starlette import status

from db.session import get_db
from schemas.analysis import CreateCorrelation, CreateRegression, ShowAnalysis, CreateClustering, AnalysisResult, \
    CreateSpatialClustering
from analysis_module.regression_module import RegressionModule
from analysis_module.correlation_module import CorrelationModule
from analysis_module.clustering_module import GMMModule
from db.models.data import GgsStatis
from db.repository.data import get_pivoted_df


def create_correlation_analysis(analysis_data: CreateCorrelation, db: Session):
    pivoted_df, dat_no_dat_nm_dict = get_pivoted_df(analysis_data.variable_list,
                                                    analysis_data.period_unit,
                                                    db)

    if len(pivoted_df) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="데이터가 크기가 0입니다. 다른 데이터를 선택해주세요.")

    correlation_module = CorrelationModule(pivoted_df.iloc[:, 3:], dat_no_dat_nm_dict)
    corr_result = ShowAnalysis(data=[])

    correlation_matrix = correlation_module.get_correlation_matrix(test_side=analysis_data.test_side)
    pair_plot = correlation_module.save_pair_plot(),
    heatmap_plot = correlation_module.save_heatmap_plot(),
    descriptive_statistics_table = correlation_module.get_descriptive_statistics_table()

    corr_result.data.append(AnalysisResult(title="산점도행렬", result=pair_plot[0], format="base64"))
    corr_result.data.append(AnalysisResult(title="상관계수매트릭스", result=correlation_matrix, format="base64"))
    corr_result.data.append(AnalysisResult(title="상관계수 히트맵", result=heatmap_plot[0], format="base64"))
    corr_result.data.append(AnalysisResult(title="기술통계", result=descriptive_statistics_table, format="base64"))
    return corr_result


def create_regression_analysis(analysis_data: CreateRegression, db: Session):
    pivoted_df, dat_no_dat_nm_dict = get_pivoted_df(
        analysis_data.independent_variable_list + [analysis_data.dependent_variable],
        analysis_data.period_unit,
        db)

    if len(pivoted_df) == 0:
        raise HTTPException(status_code=404, detail="데이터가 크기가 0입니다. 다른 데이터를 선택해주세요.")

    regression_module = RegressionModule(pivoted_df, analysis_data.dependent_variable, dat_no_dat_nm_dict)
    regression_module.fit()
    regression_summary_table0 = regression_module.get_result_summary_table0()
    regression_summary_table1 = regression_module.get_result_summary_table1()
    regression_summary_table2 = regression_module.get_result_summary_table2()
    anova_table = regression_module.get_anova_lm()
    descriptive_statistics_table = regression_module.save_descriptive_statistics_table()

    regression_result = ShowAnalysis(data=[])
    regression_result.data.append(AnalysisResult(title="모형요약표1", result=regression_summary_table0, format="base64"))
    regression_result.data.append(AnalysisResult(title="모형요약표2", result=regression_summary_table1, format="base64"))
    regression_result.data.append(AnalysisResult(title="모형요약표3", result=regression_summary_table2, format="base64"))
    regression_result.data.append(AnalysisResult(title="분산분석표", result=anova_table, format="base64"))
    regression_result.data.append(AnalysisResult(title="기술통계", result=descriptive_statistics_table, format="base64"))
    return regression_result


def create_clustering_analysis(analysis_data: CreateClustering, db: Session):
    pivoted_df, dat_no_dat_nm_dict = get_pivoted_df(analysis_data.variable_list,
                                                    analysis_data.period_unit,
                                                    db)
    gmm_module = GMMModule(pivoted_df, dat_no_dat_nm_dict)
    gmm_module.optimal_k = analysis_data.n_point
    gmm_module.fit()

    clustering_result = ShowAnalysis(data=[])
    clustering_result.data.append(
        AnalysisResult(title="GMM Clustering Table", result=gmm_module.get_clustering_result(), format="json"))
    clustering_result.data.append(
        AnalysisResult(title="GMM Plot", result=gmm_module.get_cluster_output_plot(), format="base64"))

    return clustering_result


def create_spatial_clustering_analysis(analysis_data: CreateClustering, db: Session):
    pivoted_df, dat_no_dat_nm_dict = get_pivoted_df(analysis_data.variable_list,
                                                    analysis_data.period_unit,
                                                    db)
    gmm_module = GMMModule(pivoted_df, dat_no_dat_nm_dict)
    gmm_module.optimal_k = analysis_data.n_point
    gmm_module.fit()
    result = gmm_module.get_spatial_result_as_json(analysis_data.crs)

    clustering_result = ShowAnalysis(data=[])
    clustering_result.data.append(
        AnalysisResult(title="Spatial Clustering Result", result=result, format="json")
    )

    return clustering_result