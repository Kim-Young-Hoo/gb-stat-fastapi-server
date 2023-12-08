from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from schemas.analysis import *
from db.session import get_db
from db.repository.analysis import create_correlation_analysis, create_regression_analysis, create_clustering_analysis, create_spatial_clustering_analysis

router = APIRouter()


@router.post("/correlation", response_model=ShowAnalysis, status_code=status.HTTP_201_CREATED)
def create_correlation(analysis_data: CreateCorrelation, db: Session = Depends(get_db)):
    analysis_result = create_correlation_analysis(analysis_data=analysis_data, db=db)
    return analysis_result


@router.post("/regression", response_model=ShowAnalysis, status_code=status.HTTP_201_CREATED)
def create_regression(analysis_data: CreateRegression, db: Session = Depends(get_db)):
    analysis_result = create_regression_analysis(analysis_data=analysis_data, db=db)
    return analysis_result


@router.post("/clustering", response_model=ShowAnalysis, status_code=status.HTTP_201_CREATED)
def create_clustering(analysis_data: CreateClustering, db: Session = Depends(get_db)):
    analysis_result = create_clustering_analysis(analysis_data=analysis_data, db=db)
    return analysis_result


@router.post("/clustering/spatial", response_model=ShowAnalysis, status_code=status.HTTP_201_CREATED)
def create_spatial_clustering(analysis_data: CreateSpatialClustering, db: Session = Depends(get_db)):
    analysis_result = create_spatial_clustering_analysis(analysis_data=analysis_data, db=db)
    return analysis_result

