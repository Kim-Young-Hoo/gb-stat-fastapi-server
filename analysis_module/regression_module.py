import base64
import io
import os
import uuid
from typing import List

import pandas as pd
from statsmodels.stats.anova import anova_lm
from statsmodels.stats.outliers_influence import variance_inflation_factor

from utils.logging_module import logger
import statsmodels.api as sm
from statsmodels.formula.api import ols
import dataframe_image as dfi

BASE_PATH = "./output/regression/"


class RegressionModule:

    def __init__(self, data: pd.DataFrame, target_column_id: str, dat_no_dat_nm_dict: dict) -> object:

        self.uuid = uuid.uuid4()
        logger.info("class uuid : " + str(self.uuid))
        self.data = data
        self.data = self.data.fillna(0)

        self.y_column_id: str = target_column_id
        self.X_column_id_list: List[str] = self.data.iloc[:, 3:].columns.to_list()

        self.X_column_id_list.remove(self.y_column_id)
        self.directory: str = None
        self.model = None
        self.name_dict: dict = dat_no_dat_nm_dict

    def save_descriptive_statistics_table(self):
        if self.data.empty:
            raise AttributeError("data must be initialized")

        statistics = self.data.describe()
        statistics = statistics.rename(columns=self.name_dict)
        statistics = statistics.T
        statistics = statistics.iloc[1:, :]
        statistics = statistics.applymap(lambda x: "{:.3f}".format(x) if isinstance(x, (int, float)) else x)
        statistics.columns = ['빈도', '평균', '표준편차', '최소값', '25%', '50%', '75%', '최대값']

        buffer = io.BytesIO()
        dfi.export(statistics, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("descriptive statistics table converted to base64 successfully")
        return base64_table

    def fit(self):
        formula = self.y_column_id + " ~ " + " + ".join(self.X_column_id_list)
        model = ols(formula, data=self.data.iloc[:, 3:])
        self.model = model.fit()

    def get_result_summary_table0(self) -> str:
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")

        summary_df = pd.read_html(self.model.summary().tables[0].as_html())[0]
        summary_df.iat[7, 2] = '추정값의 표준오차'
        summary_df.iat[7, 3] = (self.model.resid ** 2).mean() ** 0.5
        summary_df.iat[8, 2] = ""
        summary_df.iat[8, 3] = ""

        summary_df.iat[0, 1] = self.name_dict[summary_df.iat[0, 1]]

        summary_df.index = [''] * len(summary_df)
        summary_df.columns = ['속성', '값', '속성', '값']

        buffer = io.BytesIO()
        dfi.export(summary_df, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("summary table converted to base64 successfully")

        return base64_table

    def get_result_summary_table1(self) -> str:
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")

        summary_df = pd.read_html(self.model.summary().tables[1].as_html())[0]
        summary_df = summary_df.iloc[1:, :]
        summary_df.columns = ['변수명', '비표준화계수(B)', '표준오차', '자유도', 'P>[t]', '[0.025', '0.975]']

        name_dict = self.name_dict
        name_dict["Intercept"] = "(상수)"
        summary_df['변수명'] = summary_df['변수명'].replace(name_dict)

        summary_df.index = [''] * len(summary_df)

        X = sm.add_constant(self.data.loc[:, self.X_column_id_list])
        summary_df['표준화계수'] = list(map(float, summary_df['비표준화계수(B)'].values)) / X.std(axis=0).values


        # 다중 공선성 컬럼 추가
        summary_df['VIF'] = [variance_inflation_factor(self.model.model.exog, i) for i in
                             range(self.model.model.exog.shape[1])]
        # 공차 컬럼 추가. 공차는 그냥 VIF의 inverse라고 함.
        summary_df['공차'] = 1 / summary_df['VIF']

        buffer = io.BytesIO()
        dfi.export(summary_df, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("summary table converted to base64 successfully")

        return base64_table

    def get_result_summary_table2(self) -> str:
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")

        summary_df = pd.read_html(self.model.summary().tables[2].as_html())[0]
        summary_df.index = [''] * len(summary_df)
        summary_df.columns = ['속성', '값', '속성', '값']

        buffer = io.BytesIO()
        dfi.export(summary_df, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("summary table converted to base64 successfully")

        return base64_table

    def get_anova_lm(self):
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")

        anova_table = anova_lm(self.model)
        anova_table = anova_table.rename(
            columns={"df": "자유도", "sum_sq": "제곱합", "mean_sq": "평균제곱", "F": "F-통계량"},
            index=self.name_dict
        )
        buffer = io.BytesIO()
        dfi.export(anova_table, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        return base64_table

    def predict(self, x):
        pass

    def _mkdir(self):
        if not os.path.exists(BASE_PATH):
            os.mkdir(BASE_PATH)

        if not self.directory:
            self.directory = BASE_PATH + str(self.uuid)
            os.mkdir(self.directory)


