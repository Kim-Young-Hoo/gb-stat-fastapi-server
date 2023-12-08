import matplotlib

matplotlib.use('Agg')  # Set the backend to 'Agg'

import base64
import io
import os
import uuid
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from typing_extensions import Union, List, Literal
from utils.logging_module import logger
import seaborn as sns
import dataframe_image as dfi
from scipy.stats import pearsonr
from matplotlib import font_manager



BASE_PATH = "./output/regression/"
import matplotlib.font_manager
font_list = matplotlib.font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
[matplotlib.font_manager.FontProperties(fname=font).get_name() for font in font_list if 'Nanum' in font]
import matplotlib.pyplot as plt
plt.rc('font', family='NanumGothicCoding')
import matplotlib as mpl
mpl.rcParams['axes.unicode_minus'] = False


class CorrelationModule:

    def __init__(self, data: Union[np.ndarray, pd.DataFrame], dat_no_dat_nm_dict: dict) -> object:
        self.uuid = uuid.uuid4()
        logger.info("class uuid : " + str(self.uuid))

        if isinstance(data, np.ndarray):
            data = pd.DataFrame(data=data)
        self.X: pd.DataFrame = data
        self.X = self.X.fillna(0)
        self.selected_columns: List[str] = self.X.columns
        self.directory: str = None
        self.name_dict: dict = dat_no_dat_nm_dict

    @property
    def columns(self) -> List[str]:
        return self.X.columns

    def get_pvalue_of_correlation(self):
        num_variables = data.shape[1]
        X = self.X.values

        p_values = np.zeros((num_variables, num_variables))

        for i in range(num_variables):
            for j in range(num_variables):
                if i != j:
                    corr, p_value = pearsonr(X[i], X[j])
                    p_values[i, j] = p_value
        return p_values

    def get_correlation_matrix(self, test_side='two-sided', accent_valid_pvalue=True):

        if self.X.empty:
            raise AttributeError("data must be initialized")

        correlation_matrix = pd.DataFrame(index=self.X.columns, columns=self.X.columns, dtype=float)
        p_value_matrix = pd.DataFrame(index=self.X.columns, columns=self.X.columns, dtype=float)

        for i in range(len(self.X.columns)):
            for j in range(len(self.X.columns)):
                col1 = self.X.columns[i]
                col2 = self.X.columns[j]

                correlation, p_value = pearsonr(self.X[col1], self.X[col2], alternative=test_side)
                correlation_matrix.at[col1, col2] = correlation
                p_value_matrix.at[col1, col2] = p_value

        result_df = pd.DataFrame(index=pd.MultiIndex.from_product([self.X.columns, ['pearsonr', 'pvalue']]), columns=self.X.columns)

        for col1 in self.X.columns:
            for col2 in self.X.columns:

                result_df.at[(col1, 'pearsonr'), col2] = correlation_matrix.at[col1, col2]
                result_df.at[(col1, 'pvalue'), col2] = p_value_matrix.at[col1, col2]

        result_df = result_df.rename(index=self.name_dict)
        result_df = result_df.rename(columns=self.name_dict)

        if accent_valid_pvalue:
            for col1 in self.X.columns:
                for col2 in self.X.columns:
                    p_value = result_df.at[(self.name_dict[col1], 'pvalue'), self.name_dict[col2]]
                    pearsonr_value = result_df.at[(self.name_dict[col1], 'pearsonr'), self.name_dict[col2]]

                    if p_value < 0.01:
                        result_df.at[(self.name_dict[col1], 'pearsonr'), self.name_dict[col2]] = f"{pearsonr_value:.4f}**"
                    elif 0.01 <= p_value < 0.05:
                        result_df.at[(self.name_dict[col1], 'pearsonr'), self.name_dict[col2]] = f"{pearsonr_value:.4f}*"

        buffer = io.BytesIO()
        dfi.export(result_df, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("get_correlation_matrix converted to base64 successfully")
        return base64_table


    def save_heatmap_plot(self, method: Literal["pearson", "kendall", "spearman"] = "pearson") -> str:

        if self.X.empty:
            raise AttributeError("data must be initialized")
        data = self.X.rename(columns=self.name_dict)
        corr = data.corr(method=method)
        sns.heatmap(corr, annot=True, cmap="coolwarm", square=True)

        plt.xticks(fontsize=3, rotation=20)
        plt.yticks(fontsize=3, rotation=20)

        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", dpi=300)
        buffer.seek(0)
        base64_image = base64.b64encode(buffer.read()).decode()
        logger.info("heatmap plot saved successfully")

        return base64_image

    def save_pair_plot(self, method: Literal["pearson", "kendall", "spearman"] = "pearson") -> str:
        # plt.clf()
        if self.X.empty:
            raise AttributeError("data must be initialized")

        scatter_matrix = pd.plotting.scatter_matrix(self.X)

        for subaxis in scatter_matrix:
            for ax in subaxis:
                ax.xaxis.set_ticks([])
                ax.yaxis.set_ticks([])
                ax.set_xlabel(self.name_dict[ax.get_xlabel()], fontsize=3, rotation=20, labelpad=10)
                ax.set_ylabel(self.name_dict[ax.get_ylabel()], fontsize=3, rotation=20, labelpad=30)

        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", dpi=300)
        buffer.seek(0)
        base64_image = base64.b64encode(buffer.read()).decode()
        plt.close()

        logger.info("pair plot saved successfully")

        return base64_image

    def get_descriptive_statistics_table(self):
        if self.X.empty:
            raise AttributeError("data must be initialized")

        statistics = self.X.describe()
        statistics = statistics.rename(columns=self.name_dict)
        statistics = statistics.T
        statistics = statistics.applymap(lambda x: "{:.0f}".format(x) if isinstance(x, (int, float)) else x)
        statistics.columns = ['빈도', '평균', '표준편차', '최소값', '25%', '50%', '75%', '최대값']

        buffer = io.BytesIO()
        dfi.export(statistics, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("descriptive statistics table converted to base64 successfully")
        return base64_table

    def _mkdir(self):
        if not os.path.exists(BASE_PATH):
            os.mkdir(BASE_PATH)

        if not self.directory:
            self.directory = BASE_PATH + str(self.uuid)
            os.mkdir(self.directory)


