import base64
import io
import os
from abc import abstractmethod, ABCMeta

import numpy
import numpy as np
import pandas as pd
from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
import uuid
import pickle

from sklearn.mixture import GaussianMixture
import dataframe_image as dfi

from core.crs_converter import convert_coordinates

from utils.logging_module import logger

BASE_PATH = "./output/clustering/"


class BaseModule(metaclass=ABCMeta):
    def __init__(self, data: pd.DataFrame, dat_no_dat_nm_dict: dict):
        self.uuid = uuid.uuid4()
        logger.info("class uuid : " + str(self.uuid))
        self.directory: str = None
        self.data: pd.DataFrame = data
        self.model: object = None
        self.db_connection: object = None
        self.name_dict: dict = dat_no_dat_nm_dict

    @abstractmethod
    def fit(self, n_init=100, max_iter=300) -> None:
        pass

    @abstractmethod
    def predict(self, data):
        pass

    def save_model(self) -> None:

        if not self.model:
            raise AttributeError("model is not fitted yet")

        self._mkdir()

        with open(self.directory + "/model.pickle", "wb") as fw:
            pickle.dump(self, fw)

    def _mkdir(self) -> None:

        if not os.path.exists(BASE_PATH):
            os.mkdir(BASE_PATH)

        if not self.directory:
            self.directory = BASE_PATH + str(self.uuid)
            os.mkdir(self.directory)


class BaseClusteringModule(BaseModule):

    def __init__(self, data, dat_no_dat_nm_dict: dict):
        super().__init__(data, dat_no_dat_nm_dict)

    @abstractmethod
    def set_optimal_k(self, method: str) -> None: pass

    @abstractmethod
    def save_k_method_output_plot(self) -> None: pass

    @abstractmethod
    def get_cluster_output_plot(self) -> None: pass

    @abstractmethod
    def save_data_scatter_plot(self) -> None: pass

    @abstractmethod
    def fit(self, n_init=100, max_iter=300) -> None: pass

    @abstractmethod
    def predict(self, data): pass


class GMMModule(BaseClusteringModule):
    optimal_k_methods = {"BIC", "AIC"}

    def __init__(self, data: pd.DataFrame, dat_no_dat_nm_dict: dict):
        super().__init__(data, dat_no_dat_nm_dict)

        self.labels = []
        self.optimal_k: int = 2
        self.k_method: str = None
        self.k_range: range = range(2, 10)
        self.bic_scores = []
        self.aic_scores = []

    def __str__(self):
        return """
        GMM model
        uuid : {uuid}
        k_method : {k_method}
        k : {k}
        """.format(uuid=self.uuid, k_method=self.k_method, k=self.optimal_k)

    def set_k_range(self, start, end) -> None:
        if start < 2:
            raise ValueError("start must be larger than 1")
        self.k_range = range(start, end)

    def set_optimal_k(self, method: str = "AIC", fixed_size=2) -> None:

        if method == "fixed":
            self.optimal_k = fixed_size
            return

        if method and method not in self.optimal_k_methods:
            raise ValueError("not supported method")

        for n in self.k_range:
            gmm = GaussianMixture(n_components=n)
            gmm.fit(self.data)
            self.bic_scores.append(gmm.bic(self.data))
            self.aic_scores.append(gmm.aic(self.data))

        if method == "BIC":
            self.optimal_k = list(self.k_range)[np.argmin(self.bic_scores)]
        elif method == "AIC":
            self.optimal_k = list(self.k_range)[np.argmin(self.aic_scores)]

        logger.info("optimal k is set as : " + str(self.optimal_k))

    def fit(self, n_init=10, max_iter=100, reg_covar=0.000001) -> None:
        # print(self.data.iloc[:, 3:])
        if not len(self.data):
            raise AttributeError("data must be initialized")
        self.data = self.data.fillna(0)
        self.model = GaussianMixture(
            n_components=self.optimal_k,
            n_init=n_init,
            max_iter=max_iter,
            reg_covar=reg_covar
        ).fit(self.data.iloc[:, 3:])
        # ).fit(self.data)
        self.labels = self.model.predict(self.data.iloc[:, 3:])  # Get cluster labels
        # self.labels = self.model.predict(self.data)  # Get cluster labels
        self.data["labels"] = self.labels

        logger.info("model is successfully fitted")

    def predict(self, data):
        return NotImplemented

    def save_k_method_output_plot(self) -> None:

        plt.clf()
        self._mkdir()
        if not self.data.any():
            raise AttributeError("data must be initialized")

        plt.figure(figsize=(10, 6))
        plt.plot(self.k_range, self.bic_scores, label='BIC')
        plt.plot(self.k_range, self.aic_scores, label='AIC')
        plt.xlabel('Number of Clusters')
        plt.ylabel('Score')
        plt.title('BIC and AIC Scores for GMM')
        plt.legend()

        # Find the index of minimum BIC and AIC scores
        min_bic_idx = np.argmin(self.bic_scores)
        min_aic_idx = np.argmin(self.aic_scores)

        # Add markers for minimum scores
        plt.scatter(list(self.k_range)[np.argmin(self.bic_scores)], self.bic_scores[min_bic_idx], color='blue',
                    marker='o', label='Min BIC')
        plt.scatter(list(self.k_range)[np.argmin(self.aic_scores)], self.aic_scores[min_aic_idx], color='red',
                    marker='o', label='Min AIC')
        plt.savefig(self.directory + '/aic_bic_scores.jpg')

    def get_cluster_output_plot(self) -> None:
        plt.clf()
        if not self.model:
            raise AttributeError("model is not fitted yet")

        if not len(self.data):
            raise AttributeError("data must be initialized")

        for label in range(self.optimal_k):
            plt.scatter(self.data.iloc[self.labels == label, 3], self.data.iloc[self.labels == label, 4],
                        label=f'Cluster {label + 1}')
        plt.legend()
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", dpi=300)
        buffer.seek(0)
        base64_image = base64.b64encode(buffer.read()).decode()
        logger.info("clustering output plot saved successfully")
        return base64_image

    def save_data_scatter_plot(self) -> None:
        plt.clf()
        if not len(self.data):
            raise AttributeError("data must be initialized")

        self._mkdir()
        plt.scatter(self.data[:, 0], self.data[:, 1])
        plt.savefig(self.directory + '/data_scatter_plot.jpg')
        logger.info("data scatter plot saved successfully")

    def get_clustering_result(self):
        """
        clustering 된 결과를 반환한다
        index(지역코드), label(클러스터링 레이블)의 헤더로 구성
        """
        # columns = ["yr", "stdg_nm", "variable", "labels"]
        # selected_data = self.data[columns]
        # json_dict = selected_data.to_dict(orient='records')

        # 기획 변경. label별 count를 집계하는 걸로
        result_df = self.data.groupby('labels').size().reset_index(name='count')
        result_df.index = [''] * len(result_df)

        buffer = io.BytesIO()
        dfi.export(result_df, buffer)
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()
        logger.info("clustering result table saved successfully")
        return base64_table

    def get_spatial_result_as_json(self, crs):
        output = []
        for index, row in self.data.iterrows():
            converted_x, converted_y = convert_coordinates(row.iloc[3], row.iloc[4], crs)
            output.append({'x_coord': converted_x, 'y_coord': converted_y, 'label': row['labels']})
        return output


class KMeansModule(BaseClusteringModule):
    optimal_k_methods = {"wcss", "silhouette"}

    def __init__(self, data: numpy.array):
        super().__init__(data)

        self.optimal_k: int = 2
        self.k_method: str = None
        self.silhouette_scores: list = []
        self.wcss: list = []
        self.k_range: range = range(2, 10)

    def __str__(self):
        return """
        K-Means model
        uuid : {uuid}
        k_method : {k_method}
        k : {k}
        """.format(uuid=self.uuid, k_method=self.k_method, k=self.optimal_k)

    def set_k_range(self, start, end) -> None:
        if start < 2:
            raise ValueError("start must be larger than 1")
        self.k_range = range(start, end)

    def set_optimal_k(self, method: str = "silhouette", fixed_size=2) -> None:

        if method and method not in self.optimal_k_methods:
            raise ValueError("not supported method")

        if method == "silhouette":
            for _k in self.k_range:
                kmeans = KMeans(n_clusters=_k, n_init=30, max_iter=30).fit(self.data)
                self.silhouette_scores.append(silhouette_score(self.data, kmeans.labels_))
            self.optimal_k = self.k_range[np.argmax(self.silhouette_scores)]
            self.k_method = method

        elif method == "wcss":
            for _k in self.k_range:
                kmeans = KMeans(n_clusters=_k, n_init=30, max_iter=30).fit(self.data)
                self.wcss.append(kmeans.inertia_)
            elbow_index = np.argmin(np.diff(self.wcss)) + 1
            self.optimal_k = self.wcss[elbow_index]
            self.k_method = method

        else:
            self.optimal_k = fixed_size
            self.k_method = None

        logger.info("optimal k is set as : " + str(self.optimal_k))

    def fit(self, n_init=100, max_iter=300) -> None:

        if not self.data.any():
            raise AttributeError("data must be initialized")

        self.model = KMeans(
            n_clusters=self.optimal_k,
            n_init=n_init,
            max_iter=max_iter
        ).fit(self.data)

        logger.info("model is successfully fitted")

    def predict(self, data):
        return NotImplemented

    def save_k_method_output_plot(self) -> None:

        plt.clf()
        self._mkdir()
        if not self.data.any():
            raise AttributeError("data must be initialized")

        if self.k_method == "silhouette":
            plt.bar(self.k_range, self.silhouette_scores)
            plt.xlabel('Number of clusters (k)')
            plt.ylabel('Silhouette Score')
            plt.title('Silhouette Scores for Different Number of Clusters')
            max_index = np.argmax(self.silhouette_scores)
            plt.bar(self.k_range[max_index], self.silhouette_scores[max_index], color='red')
            plt.savefig(self.directory + "/silhouette_scores.jpg")
            logger.info("silhouette scores plot saved successfully")

        elif self.k_method == "wcss":
            plt.plot(self.k_range, self.wcss, marker='o')
            plt.xlabel('Number of Clusters (k)')
            plt.ylabel('WCSS')
            plt.title('Elbow Point Plot')
            plt.axvline(x=self.optimal_k, color='r', linestyle='--', label='Elbow Point')
            plt.legend()
            plt.savefig(self.directory + "/wcss.jpg")
            logger.info("elbow point plot saved successfully")

        else:
            logger.warning("no screenshot to save")

    def get_cluster_output_plot(self) -> None:
        plt.clf()
        if not self.model:
            raise AttributeError("model is not fitted yet")

        if not self.data.any():
            raise AttributeError("data must be initialized")

        labels = self.model.labels_

        for label in range(self.optimal_k):
            plt.scatter(self.data[labels == label, 0], self.data[labels == label, 1], label=f'Cluster {label + 1}')

        plt.legend()

        self._mkdir()
        plt.savefig(self.directory + '/cluster_output.jpg')
        logger.info("clustering output plot saved successfully")

    def save_data_scatter_plot(self) -> None:
        plt.clf()
        if not self.data.any():
            raise AttributeError("data must be initialized")

        self._mkdir()
        plt.scatter(self.data[:, 0], self.data[:, 1])
        plt.savefig(self.directory + '/data_scatter_plot.jpg')
        logger.info("data scatter plot saved successfully")


