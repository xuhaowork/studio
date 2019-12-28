
from sklearn.cluster import SpectralClustering
import numpy as np

X = np.array([[1, 1], [2, 1], [1, 0],
              [4, 7], [3, 5], [3, 6]])
X1 = np.array([[1, 1], [2, 1]])
params = {}
params['affinity'] = X1
print(type(params))
clustering = SpectralClustering(affinity='precomputed', assign_labels='discretize', coef0=1,
                                degree=3, eigen_solver=None, eigen_tol=0.0, gamma=1.0,
                                kernel_params=None, n_clusters=2, n_init=10, n_jobs=None,
                                n_neighbors=10, random_state=0).set_params(**params)
# print(clustering)