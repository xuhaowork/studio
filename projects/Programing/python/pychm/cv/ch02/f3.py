import numpy as np
import cv2 as cv
from matplotlib import pyplot as plt

img = cv.imread('./opencv.png')
kernel = np.ones((5, 5), np.float32) / 25
dst = cv.filter2D(img, -1, kernel)
plt.subplot(121), plt.imshow(img), plt.title('Original')
plt.xticks([]), plt.yticks([])
plt.subplot(122), plt.imshow(dst), plt.title('Averaging')
plt.xticks([]), plt.yticks([])
plt.show()

# 进行laplacian滤波
kernel2 = np.array([[1, 1, 1], [1, -8, 1], [1, 1, 1]], np.float32)
dst = cv.filter2D(img, -1, kernel2)
alpha = 1.0
beta = 1.0
# 滤波后的图层叠加
overlapping = cv.addWeighted(img, alpha, dst, beta, 0.1)
plt.subplot(121), plt.imshow(dst), plt.title('dst')
plt.xticks([]), plt.yticks([])
plt.subplot(122), plt.imshow(overlapping), plt.title('overlapping')
plt.xticks([]), plt.yticks([])
plt.show()

# 进行锐化操作
kernel31 = np.array([[0, 0, 0], [0, 2, 0], [0, 0, 0]], np.float32)
dst1 = cv.filter2D(img, -1, kernel31)

kernel32 = np.array([[1, 1, 1], [1, 1, 1], [1, 1, 1]], np.float32) / 9
dst2 = cv.filter2D(img, -1, kernel32)

alpha = 2.0
beta = -2.0
# 滤波后的图层叠加
overlapping = cv.addWeighted(dst1, alpha, dst2, beta, 0.0)
plt.subplot(121), plt.imshow(img), plt.title('img')
plt.xticks([]), plt.yticks([])
plt.subplot(122), plt.imshow(overlapping), plt.title('overlapping')
plt.xticks([]), plt.yticks([])
plt.show()
