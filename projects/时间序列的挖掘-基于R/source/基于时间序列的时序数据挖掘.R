# 基于时间序列进行处理

# ----
# 需要的package
# ----
library(tseries)

# ----
# 读取数据
# ----
# 读取平台输出的数据
# 需要手动将hdfs的分区文件转为csv
path <- "file:///E:/上海出差/testData.csv"
data <- read.csv(path)[, -1]

ord <- order(data[, "time_stamp"])
ts_all_data <- ts(data[ord, "lables"][1 : 263])
train_data <- ts_all_data[1 : 243]
test_data <- ts_all_data[244 : 263]

# ----
# acf和pcf以及周期发现
# ----
# 查看几阶平稳
ts_data <- ts(train_data)
plot(ts_data)
adf.test(diff(ts_data)) # 1阶差分平稳，其实未查分前的p值就是0.06，已经很低了

acf <- acf(diff(ts_data, 1), lag.max = 20, plot = FALSE) # q = 2阶托尾, 检测ma滞后
plot(acf)
pacf <- pacf(diff(ts_data, 1), lag.max = 30, plot = FALSE) # p = 28阶拖尾, 检测ar滞后
plot(pacf)
arima_model <- arima(ts_data, c(28, 1, 2)) # p, d, q
tsdiag(arima_model) # 查看模型拟合效果, 注意残差的acf和Ljung-Box的p值

# 预测今后20阶的数据并查看效果
resi <- arima_model$residuals
prediction <- predict(arima_model, 20)
length(prediction)

plot(ts_all_data)
lines(ts_data - resi, col = "red")
lines(prediction$pred, col = "blue")


# 周期分解 --技术储备
train_data_ts <- ts(train_data[, "labels"], frequency = 48)
plot(train_data_ts)
train_data_components <- decompose(train_data_ts)
plot(train_data_components)


# 小波分析 --暂时不熟，没搞懂怎么用
library(wavelets)


