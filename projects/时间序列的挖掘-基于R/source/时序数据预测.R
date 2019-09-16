# 时序数据预测 --时序回归所可能用到的拟合模型或回归模型

# library
library(dplyr)

# 要分析的数据 data.frame
data <- mutate(
  read.csv("file:///E:/上海出差/时序数据.csv"),
  "timestamp" = 
    (as.numeric(as.POSIXct(time, format="%Y/%m/%d")) - 696787200) / 2419200)

# 标签列 dist
# 特征列 speed
set.seed(1123)
num <- dim(data)[1]
train_num <- floor(floor(num * 0.8) / 2) * 2
develop_num <- floor((num - train_num) / 2)

train_id <- sort(sample(num, train_num, F))
train_data <- data[train_id, ]
develop_id <- sort(sample(num - train_num, develop_num, F))
develop_data <- data[-train_id, ][develop_id, ]
test_data <- data[-train_id, ][-develop_id, ]

# 评价模型性能 --均方误差
mse <- function(dt, fit){
  notNaFit <- fit[!is.na(fit)]
  correspondDt <- dt[!is.na(fit)]
  fit_num <- length(notNaFit)
  train_loess = 0.0
  for (i in 1 : fit_num) {
    train_loess = train_loess + (notNaFit[i] - correspondDt[i]) ^ 2 / fit_num
  }
  return(train_loess)
}


# ----
# loess -- stats::loess
# ----
# 训练模型 span是拟合度
loess_model <- loess(labels ~ 1 + timestamp, data = train_data, span = 0.4)
train_loess = mse(train_data[, "labels"], loess_model$fitted)
develop_loess = mse(develop_data[, "labels"], predict(loess_model, develop_data[, "timestamp"]))
# 参数调试
span_grids <- seq(0.3, 10, 0.1)
best_model = 0
mini_loss = + Inf
best_param = 0
for(span in span_grids) {
  tmp_model <- loess(labels ~ 1 + time, data = train_data, span)
  develop_loess = mse(develop_data[, "labels"], predict(tmp_model, develop_data[, "timestamp"]))
  if(develop_loess < mini_loss){
    best_model <- tmp_model
    mini_loss <- develop_loess
    best_param <- span
  }
}
print(best_param)
print(mini_loss)
# 可视化
plot(train_data)
lines(train_data$time, best_model$fitted, col = "red", lty=2, lwd=2)


# ----
# 平滑样条插值
# ----
spline_model <- smooth.spline(train_data[,"timestamp"], train_data[,"labels"], df = 2)
mse(train_data[, "labels"], spline_model$y)

findBestModel <- function(train_data, develop_data, span_grids){
  best_model = 0
  mini_loss = + Inf
  best_param = 0
  for(span in span_grids) {
    tmp_model <- loess(labels ~ 1 + timestamp, data = train_data, span = span)
    develop_loess = mse(develop_data[, "labels"], predict(tmp_model, develop_data[, "timestamp"]))
    if(develop_loess < mini_loss){
      best_model <- tmp_model
      mini_loss <- develop_loess
      best_param <- span
    }
  }
  print(best_param)
  print(mini_loss)
  return(best_model)
}

span_grids <- seq(2, 10, 1)
bestModel <- findBestModel(train_data, develop_data, span_grids)
mse(train_data[, "labels"], bestModel$y)

# 效果可视化
plot(train_data[, c("timestamp", "labels")], col = "blue", type = "p", lwd = 2, main = "样条插值")
lines(bestModel$x, bestModel$y, col = "red")
data.frame(train_data, "predict" = bestModel$y)

# ----
# arima模型试探
# ----
# 查看阶数
dim(data)
train_data_ts = ts(log10(data[, "labels"][1 : 240]), start = 1)
acf <- acf(train_data_ts, lag.max=100, plot=FALSE) # 48阶托尾, 检测ma滞后
plot(acf)
pacf <- pacf(train_data_ts, lag.max=100, plot=FALSE) # 2阶震荡, 检测ar滞后
plot(pacf)
arima_model <- arima(train_data_ts, c(9, 2, 2)) # p, d, q
tsdiag(arima_model) # 查看模型拟合效果

# 预测并查看效果
forecast_model <- predict(arima_model, 23)
mse(log10(data[, "labels"][241: 263]), forecast_model$pred)
plot((data[, "timestamp"][1: 263]), log10((data[, "labels"][1: 263])), col = "red")
points(data[, "timestamp"][241: 263], forecast_model$pred, col = "blue")


# 周期分解
train_data_ts <- ts(train_data[, "labels"], frequency = 48)
plot(train_data_ts)
train_data_components <- decompose(train_data_ts)
plot(train_data_components)


# 小波分析
library(wavelets)
