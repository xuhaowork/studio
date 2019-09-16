# 平台上进行arima模型

# ----
# 获取输入数据
# ----
# 需要两列，列名分别为labels和time_stamp
tableName <- "截取一段最好连续序列_BqwzyCnS"
data <- collect(z.rdd(tableName))

train_length <- 243
ord <- order(data[, "time_stamp"])
ts_all_data <- ts(data[ord, "labels"])
train_data <- ts_all_data[1: train_length]
test_data <- ts_all_data[(train_length + 1) : length(ts_all_data)]

# ----
# arima模型
# ----
arima_model <- arima(train_data, c(28, 1, 2)) # p, d, q
train_fitted <- data.frame(
  series = 1 : train_length, 
  real_value = as.vector(train_data), 
  fitted = as.vector(train_data - arima_model$residuals),
  residual = as.vector(arima_model$residuals)
  )

test_predict <- data.frame(
  series = (train_length + 1) : length(ts_all_data), 
  real_value = test_data, 
  prediction = as.vector(predict(arima_model, length(ts_all_data) - train_length)$pred)
  )

train_fitted_df <- as.DataFrame(train_fitted)
name1 <- paste(tableName, "_train", sep = '')
createOrReplaceTempView(train_fitted_df, name1)
outputrdd.put(name1, train_fitted_df)

test_predict_df <- as.DataFrame(test_predict)
name2 <- paste(tableName, "_test", sep = '')
createOrReplaceTempView(test_predict_df, name2)
outputrdd.put(name2, test_predict_df)
