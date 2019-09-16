# 快速回归的变量选择

# 需要的library: dplyr
library(dplyr)

# ----
# 读取数据并进行特征增强
# ----
# 读取平台输出的数据
# 需要手动将hdfs的分区文件转为csv
path <- "file:///E:/上海出差/testData.csv"
data <- read.csv(path)[, -1]

# 初始的时间相位, 因为时间要作为特征, 无论训练还是预测以phase作为起始点(减去他)
phase <- 6.96787e+11
# 周期的最小粒度
period <- 30 * 24 * 60 * 60 * 1000
# 最大周期数
steps <- 34 # 48

tmp_data <- data.frame(data[, "labels"], "x" = data[, "time_stamp"] - phase)
tmp_colnames <- c("labels", "x")
for(i in 1 : steps){
  tmp_data <- data.frame(tmp_data, sin4name = sin(tmp_data[, "x"] * 2 * pi / (i * period)))
  tmp_colnames <- append(tmp_colnames, paste("sinx_", i))
  tmp_data <- data.frame(tmp_data, cos4name = cos(tmp_data[, "x"] * 2 * pi / (i * period)))
  tmp_colnames <- append(tmp_colnames, paste("cosx_", i))
}
colnames(tmp_data) <- tmp_colnames
head(tmp_data)

# ----
# 线性回归并查看系数的显著性, 进行初步的筛选
# ----
lm_forAllVariables_sol <- lm(labels ~ 1 + ., tmp_data)
summary(lm_forAllVariables_sol) # 发现34之后系数大量为NA, 有共线性, steps取34

# ----
# 逐步回归 通过AIC分别由后往前和由前往后进行特征筛选
# ----
# 由前向后
model_forward <- step(lm_forAllVariables_sol, direction = "forward") 
# AIC=3131.74, r^2 = 0.9058
models_backward <- step(lm_forAllVariables_sol, direction = "backward") 
# AIC=3093.39, r^2 = 0.9038
summary(model_forward)
summary(models_backward)

# 可视化查看效果
prediction <- predict(models_backward, tmp_data)
od <- order(tmp_data[, "x"])
plot(tmp_data[, "x"][od], tmp_data[, "labels"][od])
lines(tmp_data[, "x"][od], prediction[od], col = "red")


