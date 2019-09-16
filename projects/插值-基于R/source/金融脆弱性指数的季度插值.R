# R语言
## 金融脆弱性指数的季度插值


# ----
# 数据准备
# ----
x <- 2000 : 2016 # 年份
y <- c(-48.85865688,
       -58.48989952,
       -52.59137602,
       -53.09473024,
       -50.60971871,
       -43.82799959,
       -34.11354369,
       6.17646544,
       7.37410197,
       8.49695456,
       22.70347277,
       21.81867431,
       22.12059413,
       25.84893358,
       51.08411292,
       103.671196,
       72.29083925) # 金融脆弱性指数
predict_sequence = seq(2000, 2017, by = 0.25) # 拟合序列

# ----
# 平滑样条插值
# ----
spline <- smooth.spline(x, y)
model <- predict(spline, predict_sequence)
result0 <- data.frame("time" = model$x, "y" = model$y)
# 效果
plot(x, y, col = "blue", type = "p", lwd = 2, main = "样条插值")
lines(result0[, "time"], result0[, "y"], col = "red")

# ----
# 线性插值
# ----
v <- c()
diff <- 0
for(j in 1 : length(y) - 1) {
  end = y[j + 1]
  start = y[j]
  
  for(i in 0 : 3) {
    diff <- (end - start)
    v <- append(v, start + diff * i / 4)
  }
}

for(i in 0 : 4) {
  v <- append(v, y[length(y)] + diff * i / 4)
}

result1 <- data.frame("time" = predict_sequence, "y" = v)
# 效果
plot(x, y, col = "blue", type = "p", lwd = 2, main = "样条插值")
lines(result1[, "time"], result1[, "y"], col = "red")

# ----
# 结果保存
# ----
write.csv(result0, file = "e:/平滑样条插值.csv", row.names = F)
write.csv(result1, file = "e:/线性插值.csv", row.names = F)
