# 时序数据挖掘

install.packages("tseries")
install.packages("xts")
install.packages("TTR")
library(TTR)
library(xts)
library(tseries)

## 数据
total <- ts(c(
  132,
  96,
  128,
  180,
  225,
  205,
  253,
  253,
  207,
  286,
  245,
  193,
  166,
  178,
  201,
  196,
  255,
  211,
  221,
  254,
  231,
  266,
  350,
  265,
  285,
  160
), start = c(2017, 1), frequency = 12)
plot(total)

adf.test(total)



