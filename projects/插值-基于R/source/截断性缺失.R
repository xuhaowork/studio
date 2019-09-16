# ----
# svm缺失值补全
# ----
library(kernlab)
path <- "F://myWorkplace/data/Bankscope_interpelate.txt"

raw_data <- read.table(path, encoding = "UTF-8", header = T)
rowsHasNa <- apply(raw_data, 1, function(x) any(is.na(x)))

year_list_na <- colnames(raw_data)[-1]
na_fill <- function(i, data) {
  predict_name <- year_list_na[i]
  train_data <- data[!is.na(data[, predict_name]), year_list_na[i : length(year_list_na)]]
  predict_data <- data[is.na(data[, predict_name]), year_list_na[i : length(year_list_na)]]
  print(predict_name)
  sample_data <- train_data[sample(dim(train_data)[1], replace = F), ]
  sample_train_data <- sample_data[1:round(dim(train_data)[1] * 0.9), ]
  sample_test_data <- sample_data[-(1:round(dim(train_data)[1] * 0.9)), ]
  ksvm.model <- ksvm(formula(paste(predict_name, "~.")), sample_train_data, kernel = "vanilladot")
  print(formula(paste(predict_name, "~.")))
    # 检验效果
  data[is.na(data[, predict_name]), predict_name] <- predict(ksvm.model, predict_data)
  return(data)
}

# 2013年及以前的数据有缺失值
raw_data <<- na_fill(6, raw_data)
raw_data <<- na_fill(5, raw_data)
raw_data <<- na_fill(4, raw_data)
raw_data <<- na_fill(3, raw_data)
raw_data <<- na_fill(2, raw_data)
raw_data <<- na_fill(1, raw_data)


# ----
# knn缺失值补全
# ----
k = 5
na_fill <- function(i, raw_data, logic) {
  year_list_na <- colnames(raw_data)[-1]
  predict_name <- year_list_na[i]

  train_data <- raw_data[!is.na(raw_data[, predict_name]), year_list_na[i : length(year_list_na)]]
  predict_data <- raw_data[is.na(raw_data[, predict_name]), year_list_na[i : length(year_list_na)]]
  
  if(logic) {
    train_data_f <- raw_data[!is.na(raw_data[, predict_name]), year_list_na[-i]]
    train_data <- train_data_f[apply(train_data_f, 1, function(x) !any(is.na(x))), ]
    predict_data <- raw_data[is.na(raw_data[, predict_name]), year_list_na[-i]]
  }
  

  knn_by_y <- function(y) {
    distances <- apply(train_data, 1, function(x) {
      dist(rbind(x[2: length(x)], y[2:length(x)]), "euclidean")
    })
    
    return(mean(train_data[order(distances)[1:k], predict_name]))
  }
  
  raw_data[is.na(raw_data[, predict_name]), predict_name] <- apply(predict_data, 1, knn_by_y)
  
  return(raw_data)
}

raw_data <<- na_fill(6, raw_data, F)
raw_data <<- na_fill(5, raw_data, F)
raw_data <<- na_fill(4, raw_data, F)
raw_data <<- na_fill(3, raw_data, F)
raw_data <<- na_fill(2, raw_data, F)
raw_data <<- na_fill(1, raw_data, F)

output <- "F://myWorkplace/data/Bankscope_interpelate_output.csv"
write.csv(raw_data, output, row.names = F)

# --------
# 外资银行
# --------
k = 3
path_foreign_capital <- "file:///F:/myWorkplace/data/Bankscope_foreign_capital.csv"

raw_data_foreign_capital2 <- read.csv(path_foreign_capital, encoding = "UTF-8", header = T)
raw_data_foreign_capital <- read.csv(path_foreign_capital, encoding = "UTF-8", header = T)[, -1]
rowsHasNa_foreign_capital <- apply(raw_data_foreign_capital, 1, function(x) any(is.na(x)))


na_fill_foreign <- function(i, raw_data) {
  train_data_f <- raw_data[!is.na(raw_data[, i]), ]
  train_data <- train_data_f[apply(train_data_f, 1, function(x) !any(is.na(x))), ]
  predict_data <- raw_data[is.na(raw_data[, i]), ]
  # print(train_data)
  
  knn_by_y <- function(y) {
    distances <- apply(train_data, 1, function(x) {
      dist(rbind(x[-i], y[-i]), "euclidean")
    })
    
    return(mean(train_data[order(distances)[1:k], i]))
  }
  
  raw_data[is.na(raw_data[, i]), i] <- apply(predict_data, 1, knn_by_y)
  
  return(raw_data)
}

raw_data_foreign_capital <- na_fill_foreign(11, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(10, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(9, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(8, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(7, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(6, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(5, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(4, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(3, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(2, raw_data_foreign_capital)
raw_data_foreign_capital <- na_fill_foreign(1, raw_data_foreign_capital)

first_dt <- read.csv(path_foreign_capital, encoding = "UTF-8", header = T)[, 1]
raw_data_foreign_capital <- data.frame("bank" = first_dt, raw_data_foreign_capital)

output2 <- "F://myWorkplace/data/Bankscope_foreign_capital_output.csv"
write.csv(raw_data_foreign_capital, output2, row.names = F)
