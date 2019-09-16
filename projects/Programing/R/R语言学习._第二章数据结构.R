# R语言学习

## 第一章
# 赋值
m <- 2
# 向上一个作用于赋值, 最高到GlobalEnvironment
m <<- 2
# 习惯约定 <- 为赋值语句, = 为传参语句

# type class mode
## type是容器的元素类型 如: character double integer
typeof(c("1", "2", "3"))
typeof(c(1, 2, 3))
typeof(c(1L, 2L, 3L))
typeof(c(T, F, T))

## class也是容器的元素类型 不过class要比type粗
class(c("1", "2", "3"))
class(c(1, 2, 3))
class(c(1L, 2L, 3L))
class(c(T, F, T))

## mode 更粗
mode(c("1", "2", "3"))
mode(c(1, 2, 3))
mode(c(1L, 2L, 3L))
mode(c(T, F, T))

typeof(list(1, 2))
class(list(1, 2))
mode(list(1, 2))

## 类型判定
class(factor(1, 2, 3)) == "factor"
class(factor(1, 2, 3)) == class(factor())
is.factor(factor(1, 2, 3))

attributes(list(1, 2, 3))
x <- cbind(a = 1:3, pi = pi) # simple matrix with dimnames
attributes(x)



## 通过上述可以发现: double|integer => numeric
## 另外R中诸多对象type mode class之间的对应关系如下
library(methods)
library(dplyr)
library(xml2)

setClass("dummy", representation(x="numeric", y="numeric"))

types <- list(
  "logical vector" = logical(),
  "integer vector" = integer(),
  "numeric vector" = numeric(),
  "complex vector" = complex(),
  "character vector" = character(),
  "raw vector" = raw(),
  factor = factor(),
  "logical matrix" = matrix(logical()),
  "numeric matrix" = matrix(numeric()),
  "logical array" = array(logical(8), c(2, 2, 2)),
  "numeric array" = array(numeric(8), c(2, 2, 2)),
  list = list(),
  pairlist = .Options,
  "data frame" = data.frame(),
  "closure function" = identity,
  "builtin function" = `+`,
  "special function" = `if`,
  environment = new.env(),
  null = NULL,
  formula = y ~ x,
  expression = expression(),
  call = call("identity"),
  name = as.name("x"),
  "paren in expression" = expression((1))[[1]],
  "brace in expression" = expression({1})[[1]],
  "S3 lm object" = lm(dist ~ speed, cars),
  "S4 dummy object" = new("dummy", x = 1:10, y = rnorm(10)),
  "external pointer" = read_xml("<foo><bar /></foo>")$node
)

type_info <- Map(
  function(x, nm)
  {
    data_frame(
      "spoken type" = nm,
      class = class(x), 
      mode  = mode(x),
      typeof = typeof(x),
      storage.mode = storage.mode(x)
    )
  },
  types,
  names(types)
) %>% bind_rows

knitr::kable(type_info)
## 最常用的类型查看class

## 类型转换
as.double(T)

## function类型



## 第二章

# ----
# 向量
# ----
# R中所有的数字其实都是一个向量
1 # [1] 1

v1 <- c(1, 2, 3)
typeof(v1)
v2 <- c(1L, 2L, 3L)
typeof(v2) # 向量元素类型

length(1) # 1可以看做长度是1的向量
# R中向量化运算
c(1, 2, 3) * 3

# vector的names
v3 <- c("a" = 1, "b" = 2, "c" = 3)
v3["a"]
attributes(v3)

v4 <- 1
names(v4) <- "name"
v4

# 矩阵
mdat <- matrix(c(1,2,3, 11,12,13), nrow = 2, ncol = 3, byrow = TRUE,
               dimnames = list(c("row1", "row2"),
                               c("C.1", "C.2", "C.3")))
class(mdat)
mdat[1, ]
class(mdat[1, ]) == class(numeric()) # 矩阵取一行类型为向量类型
mdat[1, , drop = F]
class(mdat[1, , drop = F]) # 矩阵取值中不drop掉矩阵类型，此时取一行后仍为Matrix



path <- "F://myWorkplace/data/Bankscope_interpelate.txt"
file_path <- file(path)
input <- readLines(file_path, encoding = "UTF-8")
