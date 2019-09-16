m <- HoltWinters(AirPassengers, start.periods = 4, seasonal = "additive")
plot(m)
AirPassengers

install.packages("timeSeries")
install.packages("fBasics")
install.packages("fGarch")

library(timeDate)
library(timeSeries)
library(fBasics)
library(fGarch)


X.timeSeries = as.timeSeries(data(LPP2005REC))
X.mat = as.matrix(X.timeSeries)
## Not run: X.zoo = zoo(X.mat, order.by = as.Date(rownames(X.mat)))
X.mts = ts(X.mat)
model <- garchFit(100*(SPI - SBI) ~ garch(1,1), data = X.timeSeries)
# The remaining are not yet supported ...
# garchFit(100*(SPI - SBI) ~ garch(1,1), data = X.mat)
# garchFit(100*(SPI - SBI) ~ garch(1,1), data = X.zoo)
# garchFit(100*(SPI - SBI) ~ garch(1,1), data = X.mts)
plot(model)


X.timeSeries = MSFT
model <- garchFit(Open ~ garch(1,1), data = returns(X.timeSeries)) 
plot(model)
garchFit(100*(High-Low) ~ garch(1,1), data = returns(X.timeSeries)) 

predict(X.timeSeries, model)


library(tseries)
n <- 1100
a <- c(0.1, 0.5, 0.2)  # ARCH(2) coefficients
e <- rnorm(n)  
x <- double(n)
x[1:2] <- rnorm(2, sd = sqrt(a[1]/(1.0-a[2]-a[3]))) 
for(i in 3:n)  # Generate ARCH(2) process
{
  x[i] <- e[i]*sqrt(a[1]+a[2]*x[i-1]^2+a[3]*x[i-2]^2)
}
x <- ts(x[101:1100])
x.arch <- garch(x, order = c(0,2))  # Fit ARCH(2) 
summary(x.arch)                     # Diagnostic tests
plot(x.arch)


set.seed(1)
alpha0 <- 0.1
alpha1 <- 0.4
beta1 <- 0.2
w <- rnorm(10000)
a <- rep(0, 10000)
h <- rep(0, 10000)

for (i in 2:10000) {
  h[i] <- alpha0 + alpha1 * (a[i - 1]^2) + beta1 * h[i -
                                                       1]
  a[i] <- w[i] * sqrt(h[i])
}
a

acf(a)
acf(a^2)
a.garch <- garch(a, grad = "numerical", trace = FALSE)
a.garch

SP500

data(usd.hkd)
stemp <- scan("http://www.massey.ac.nz/~pscowper/ts/stemp.dat")

s <- ts(c(0, 1, 1, 2, 3, 5, 8, 13, 21, 34))
diff(s)
