library(wavethresh)
y <- example.1()$y
ynoise <- y + rnorm(512, sd=0.1)
#
# Do complex-valued wavelet shrinkage with decimated wavelets
#
est1 <- cthresh(ynoise, TI=FALSE)
#
# Do complex-valued wavelet shrinkage with nondecimated wavelets
#
est2 <- cthresh(ynoise, TI=TRUE)
#
#
#
plot(1:512, y, lty=2, type="l")
lines(1:512, est1, col=2)
lines(1:512, est2, col=3)


library(waveslim)

## Figure 7.3 from Gencay, Selcuk and Whitcher (2001)
data(ar1)
data_ts <- ts(5 * sin(1:256 / 12 * 2 * pi) + sin(1:256 / 5 * 2 * pi) )
plot(data_ts)
ar1.modwt <- modwt(data_ts, "haar", 6)


ar1.modwt.var2 <- wave.variance(ar1.modwt, type="gaussian")
ar1.modwt.var2
matplot(2^(0:5), , type="b", 
        xaxt="n", ylim=c(.025, 6), pch="*LU", lty=1, col=c(1,4,4),
        xlab="Wavelet Scale", ylab="")

ar1.modwt.bw <- brick.wall(ar1.modwt, "haar")
ar1.modwt.var2 <- wave.variance(ar1.modwt.bw, type="gaussian")
ar1.modwt.var <- wave.variance(ar1.modwt.bw, type="nongaussian")
par(mfrow=c(1,1), las=1, mar=c(5,4,4,2)+.1)
matplot(2^(0:5), , type="b", log="xy",
        xaxt="n", ylim=c(.025, 6), pch="*LU", lty=1, col=c(1,4,4),
        xlab="Wavelet Scale", ylab="")
matlines(2^(0:5), as.matrix(ar1.modwt.var)[-7,2:3], type="b",
         pch="LU", lty=1, col=3)
axis(side=1, at=2^(0:5))
legend(1, 6, c("Wavelet variance", "Gaussian CI", "Non-Gaussian CI"),
       lty=1, col=c(1,4,3), bty="n")
## Figure 7.8 from Gencay, Selcuk and Whitcher (2001)
data(exchange)
returns <- diff(log(as.matrix(exchange)))
returns <- ts(returns, start=1970, freq=12)
wf <- "d4"
J <- 6
demusd.modwt <- modwt(returns[,"DEM.USD"], wf, J)
demusd.modwt.bw <- brick.wall(demusd.modwt, wf)
jpyusd.modwt <- modwt(returns[,"JPY.USD"], wf, J)
jpyusd.modwt.bw <- brick.wall(jpyusd.modwt, wf)
returns.modwt.cov <- wave.covariance(demusd.modwt.bw, jpyusd.modwt.bw)
par(mfrow=c(1,1), las=0, mar=c(5,4,4,2)+.1)
matplot(2^(0:(J-1)), returns.modwt.cov[-(J+1),], type="b", log="x",
        pch="*LU", xaxt="n", lty=1, col=c(1,4,4), xlab="Wavelet Scale",
        ylab="Wavelet Covariance")
axis(side=1, at=2^(0:7))
abline(h=0)
returns.modwt.cor <- wave.correlation(demusd.modwt.bw, jpyusd.modwt.bw,
                                      N = dim(returns)[1])
par(mfrow=c(1,1), las=0, mar=c(5,4,4,2)+.1)
matplot(2^(0:(J-1)), returns.modwt.cor[-(J+1),], type="b", log="x",
        pch="*LU", xaxt="n", lty=1, col=c(1,4,4), xlab="Wavelet Scale",
        ylab="Wavelet Correlation")

require(grDevices)

library(wavelets)
X <- rnorm(512)
dwtobj <- dwt(X)
# Plotting wavelet coefficients of levels 1 through 6 and scaling
# coefficients of level 6.
plot.dwt(dwtobj, levels = 6)
