library(factoextra)
librar(ggplot2)
data("multishapes")

write.csv(multishapes, "E://company/multishapes.csv", row.names = F)

df <- multishapes[, 1:2]
df0<-multishapes
df0$shape<-as.factor(df0$shape)
ggplot(df0,aes(x=x,y=y,colour=shape))+geom_point()


#载入相关包
library("fpc")
#设置随机数种子
set.seed(123)
db <- fpc::dbscan(df, eps = 0.15, MinPts = 5)
fviz_cluster(db, data = df, stand = FALSE,
             ellipse = FALSE, show.clust.cent = FALSE,
             geom = "point",palette = "jco", ggtheme = theme_classic())