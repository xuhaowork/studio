# coding: utf-8

# ----
# 模型的保存
# ----
# 数据集介绍
# MNIST数据集，100k的训练数据，10k的预测数据，数据由tensorflow中的examples.tutorials.mnist读取
# 数据集介绍：：Yann LeCun's website
# 由28*28的像素组成输入特征，输出特征为0-9的数字

# 可调节参数：
# --------
# batch_size, initial_weight,二次损失函数,learning_rate,epoch_n
# --------

import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

mnist = input_data.read_data_sets("MNIST_data", one_hot=True)

# mini_batch的大小
batch_size = 100
batch_n = mnist.train.num_examples // batch_size

# # 定义两个placeholder用来feed数据，分别代表x和y --784列和10列(one-hot)
x = tf.placeholder(tf.float32, [None, 784])
y = tf.placeholder(tf.float32, [None, 10])

# # ----
# # 构建多分类回归

# # 定义weight和bias，初始化分别为正态随机和0.0
initial_weight = tf.random_normal([784, 10])
weight = tf.Variable(initial_weight)
bias = tf.Variable(tf.zeros([10]))
a = tf.matmul(x, weight) + bias
y_head = tf.nn.softmax(a)

# # 定义二次损失函数并依据梯度下降法进行训练 -- 这样梯度下降的train就变成了x和y的函数
learning_rate = 0.1
loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=y_head, labels=y))
optimizer = tf.train.AdamOptimizer(learning_rate)
train = optimizer.minimize(loss)

init = tf.global_variables_initializer()

correct_prediction = tf.equal(tf.argmax(y, 1), tf.argmax(y_head, 1))  # tf.argmax找到x中等于1的最大的id
correction = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))  # tf.cast 转换类型，将bool转为float，从而求得准确率

# 模型保存
saver = tf.train.Saver()
DIR = "D://tensorboardLogDir/eightWeek/"

# 迭代100次，进行mini_batch梯度下降
epoch_n = 100
with tf.Session() as session:
    session.run(init)
    for step in range(epoch_n):
        for batch in range(batch_n):
            batch_x, batch_y = mnist.train.next_batch(batch_size)
            session.run(train, feed_dict={x: batch_x, y: batch_y})  # 此处是最小化
        corr = session.run(correction, feed_dict={x: mnist.test.images, y: mnist.test.labels})  # 基于测试集对准确率进行测试
        print("in iteration " + str(step) + "the accuracy is : " + str(corr))  # 打印准确率
    # 这里看似有问题，其实没问题，因为图没变，DAG对输入的batch依次执行梯度下降法，
    # 并执行epoch_n个周期，权重会更新epoch_n * batch_n次
    saver.save(session, DIR + "net/my_save.ckpt")  # 保存前需要保证ckpt文件保存的父目录是存在的
    session.close()
