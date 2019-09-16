# coding: utf-8

# ----
# 模型读取
# ----


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
learning_rate = 0.01
loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=y_head, labels=y))
optimizer = tf.train.AdamOptimizer(learning_rate)
train = optimizer.minimize(loss)

init = tf.global_variables_initializer()

correct_prediction = tf.equal(tf.argmax(y, 1), tf.argmax(y_head, 1))  # tf.argmax
correction = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))  # tf.cast 转换类型，将bool转为float，从而求得准确率

# 模型保存
saver = tf.train.Saver()
DIR = "D://tensorboardLogDir/eightWeek/"

# 迭代100次，进行mini_batch梯度下降
epoch_n = 100
with tf.Session() as session:
    session.run(init)
    batch_x, batch_y = mnist.train.next_batch(batch_size)
    accuracy = session.run(correction, feed_dict={x: mnist.test.images, y: mnist.test.labels})  # 此处是最小化
    print("第一次测试模型的准确率:" + str(accuracy))

    saver.restore(session, DIR + "net/my_save.ckpt")
    accuracy = session.run(correction, feed_dict={x: mnist.test.images, y: mnist.test.labels})  # 此处是最小化
    print("第二次测试模型的准确率:" + str(accuracy))
    session.close()
