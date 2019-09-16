# coding: utf-8

import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

# 载入数据
mnist = input_data.read_data_sets('MNIST_data', one_hot=True)

# 认为某个图片的一行为一条词汇
n_puts = 28

# 一个图片总共28个行
max_time = 28

# lstm网络中隐藏层单元(broke)
lstm_size = 100

# 分类数
n_classes = 10

# 每个批次的大小
batch_size = 50
# 计算一共有多少个批次
n_batch = mnist.train.num_examples // batch_size

# 定义x和y的输入和输出
with tf.name_scope('input'):
    # 定义两个placeholder
    x = tf.placeholder(tf.float32, [None, 784], name='x-input')
    y = tf.placeholder(tf.float32, [None, 10], name='y-input')

# 定义权值
with tf.name_scope('layer1'):
    with tf.name_scope('W1'):
        weight = tf.Variable(tf.truncated_normal([lstm_size, n_classes], stddev=0.1))
    with tf.name_scope('b'):
        bias = tf.Variable(tf.constant(0.1, shape=[n_classes]))


def rnn(inputs, weights, biases):
    # x为feed的张量，shape为batch_size*728, 在进入rnn时需要先进行下转换
    # 转换为三维的数据，其中-1代表未定，max_time为行数，n_puts为列数，即每行的数据数
    with tf.name_scope("reshape"):
        reshape_inputs = tf.reshape(inputs, [-1, max_time, n_puts])
    # 定义一个基本的LSTM单元
    with tf.name_scope("lstm_cell"):
        lstm_cell = tf.contrib.rnn.core_rnn_cell.BasicLSTMCell(lstm_size)
        _, final_state = tf.nn.dynamic_rnn(lstm_cell, reshape_inputs, dtype=tf.float32)
        with tf.name_scope("output"):
            results = tf.nn.softmax(tf.matmul(final_state[1], weights) + biases)
    return results


with tf.name_scope("prediction"):
    prediction = rnn(x, weight, bias)

with tf.name_scope("loss"):
    loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=prediction, labels=y))

with tf.name_scope("train"):
    train_step = tf.train.AdamOptimizer(0.001).minimize(loss)

with tf.name_scope("performance"):
    # 结果存放在一个布尔列表中
    with tf.name_scope("train_accuracy"):
        correct_prediction = tf.equal(tf.argmax(prediction, 1), tf.argmax(y, 1))  # argmax返回一维张量中最大的值所在的位置
    with tf.name_scope('accuracy'):
        # 求准确率
        accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
        tf.summary.scalar('accuracy', accuracy)

# 合并所有的summary
merged = tf.summary.merge_all()

DIR = "D://tensorboardLogDir/sevenWeek/"
with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())
    train_writer = tf.summary.FileWriter(DIR + 'logs/train', sess.graph)
    test_writer = tf.summary.FileWriter(DIR + 'logs/test', sess.graph)
    for i in range(1000):
        # 训练模型
        batch_xs, batch_ys = mnist.train.next_batch(batch_size)
        sess.run(train_step, feed_dict={x: batch_xs, y: batch_ys})
        # 记录训练集计算的参数
        summary = sess.run(merged, feed_dict={x: batch_xs, y: batch_ys})
        train_writer.add_summary(summary, i)
        # 记录测试集计算的参数
        batch_xs, batch_ys = mnist.test.next_batch(batch_size)
        summary = sess.run(merged, feed_dict={x: batch_xs, y: batch_ys})
        test_writer.add_summary(summary, i)

        if i % 10 == 0:
            test_acc = sess.run(accuracy, feed_dict={x: mnist.test.images, y: mnist.test.labels})
            train_acc = sess.run(accuracy, feed_dict={x: mnist.train.images[:10000], y: mnist.train.labels[:10000]})
            print("Iter " + str(i) + ", Testing Accuracy= " + str(test_acc) + ", Training Accuracy= " + str(train_acc))

    sess.close()
