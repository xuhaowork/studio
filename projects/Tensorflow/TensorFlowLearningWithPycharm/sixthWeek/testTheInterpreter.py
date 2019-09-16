import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt

print("The head of the test file.")
# 创建数据：二次曲线的分布 + 噪声
# ----
# 创建从-0.5到0.5的等差数列，长度为200
line_data = np.linspace(-0.5, 0.5, 200)
# 为创建的数组再增加一个轴
x_data = line_data[:, np.newaxis]
noise = np.random.normal(0, 0.02, x_data.shape)

y_data = np.square(x_data) + noise

# 看下数据的类型和shape 注意python中float位数变换会导致数组变长或变短，需要astype
print(noise.shape, noise.dtype)
print(x_data.shape)
print(y_data.shape, y_data.dtype)

# 定义两个placeholder用来feed数据，分别代表x和y --1列
x = tf.placeholder(tf.float32, [None, 1])
y = tf.placeholder(tf.float32, [None, 1])

# ----
# 构建神经网络进行训练
# 假定：
#   --一个隐含层L1
#   --十个神经元
# ----

# 定义第一层的weight和bias, 都是1*10的矩阵, 初始化分别为正态随机和0.0
weight_1 = tf.Variable(tf.random_normal([1, 10]))
bias_1 = tf.Variable(tf.zeros([1, 10]))
a_L1 = tf.matmul(x, weight_1) + bias_1
z_L1 = tf.nn.tanh(a_L1)  # 这里用tanh更好一些

# 定义第二层的weight和bias，10*1的矩阵和1*1的矩阵，初始化分别为正态随机和0.0
weight_2 = tf.Variable(tf.random_normal([10, 1]))
bias_2 = tf.Variable(tf.zeros([1, 1]))
a_L2 = tf.matmul(z_L1, weight_2) + bias_2
y_head = tf.nn.tanh(a_L2)  # 这里也是用tanh更好一些

# 定义二次损失函数并依据梯度下降法进行训练 -- 这样梯度下降的train就变成了x和y的函数
loss = tf.reduce_mean(tf.square(y - y_head))
optimizer = tf.train.GradientDescentOptimizer(0.1)
train = optimizer.minimize(loss)

init = tf.global_variables_initializer()

# 运行梯度下降4000次并打印每次的结果和对应的损失函数
with tf.Session() as session:
    session.run(init)
    i = 0
    for step in range(2000):
        session.run(train, {x: x_data, y: y_data})  # 此处是最小化
        ls = session.run(loss, feed_dict={x: x_data, y: y_data})
        # 注意这里不是ls = session.run(loss), 因为loss也是DAG, 也是x和y的函数，因此同样需要feed，
        # tf中常量和运行结果可以直接打印，其他都要run后打印
        if i % 20 == 0:
            print(step, ls)  # 每隔二十次打印损失，观察收敛情况
        i += 1

        # 训练的神经网络模型中的两层的w和b
    print("weight_1", session.run(weight_1))
    print("bias_1", session.run(bias_1))
    print("weight_2", session.run(weight_2))
    print("bias_2", session.run(bias_2))

    # 预测结果
    predict = session.run(y_head, feed_dict={x: x_data})  # 这里y_head即预测结果，注意同样是一个DAG，是x的函数，因此需要feed

    # 利用matplotlib展示执行效果
    plt.figure()
    plt.scatter(x_data, y_data)  # 训练数据用散点图展示
    plt.plot(x_data, predict, 'r-', lw=3)  # 预测值用曲线展示，r表示红色，-表示实现，lw为曲线粗细
    plt.show()

    session.close()  # 在Pycharm中如果close session则会一直处于run的状态

print("The end of a test file.")
