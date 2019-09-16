// cpp.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include "pch.h"
#include <iostream>


int main()
{
	int a = 0x1654F;
	// 赋值操作时深拷贝
	int b = a;
	a = a + b;
	int arr[] = { 1, 2, 3, 5 };
	char crr[5][3] = {
		{91, 92, 93},
	{0},
	{31,32, 33},
	{11, 12, 13},
	{2, 3, 5}
	};

	int uu = 1;
	switch (uu) {
	case 1:
		printf("one \n");
		break; // break需要添加，switch相当于自动帮你进行跳转（有点类似于goto）需要你自动break
	case 2:
		printf("two \n");
	case 3:
		printf("three \n");
	default: // default表示没有符合上述的switch情况时进行的操作, 也可以不带
		printf("default");
	}
	// 注意： 
	// 1)switch必须是整数而不能是浮点数
	// 2)必须是常量
	// 3)switch能被if..else..完全覆盖


	//char c = 'a';
	//char cuu[] = "abc";
	
	//printf("this is a test file %s \n", cuu);

	// 局部变量声明
	//int a = 10;
	//goto inhalve;

 /*inhalve:
	a = 9;
	goto add;
newt:
	a = 11;
	if(a >0)
		add: 
			a = 12;
			return a;
	goto inhalve;

	printf("a is");
	cout << "a 的值：" << a << endl;*/
}

// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单

// 入门提示: 
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5. 转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件
