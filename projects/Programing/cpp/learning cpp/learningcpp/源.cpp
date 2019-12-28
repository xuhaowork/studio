#include "pch.h"
#include <iostream>

int a = 10;

void increase(int n) {
	int b = n; // n是作为函数的参数传进来的, 我们称n为参变量, b和n不是同一个地址, 只是值相同, 称为传值调用.
	a += b; // a为全局变量, b为局部变量，生命周期短.
}

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