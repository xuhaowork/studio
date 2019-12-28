#include "pch.h"
#include <iostream>

int a = 10;

void increase(int n) {
	int b = n; // n是作为函数的参数传进来的, 我们称n为参变量, b和n不是同一个地址, 只是值相同, 称为传值调用.
	a += b; // a为全局变量, b为局部变量，生命周期短.
}

int main()
{
	increase(1);

}