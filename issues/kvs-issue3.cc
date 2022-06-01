//
// Created by 14037 on 2022/6/1.
//

#include <iostream>
#include "util/coding.h"
#include "leveldb/slice.h"
using namespace leveldb;

void Test(int& a){
    a = 2;
}

int main(){
    int x = 0;
    Test(x);
    std::cout << x << std::endl;
    return 0;
}
