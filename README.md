# Project LSM-KV: 键值分离存储系统
> creeper12356
### 简介
本项目为课程SE2322高级数据结构的Project，使用C++实现一个键值分离存储系统，最终分数100/100。
### 编译和运行
注：本项目必须在**Linux**操作系统上编译和运行。
#### 编译
本项目使用日志输出以便于快速Debug，可以在编译时指定是否开启日志。

关闭日志编译：
```sh
make
```
开启日志编译：
```sh
make ENABLE_LOG=1
```
#### 运行
编译成功后，可以运行各项测试。
运行正确性测试：
```sh
./correctness -v
```
运行持久性测试：
```sh
./persistence -v
```
运行性能测试：
```
./performance
```

Enjoy coding! 😀