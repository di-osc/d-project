## 项目: d-project

### 描述
项目命令管理工具

### 安装
```
pip install d-project
```
### 使用

首先初始化project.yml文件

```
# 默认生成当前目录
project init
# 若要修改目录
project init ./configs/project.yml
```

运行自定义的命令或者流程
```
project run some_command

project run some_workflow
```

生成READMD.md文件

```
project document
```





### 目前
- 0.1.0: 完成读取project.yml功能,并且通过命令 --help可以友好提示



### 未来
- 添加自动生成文档功能
