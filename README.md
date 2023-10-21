## 项目: d-project

### 描述

项目命令管理工具

### 特点

- 通过配置文件管理项目命令
- 导出readme文件

### 安装

```
pip install d-project
```

### 使用

- 首先初始化project.yml文件

```
# 默认生成当前目录
project init
# 若要修改目录
project init ./configs/project.yml
```

- 运行自定义的命令或者流程

```
project run some_command

project run some_workflow
```

- 参照本项目[project.yml](./project.yml)中的修改命令命令或者流程


- 生成READMD.md文件

```
project document --output ./README.md
```
