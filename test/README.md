# 测试框架

一个测试框架，提供自定义测试流程和一些预设用例。

## 本地环境配置

项目依赖于以下环境，在使用之前，需要配置好：
- JDK17
- MySQL
- S3 LocalStack

### S3 LocalStack 配置

- 前置条件
  - 安装brew
  - 安装docker，并启动docker daemon

- 如何安装
```shell
# 要求安装好homebrew
brew install localstack
```

- 如何启动LocalStack
```shell
export LOCALSTACK_API_KEY=<YOUR_API_KEY>
export ACTIVATE_PRO=0 # 不启用高级模式，高级模式需要付费
localstack start

# 以下是正常启动后的输出
     __                     _______ __             __
    / /   ____  _________ _/ / ___// /_____ ______/ /__
   / /   / __ \/ ___/ __ `/ /\__ \/ __/ __ `/ ___/ //_/
  / /___/ /_/ / /__/ /_/ / /___/ / /_/ /_/ / /__/ ,<
 /_____/\____/\___/\__,_/_//____/\__/\__,_/\___/_/|_|

 💻 LocalStack CLI 2.3.2

[10:28:16] starting LocalStack in Docker mode 🐳                                                                                                                                                                           localstack.py:495
[10:28:55] container image not found on host                                                                                                                                                                               bootstrap.py:1195
[10:29:50] download complete                                                                                                                                                                                               bootstrap.py:1199
────────────────────────────────────────────────────────────────────────────────────────────── LocalStack Runtime Log (press CTRL-C to quit) ───────────────────────────────────────────────────────────────────────────────────────────────

LocalStack version: 2.3.3.dev20231101185826
LocalStack Docker container id: 988e71ee53ab
LocalStack build date: 2023-11-01
LocalStack build git hash: fd3d1be

2023-11-02T02:29:54.938  INFO --- [-functhread6] hypercorn.error            : Running on https://0.0.0.0:4566 (CTRL + C to quit)
2023-11-02T02:29:54.938  INFO --- [-functhread6] hypercorn.error            : Running on https://0.0.0.0:4566 (CTRL + C to quit)
2023-11-02T02:29:55.002  INFO --- [  MainThread] localstack.utils.bootstrap : Execution of "start_runtime_components" took 2103.81ms
Ready.

```