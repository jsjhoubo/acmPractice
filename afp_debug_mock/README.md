# AFP 第二轮全真演习 — Read & Debug Existing Code

## 规则(照真实考试)
- **60 分钟硬钟**,打开 telemetry.py 那一刻起表
- 零 AI;可查 docs.python.org 与 numpy 文档;print / breakpoint()/pdb 随便用
- 任务:让 `python tests.py` 全绿。**只改 telemetry.py,不许动 tests.py**
- 需要 numpy:`pip install numpy --user`(装过就跳过)

## 工作循环(练的就是它)
1. 跑 `python tests.py`,挑**一个**失败的测试
2. 读 traceback 最后一行(错误类型+消息)→ 跳到你文件的对应行
3. 看不出来就插 `print(repr(...))` 或 `breakpoint()` 再跑这一个测试:
   `python -m unittest tests.TestTelemetry.test_xxx -v`
4. 修一个 → 重跑 → 下一个。绝不同时修两个,绝不重构还在过的代码

## 提示
- 初始状态:7 个测试失败(1 个直接报错),1 个通过
- 每个失败测试对应恰好一个 bug;docstring 就是 spec
- 记录:总用时 + 每个 bug 大致耗时(赛后复盘用)
