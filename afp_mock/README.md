# AFP CodeSignal 全真模拟 — In-Memory Database (L1–L4)

## 规则(照真实考试执行)
- **总计时 90 分钟**,打开 LEVEL1.md 那一刻开始计时
- 只写 Python;只许查 docs.python.org;**不用 AI、不用补全**
- 所有代码写在 `solution.py` 的 `InMemoryDB` 类里
- 跑绿一级,才允许打开下一级的 LEVELn.md(自觉执行,模拟逐级解锁)

## 怎么跑测试
在本目录下:
    python tests/level1_test.py
    python tests/level2_test.py
    python tests/level3_test.py
    python tests/level4_test.py

## 建议配速(真实考生数据)
    L1 ≤ 10 分钟   L2 ≤ 15 分钟   L3 ≤ 30 分钟   L4 剩余全部

## 战术
- 题面文字多 → 跳读,直奔操作定义和测试
- brute force 无罪,跑绿就是分;不评代码质量
- 每写完一个方法立刻跑测试,别攒
- 后面级别会在前面代码上扩展 → 数据放 dict,别把逻辑写死
