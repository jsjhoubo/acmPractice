# Python App

A minimal Python starter project.

## Run

```bash
python python/src/main.py
```

## Test

```bash
python -m pytest python/tests
```

## C++ Practice (LeetCode/ACM)

Use this workspace for C++ problem solving with the active file tasks in VS Code.

### 1) Install a C++ compiler

This workspace currently does not have `g++` on PATH. Install one of these options:

- MinGW-w64 (recommended for simple local compile/run tasks)
- LLVM/Clang
- Visual Studio Build Tools (MSVC)

After installing, make sure the compiler command is available in a new terminal.

### 2) Write code

Create problem files under `../cpp/problems/`, for example `../cpp/problems/two_sum.cpp`.

### 3) Build and run in VS Code

- Run task: `C++: Build Active File`
- Run task: `C++: Run Active File`

The build task compiles the currently opened C++ file into an `.exe` in the same folder.
