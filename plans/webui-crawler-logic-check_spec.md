# WebUI Crawler Logic Optimization Specification

## 1. 目标
优化 WebUI 中爬虫及其他后台任务的管理逻辑，提升稳定性、灵活性和交互体验。

## 2. 详细变更设计

### 2.1 递归清理进程 (webui/components/task_runner.py)
修改 `stop_task` 函数：
```python
def stop_task(task_name: TaskName) -> bool:
    manager = PIDManager(task_name)
    proc = manager.get_process()
    if proc is None:
        manager.delete()
        return False

    try:
        # 获取所有子进程
        children = proc.children(recursive=True)
        for child in children:
            child.terminate()
        proc.terminate()

        # 等待所有进程结束
        gone, alive = psutil.wait_procs(children + [proc], timeout=5)
        for p in alive:
            p.kill()

        st.success(f"任务 '{TASK_LABELS[task_name]}' 及其子进程已停止。")
    except psutil.Error as e:
        st.error(f"停止任务时出错: {e}")
        return False
    finally:
        manager.delete()
    return True
```

### 2.2 动态参数与日志刷新 (webui/pages/3_任务管理.py)
- **UI 增强**:
    - 在 `crawler` 任务卡片中增加 `target_years` 选框。
    - 状态展示使用 `st.status` (Streamlit 1.25+) 或 `st.container`。
- **参数传递**:
    - 动态构建 `cmd_args`。
- **日志渲染**:
    - 使用 `st.empty()` 容器。

### 2.3 爬虫脚本兼容性 (1.report_link_crawler.py)
- 确认 `1.report_link_crawler.py` 已支持 `--year` 多值参数。当前代码行 447: `nargs="+"` 已支持此功能。

## 3. 验收标准
1. **进程树清理**: 启动多进程任务（如下载转换），停止后 `ps aux | grep python` 不应有残留。
2. **多年份支持**: WebUI 选择 2022, 2023 启动爬虫，日志应包含这两个年份的处理记录。
3. **UI 稳定性**: 日志刷新期间，用户操作其他 UI 组件不受干扰。
