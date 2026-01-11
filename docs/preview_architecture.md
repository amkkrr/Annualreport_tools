# 系统架构图预览 (System Architecture Preview)

您可以直接在 VS Code 的 Markdown 预览中查看下方的架构图。
如果下方显示空白或加载失败，请尝试直接在浏览器中打开 `docs/system_architecture_flow.html` 文件。

<iframe src="./system_architecture_flow.html" width="100%" height="800px" frameborder="0"></iframe>

## Remote SSH 预览指南 (Remote SSH Preview Guide)

由于您使用的是 Remote SSH，如果上面的预览无法显示，请尝试以下方法：

1.  **启动临时服务器**:
    在 VS Code 的终端中运行以下命令（在当前 `docs` 目录下）：
    ```bash
    cd docs && python3 -m http.server 8080
    ```

2.  **访问链接**:
    VS Code 通常会自动检测到端口转发。
    请在本地浏览器中访问: [http://localhost:8080/system_architecture_flow.html](http://localhost:8080/system_architecture_flow.html)

3.  **停止服务器**:
    查看完毕后，在终端按 `Ctrl+C` 停止服务器。
