# -pipeline
中文数据来源（resource of Chinese data）
# 1. 医疗 - HuggingFace Mirror "https://huggingface.co/datasets/Aunderline/CMeEE-V2/resolve/main/CMeEE-V2_train.js# 
# 2. 新闻 - OYE93 Chinese-NLP-Corpus  "https://raw.githubusercontent.com/OYE93/Chinese-NLP-Corpus/master/NER/MSRA/msra_train_bio.txt"
# 3. 简历 - https://github.com/jiesutd/LatticeLSTM/tree/master/ResumeNER/train.char.bmes
# 4. 生活 - https://github.com/hltcoe/golden-horse/tree/master/data/weiboNER_2nd_conll.train

英文数据来源（resource of English data）
# 1. Enron Email Dataset - https://www.cs.cmu.edu/~enron/(CMU)
# 2. Heart Disease Dataset - https://archive.ics.uci.edu/dataset/45/heart-disease（UCI)

# Semantic-Preserving Edge Data Sanitization System
# 具有语义保真的边端设备文件脱敏与返脱敏系统


项目简介 (Introduction)
本项目是一个专为边端设备（如 NAS、IoT 网关）设计的**本地化、可逆且语义保真**的数据脱敏系统。
针对传统脱敏方法（如统一打星号、假名化）导致的下游 NLP 任务（如情感分析、关系抽取）失效问题，本项目提出了一种基于**局部可预测性（Predictability, P值）**与**结构稳定性（Stability, S值）**的四象限动态分级脱敏模型。

通过融合轻量级 NER 模型与本地 SQLite 强加密映射表，系统实现了从原始文件输入到合规输出的闭环，满足《个人信息保护法》(PIPL) 与 GDPR 的双重要求。

核心创新点 (Key Features)
* **动态多粒度脱敏 (L1-L5 Levels)**：并非“一刀切”脱敏，而是根据语料的 P/S 值，动态映射至局部遮蔽（L2）、结构泛化（L3）、拟真类别替换（L4）或完全抽象（L5）。
* **上下文感知消歧 (Context-aware Role Disambiguation)**：独创 `ContextAnalyzer` 模块，根据实体上下文（如“医生”、“患者”）分配 Role 标签，解决同名实体的语义冲突问题。
* **微型指纹可逆机制 (Micro-Fingerprinting)**：在最高级别脱敏（L5）中引入微型哈希后缀，打破多对一映射的“信息熵黑洞”，实现 100% 精确的无损返脱敏。
* **多模态文件支持 (Multimodal Processing)**：原生支持对 `.docx`, `.xlsx`, 及 `.pdf` (重构为 Word) 的批量处理。

项目结构 (Repository Structure)
* `main_app_universal.py`: 核心脱敏引擎，包含 P/S 评估、决策路由及多模态文档解析。
* `restore_universal.py`: 逆向返脱敏引擎，基于本地 AES 加密数据库的快速正则还原。
* `gui.py`: (如果已包含) 提供 Tkinter 编写的用户交互界面。
* **数据预处理模块**:
  * `convert.py`: 处理 Adult 与 Heart Disease 等结构化数据集，转换为统一测试格式。
  * `deal.py`: Enron Email 英文语料清洗与批量切分脚本。
  * `download_cn.py`: 中文 CMeEE (医疗)、MSRA (新闻) 等 BIO/BMES 格式数据集的清洗与 Word 构建脚本。

快速开始 (Quick Start)

### 1. 环境依赖
```bash
pip install torch transformers pandas openpyxl python-docx PyMuPDF pycryptodome nltk
