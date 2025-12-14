import os
import sys
import json
import yaml
import shutil
import logging
from datetime import datetime
from pathlib import Path
from functools import wraps
from threading import Thread
from logging import LoggerAdapter

from flask import Flask, request, jsonify, render_template, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename
import pandas as pd

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(__file__))

# 导入现有模块
from ollama_client import OllamaClient, create_ollama_client_from_config
from process_white_alarm import process_row, is_valid_path

def load_config():
    """加载配置文件"""
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def save_config(config):
    """保存配置文件"""
    with open('config.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)

def load_tasks():
    """加载任务数据"""
    try:
        with open('tasks.json', 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                return json.loads(content)
            else:
                return {}
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        # 如果JSON解析失败，返回空字典
        return {}

def save_tasks(tasks):
    """保存任务数据"""
    with open('tasks.json', 'w', encoding='utf-8') as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)

def cleanup_tasks_on_startup():
    """在应用启动时清理任务，只保留已完成的任务"""
    try:
        tasks = load_tasks()
        cleaned_tasks = {}
        removed_count = 0
        
        for task_id, task in tasks.items():
            # 只保留状态为"completed"的任务
            if task.get('status') == 'completed':
                cleaned_tasks[task_id] = task
            else:
                removed_count += 1
        
        # 如果有任务被移除，则保存清理后的任务列表
        if removed_count > 0:
            save_tasks(cleaned_tasks)
            print(f"清理了 {removed_count} 个未完成的任务，只保留已完成的任务")
        
        return cleaned_tasks
    except Exception as e:
        print(f"清理任务时出错: {e}")
        return load_tasks()

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

# 配置
CONFIG_FILE = 'config.yaml'

# 确保必要的目录存在
def initialize_app():
    config = load_config()
    UPLOAD_FOLDER = config.get('web', {}).get('upload_folder', 'uploads')
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    return config

app_config = initialize_app()
UPLOAD_FOLDER = app_config.get('web', {}).get('upload_folder', 'uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# 在应用启动时清理任务
tasks = cleanup_tasks_on_startup()

# 任务存储（在实际应用中应使用数据库）

# 存储任务进度信息
task_progress = {}

def update_task_progress(task_id, processed_rows, total_rows, status):
    """更新任务进度"""
    task_progress[task_id] = {
        'processed_rows': processed_rows,
        'total_rows': total_rows,
        'status': status,
        'updated_at': datetime.now().isoformat()
    }

def get_task_progress(task_id):
    """获取任务进度"""
    return task_progress.get(task_id, {
        'processed_rows': 0,
        'total_rows': 0,
        'status': 'unknown',
        'updated_at': datetime.now().isoformat()
    })

def allowed_file(filename):
    config = load_config()
    allowed_extensions = config.get('web', {}).get('allowed_extensions', ['xlsx', 'xls'])
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions


@app.route('/')
def index():
    """主页"""
    config = load_config()
    return render_template('index.html', config=config)

@app.route('/config', methods=['GET', 'POST'])
def config_page():
    """配置管理页面"""
    if request.method == 'POST':
        try:
            # 更新配置
            config = load_config()
            
            # 更新Ollama配置
            config['ollama']['url'] = request.form.get('ollama_url', config['ollama']['url'])
            config['ollama']['model_name'] = request.form.get('ollama_model_name', config['ollama']['model_name'])
            config['ollama']['timeout_seconds'] = int(request.form.get('ollama_timeout', config['ollama']['timeout_seconds']))
            config['ollama']['max_retries'] = int(request.form.get('ollama_max_retries', config['ollama']['max_retries']))
            
            # 更新Web应用配置
            if 'web' not in config:
                config['web'] = {}
            config['web']['upload_folder'] = request.form.get('upload_folder', config['web'].get('upload_folder', 'uploads'))
            allowed_extensions_str = request.form.get('allowed_extensions', '')
            if allowed_extensions_str:
                config['web']['allowed_extensions'] = [ext.strip() for ext in allowed_extensions_str.split(',')]
            
            # 更新处理配置
            config['processing']['max_workers'] = int(request.form.get('max_workers', config['processing']['max_workers']))
            max_rows = request.form.get('max_rows_to_process', '')
            config['processing']['max_rows_to_process'] = int(max_rows) if max_rows else None
            
            # 更新忽略的列配置
            ignored_columns_str = request.form.get('ignored_columns', '')
            if ignored_columns_str:
                config['processing']['ignored_columns'] = [col.strip() for col in ignored_columns_str.split(',') if col.strip()]
            else:
                config['processing']['ignored_columns'] = []
            
            # 更新日志配置
            config['logging']['level'] = request.form.get('log_level', config['logging']['level'])
            config['logging']['format'] = request.form.get('log_format', config['logging']['format'])
            
            # 更新输出配置
            config['output_dir'] = request.form.get('output_dir', config.get('output_dir', 'results'))
            
            # 更新系统提示词
            config['system_prompt'] = request.form.get('system_prompt', config.get('system_prompt', ''))
            
            save_config(config)
            flash('配置已更新', 'success')
        except Exception as e:
            flash(f'配置更新失败: {str(e)}', 'error')
    
    config = load_config()
    return render_template('config.html', config=config)


@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    """文件上传页面"""
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('没有选择文件', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('没有选择文件', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # 保存任务信息
            task_id = f"task_{int(datetime.now().timestamp())}"
            # 先加载现有的任务
            tasks = load_tasks()
            tasks[task_id] = {
                'id': task_id,
                'filename': filename,
                'filepath': filepath,
                'status': 'uploaded',
                'created_at': datetime.now().isoformat(),
                'output_dir': None
            }
            
            # 保存任务到文件
            save_tasks(tasks)
            
            flash(f'文件上传成功，任务ID: {task_id}', 'success')
            return redirect(url_for('task_detail', task_id=task_id))
        else:
            flash('不支持的文件格式，请上传Excel文件(.xlsx, .xls)', 'error')
    
    # GET 请求时，加载并显示任务列表
    tasks = load_tasks()
    return render_template('upload.html', tasks=tasks)

@app.route('/preview/<task_id>')
def preview_file(task_id):
    """预览上传的文件"""
    tasks = load_tasks()
    if task_id not in tasks:
        flash('任务不存在', 'error')
        return redirect(url_for('upload_file'))
    
    task = tasks[task_id]
    filepath = task['filepath']
    
    try:
        # 读取前10行数据
        if filepath.endswith('.xlsx'):
            df = pd.read_excel(filepath)
        else:
            df = pd.read_excel(filepath, engine='xlrd')
        
        # 只取前10行
        preview_df = df.head(10)
        columns = preview_df.columns.tolist()
        rows = preview_df.values.tolist()
        
        return render_template('preview.html', task=task, columns=columns, rows=rows)
    except Exception as e:
        flash(f'文件预览失败: {str(e)}', 'error')
        return redirect(url_for('upload_file'))

@app.route('/task/<task_id>')
def task_detail(task_id):
    """任务详情页面"""
    tasks = load_tasks()
    if task_id not in tasks:
        flash('任务不存在', 'error')
        return redirect(url_for('upload_file'))
    
    task = tasks[task_id]
    return render_template('task_detail.html', task=task)

def process_task_async(task_id, max_rows_override=None):
    """异步处理任务"""
    tasks = load_tasks()
    if task_id not in tasks:
        return
    
    task = tasks[task_id]
    try:
        # 加载配置
        config = load_config()
        
        # 使用配置文件中的输出目录，如果未设置则使用默认值
        output_base_dir = config.get('output_dir', 'results')
        # 创建基于时间戳的唯一输出目录
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(output_base_dir, f"run_{timestamp}")
        os.makedirs(output_dir, exist_ok=True)
        task['output_dir'] = output_dir
        
        # 更新任务状态
        task['status'] = 'processing'
        task['started_at'] = datetime.now().isoformat()
        update_task_progress(task_id, 0, 0, 'processing')
        
        # 保存任务状态
        save_tasks(tasks)
        
        # 复制输入文件到输出目录
        input_filepath = os.path.join(output_dir, task['filename'])
        shutil.copy2(task['filepath'], input_filepath)
        
        # 处理文件
        df = pd.read_excel(task['filepath'])
        total_rows = len(df)
        task['total_rows'] = total_rows
        update_task_progress(task_id, 0, total_rows, 'processing')
        
        # 应用参数覆盖（如果有）
        if max_rows_override:
            max_rows_override = int(max_rows_override)
            df = df.head(max_rows_override)
            task['processed_rows'] = len(df)
        else:
            # 检查配置中的默认设置
            default_max_rows = config.get('processing', {}).get('max_rows_to_process')
            if default_max_rows is not None:
                df = df.head(default_max_rows)
                task['processed_rows'] = len(df)
            else:
                task['processed_rows'] = total_rows
        
        # 初始化Ollama客户端
        ollama_client = create_ollama_client_from_config(config)
        
        # 设置日志文件
        log_dir = config.get('logging', {}).get('log_dir', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        # 使用配置文件中的日志文件模板
        log_file_template = config["logging"].get("log_file", "{log_dir}/{task_id}_{timestamp}.log")
        # 生成时间戳
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # 替换占位符生成日志文件路径
        log_filename = log_file_template.replace("{log_dir}", log_dir).replace("{timestamp}", timestamp).replace("{task_id}", task_id)
        log_filepath = os.path.join(log_dir, os.path.basename(log_filename))  # 确保文件在log_dir目录下
        
        # 配置日志格式
        LOG_LEVEL = getattr(logging, config["logging"]["level"].upper())
        LOG_FORMAT = config["logging"].get("format", "text")  # 默认为文本格式
        
        if LOG_FORMAT == "json":
            # JSON格式日志
            class JsonFormatter(logging.Formatter):
                def format(self, record):
                    log_entry = {
                        "timestamp": self.formatTime(record),
                        "level": record.levelname,
                        "message": record.getMessage()
                    }
                    if hasattr(record, 'task_id'):
                        log_entry["task_id"] = record.task_id
                    return json.dumps(log_entry, ensure_ascii=False)
            
            formatter = JsonFormatter()
        else:
            # 文本格式日志
            class TextFormatter(logging.Formatter):
                def format(self, record):
                    log_message = super().format(record)
                    if hasattr(record, 'task_id'):
                        log_message = f"[task_{record.task_id}] {log_message}"
                    return log_message
            
            # 使用与process_white_alarm.py相同的格式
            formatter = TextFormatter("%(asctime)s [%(levelname)s] %(message)s")
        
        # 为当前任务创建专用的日志记录器
        task_logger = logging.getLogger(f"task_{task_id}")
        task_logger.setLevel(LOG_LEVEL)
        
        # 移除现有的处理器
        for handler in task_logger.handlers[:]:
            task_logger.removeHandler(handler)
            handler.close()
        
        # 添加新的处理器
        # 使用 RotatingFileHandler 实现日志轮转
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_filepath, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=20,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        task_logger.addHandler(file_handler)
        task_logger.addHandler(console_handler)
        
        # 立即写入一条日志，确保文件不为空
        task_logger.info(f"开始处理任务 {task_id}")
        
        # 创建任务日志适配器工厂，用于传递给 process_white_alarm 模块
        def task_logger_factory(task_id):
            return LoggerAdapter(task_logger, {'task_id': task_id})
        
        # 将任务日志记录器传递给 process_row 函数
        import process_white_alarm
        process_white_alarm.set_task_logger_factory(task_logger_factory)
        
        # 处理过程
        valid_results = []
        invalid_records = []
        
        for idx, row in df.iterrows():
            try:
                result = process_row(row, idx)
                if result["type"] == "no_path_found":
                    invalid_records.append({
                        "序号": idx + 1,
                        "原始路径": "<原始行未提取到任何路径>",
                        "文件名": "<无文件名>",
                        "类型": "未知",
                        "应用名称": "<无>",
                        "输入内容": str(result["row"])
                    })
                elif result["type"] == "processed":
                    for output in result["outputs"]:
                        raw_path = output["原始路径"]
                        is_valid = is_valid_path(raw_path, allow_filename_only=True)
                        task_logger.debug(f"路径验证结果: {repr(raw_path)} -> {'有效' if is_valid else '无效'}")
                        if is_valid:
                            valid_results.append(output)
                            task_logger.debug(f"添加有效结果: {repr(raw_path)}")
                        else:
                            invalid_records.append(output)
                            task_logger.debug(f"添加无效记录: {repr(raw_path)}")
                
                # 更新进度
                update_task_progress(task_id, idx + 1, total_rows, 'processing')
            except Exception as e:
                task_logger.error(f"处理行 {idx} 时出错: {e}", exc_info=True)
                invalid_records.append({
                    "序号": idx + 1,
                    "原始路径": f"<处理出错: {str(e)}>",
                    "文件名": "<无文件名>",
                    "类型": "错误",
                    "应用名称": "<无>",
                    "输入内容": str(row.to_dict())
                })
        
        # 保存结果
        if invalid_records:
            invalid_df = pd.DataFrame(invalid_records)
            cols = ["序号", "原始路径", "文件名", "类型", "应用名称", "输入内容"]
            for col in cols:
                if col not in invalid_df.columns:
                    invalid_df[col] = ""
            invalid_df = invalid_df[cols]
            invalid_df.to_excel(os.path.join(output_dir, "invalid_records.xlsx"), index=False)
        
        if valid_results:
            result_df = pd.DataFrame(valid_results)
            result_df.sort_values("序号", inplace=True, ignore_index=True)
            result_df.to_excel(os.path.join(output_dir, "valid_results.xlsx"), index=False)
        
        # 更新任务状态
        task['status'] = 'completed'
        task['completed_at'] = datetime.now().isoformat()
        task['valid_count'] = len(valid_results)
        task['invalid_count'] = len(invalid_records)
        update_task_progress(task_id, total_rows, total_rows, 'completed')
        
        # 记录任务完成日志
        task_logger.info(f"任务 {task_id} 处理完成，有效结果: {len(valid_results)}, 无效记录: {len(invalid_records)}")
        
        # 添加类似于 main() 函数中的日志记录
        if invalid_records:
            task_logger.info(f"💾 已保存 {len(invalid_records)} 条无效记录到 {os.path.join(output_dir, 'invalid_records.xlsx')}")
        
        if valid_results:
            task_logger.info(f"✅ 处理完成！共生成 {len(valid_results)} 条有效路径结果，已保存到 {os.path.join(output_dir, 'valid_results.xlsx')}")
        else:
            task_logger.warning("⚠️ 未生成任何有效路径结果")
        
        task_logger.info(f"📄 详细日志请查看: {log_filepath}")
        task_logger.info(f"📁 有效结果保存在: {os.path.join(output_dir, 'valid_results.xlsx')}")
        task_logger.info(f"📁 无效记录保存在: {os.path.join(output_dir, 'invalid_records.xlsx')}")
        
        # 保存任务状态
        tasks = load_tasks()  # 重新加载任务以防在处理过程中有更新
        if task_id in tasks:
            tasks[task_id].update(task)
            save_tasks(tasks)
        
        # 关闭日志处理器
        for handler in task_logger.handlers[:]:
            handler.close()
            task_logger.removeHandler(handler)
            
    except Exception as e:
        task['status'] = 'failed'
        task['error'] = str(e)
        update_task_progress(task_id, 0, 0, 'failed')
        logging.error(f"处理任务 {task_id} 时出错: {e}", exc_info=True)
        
        # 保存任务状态
        tasks = load_tasks()  # 重新加载任务以防在处理过程中有更新
        if task_id in tasks:
            tasks[task_id].update(task)
            save_tasks(tasks)

@app.route('/process/<task_id>', methods=['POST'])
def process_task(task_id):
    """处理任务 - 立即返回，实际处理在后台进行"""
    # 加载最新的任务数据
    tasks = load_tasks()
    if task_id not in tasks:
        return jsonify({'error': '任务不存在'}), 404
    
    # 获取表单参数
    max_rows_override = request.form.get('max_rows_override')
    
    # 启动异步任务处理
    thread = Thread(target=process_task_async, args=(task_id, max_rows_override))
    thread.start()
    
    # 立即返回成功响应
    return jsonify({'status': 'success', 'message': '任务已提交，正在后台处理中'})

@app.route('/tasks')
def task_list():
    """任务列表"""
    tasks = load_tasks()
    return render_template('task_list.html', tasks=tasks)

@app.route('/task/delete/<task_id>', methods=['POST'])
def delete_task(task_id):
    """删除任务及其相关文件"""
    tasks = load_tasks()
    if task_id not in tasks:
        flash('任务不存在', 'error')
        return redirect(url_for('task_list'))
    
    task = tasks[task_id]
    
    try:
        # 删除输出目录（如果存在）
        if task.get('output_dir') and os.path.exists(task['output_dir']):
            shutil.rmtree(task['output_dir'])
        
        # 删除日志文件（如果存在）
        config = load_config()
        log_dir = config.get('logging', {}).get('log_dir', 'logs')
        
        # 查找并删除匹配的任务日志文件（支持新旧两种命名规范）
        if os.path.exists(log_dir):
            for filename in os.listdir(log_dir):
                if (filename.startswith(f"{task_id}_") or filename.startswith(f"task_{task_id}_")) and filename.endswith(".log"):
                    log_filepath = os.path.join(log_dir, filename)
                    if os.path.exists(log_filepath):
                        os.remove(log_filepath)
        
        # 从任务字典中删除任务
        del tasks[task_id]
        
        # 保存更新后的任务列表
        save_tasks(tasks)
        
        flash(f'任务 {task_id} 已成功删除', 'success')
    except Exception as e:
        logging.error(f"删除任务 {task_id} 时出错: {e}", exc_info=True)
        flash(f'删除任务失败: {str(e)}', 'error')
    
    return redirect(url_for('task_list'))

@app.route('/download/<task_id>/<file_type>')
def download_file(task_id, file_type):
    """下载结果文件"""
    tasks = load_tasks()
    if task_id not in tasks:
        flash('任务不存在', 'error')
        return redirect(url_for('task_list'))
    
    task = tasks[task_id]
    if not task.get('output_dir'):
        flash('任务尚未完成', 'error')
        return redirect(url_for('task_detail', task_id=task_id))
    
    file_map = {
        'valid': 'valid_results.xlsx',
        'invalid': 'invalid_records.xlsx'
    }
    
    if file_type not in file_map:
        flash('文件类型不支持', 'error')
        return redirect(url_for('task_detail', task_id=task_id))
    
    file_path = os.path.join(task['output_dir'], file_map[file_type])
    if not os.path.exists(file_path):
        flash('文件不存在', 'error')
        return redirect(url_for('task_detail', task_id=task_id))
    
    return send_file(file_path, as_attachment=True)

@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    """配置管理API"""
    if request.method == 'GET':
        return jsonify(load_config())
    elif request.method == 'POST':
        try:
            config = request.get_json()
            save_config(config)
            return jsonify({'status': 'success', 'message': '配置已更新'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/process', methods=['POST'])
def api_process():
    """处理任务API"""
    try:
        data = request.get_json()
        filepath = data.get('filepath')
        if not filepath or not os.path.exists(filepath):
            return jsonify({'status': 'error', 'message': '文件不存在'}), 400
        
        # 创建任务
        task_id = f"api_task_{int(datetime.now().timestamp())}"
        filename = os.path.basename(filepath)
        tasks[task_id] = {
            'id': task_id,
            'filename': filename,
            'filepath': filepath,
            'status': 'processing',
            'created_at': datetime.now().isoformat(),
            'output_dir': None
        }
        
        # 这里应该实际处理任务，但为了简化示例，我们模拟处理
        # 在实际应用中，这里应该调用实际的处理逻辑
        tasks[task_id]['status'] = 'completed'
        tasks[task_id]['completed_at'] = datetime.now().isoformat()
        tasks[task_id]['valid_count'] = 0
        tasks[task_id]['invalid_count'] = 0
        
        return jsonify({
            'status': 'success',
            'task_id': task_id,
            'message': '任务处理完成'
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/tasks')
def api_tasks():
    """任务列表API"""
    return jsonify(list(tasks.values()))

@app.route('/api/task/<task_id>')
def api_task_detail(task_id):
    """任务详情API"""
    if task_id not in tasks:
        return jsonify({'status': 'error', 'message': '任务不存在'}), 404
    return jsonify(tasks[task_id])

@app.route('/api/task/<task_id>/progress')
def api_task_progress(task_id):
    """获取任务进度API"""
    if task_id not in tasks:
        return jsonify({'status': 'error', 'message': '任务不存在'}), 404
    
    progress = get_task_progress(task_id)
    task = tasks[task_id]
    
    return jsonify({
        'status': 'success',
        'data': {
            'task_id': task_id,
            'task_status': task['status'],
            'progress': progress
        }
    })

@app.route('/api/logs/<task_id>')
def get_task_log(task_id):
    """获取任务日志"""
    tasks = load_tasks()
    if task_id not in tasks:
        return jsonify({'status': 'error', 'message': '任务不存在'}), 404
    
    task = tasks[task_id]
    
    # 安全检查：防止路径遍历攻击
    if '..' in task_id or '/' in task_id or '\\' in task_id:
        return jsonify({'status': 'error', 'message': '无效的任务ID'}), 400
    
    # 从配置中获取日志目录
    config = load_config()
    log_dir = config.get('logging', {}).get('log_dir', 'logs')
    
    # 查找匹配的任务日志文件（使用新的命名规范）
    log_filename = None
    if os.path.exists(log_dir):
        for filename in os.listdir(log_dir):
            if filename.startswith(f"{task_id}_") and filename.endswith(".log"):
                log_filename = filename
                break
    
    # 如果没找到特定格式的日志文件，尝试查找旧格式的日志文件
    if log_filename is None:
        log_filename = f"task_{task_id}.log"
    
    log_filepath = os.path.join(log_dir, log_filename)
    
    # 检查文件是否存在
    if not os.path.exists(log_filepath):
        return jsonify({'status': 'error', 'message': '日志文件不存在'}), 404
    
    try:
        # 读取日志文件内容（以只读模式打开，支持正在被写入的文件）
        with open(log_filepath, 'r', encoding='utf-8') as f:
            log_content = f.read()
        return jsonify({'status': 'success', 'data': log_content})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'读取日志文件失败: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True)