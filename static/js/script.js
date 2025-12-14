// 全局JavaScript功能

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 为所有带确认操作的按钮添加确认对话框
    const confirmButtons = document.querySelectorAll('[data-confirm]');
    confirmButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            const message = this.getAttribute('data-confirm') || '确定要执行此操作吗？';
            if (!confirm(message)) {
                e.preventDefault();
            }
        });
    });
});

// 刷新任务状态
function refreshTaskStatus(taskId) {
    fetch(`/api/task/${taskId}`)
        .then(response => response.json())
        .then(data => {
            const statusElement = document.getElementById('task-status');
            if (statusElement) {
                statusElement.textContent = getStatusText(data.status);
                statusElement.className = 'badge ' + getStatusClass(data.status);
            }
            
            // 如果任务仍在处理中，5秒后再次刷新
            if (data.status === 'processing') {
                setTimeout(() => refreshTaskStatus(taskId), 5000);
            }
        })
        .catch(error => {
            console.error('获取任务状态失败:', error);
        });
}

// 获取状态文本
function getStatusText(status) {
    const statusMap = {
        'uploaded': '已上传',
        'processing': '处理中',
        'completed': '已完成',
        'failed': '失败'
    };
    return statusMap[status] || status;
}

// 获取状态CSS类
function getStatusClass(status) {
    const classMap = {
        'uploaded': 'bg-secondary',
        'processing': 'bg-warning',
        'completed': 'bg-success',
        'failed': 'bg-danger'
    };
    return classMap[status] || 'bg-secondary';
}