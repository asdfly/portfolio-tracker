"""从历史智能报告(smart_report_*.md)解析建议并回填advice_history"""
import re, os, sqlite3, glob
from datetime import datetime

DB = r'C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\data\database\portfolio.db'
REPORT_DIR = r'C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\data\reports'

# 解析单个报告文件
def parse_report(filepath):
    """从报告markdown中提取建议列表"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 提取生成时间
    time_match = re.search(r'\*\*生成时间\*\*: (\d{4}-\d{2}-\d{2} \d{2}:\d{2})', content)
    if not time_match:
        # 从文件名提取日期
        fname = os.path.basename(filepath)
        date_match = re.search(r'(\d{8})', fname)
        if date_match:
            created_at = date_match.group(1)[:4] + '-' + date_match.group(1)[4:6] + '-' + date_match.group(1)[6:8] + ' 15:00:00'
        else:
            return None, []
    else:
        created_at = time_match.group(1) + ':00'
    
    # 提取报告周期
    period_match = re.search(r'\*\*报告周期\*\*: (\d{4}-\d{2}-\d{2})', content)
    report_date = period_match.group(1) if period_match else created_at[:10]
    
    # 正则匹配每条建议
    # 格式: ### N. [优先级] 标题\n**类型**: xxx | **优先级**: xxx | **置信度**: xx%\n\n描述
    advice_pattern = re.compile(
        r'### \d+\.\s+\[(.+?)\]\s+(.+?)\n'  # ### 1. [高] 标题
        r'\*\*类型\*\*:\s*(.+?)\s*\|\s*\*\*优先级\*\*:\s*(.+?)\s*\|\s*\*\*置信度\*\*:\s*(\d+)%\s*\n'  # **类型**: xxx | **优先级**: xxx | **置信度**: xx%
        r'(?:\n(.*?))?'  # 描述（可选）
        r'(?:\n\*\*建议操作\*\*:)',  # 截止到建议操作
        re.DOTALL
    )
    
    # 备用正则：更宽松的匹配
    advice_pattern2 = re.compile(
        r'### \d+\.\s+\[(.+?)\]\s+(.+?)\n'
        r'\*\*类型\*\*:\s*(.+?)\s*\|\s*\*\*优先级\*\*:\s*(.+?)\s*\|\s*\*\*置信度\*\*:\s*(\d+)%',
    )
    
    advices = []
    
    # 先用主正则
    for m in advice_pattern.finditer(content):
        priority_cn = m.group(1).strip()
        title = m.group(2).strip()
        advice_type = m.group(3).strip()
        priority = m.group(4).strip()
        confidence = float(m.group(5))
        description = (m.group(6) or '').strip()
        
        # 截取描述：到 **建议操作** 之前
        if description:
            desc_parts = description.split('\n**建议操作**:')
            description = desc_parts[0].strip()
        
        # 提取相关标的
        related_match = re.search(r'\*\*相关标的\*\*:\s*(.+?)(?:\n|$)', content[content.find(title):content.find(title)+500])
        related_codes = related_match.group(1).strip() if related_match else ''
        
        # 优先级中文→英文映射
        priority_map = {'高': 'high', '中': 'medium', '低': 'low'}
        priority_en = priority_map.get(priority_cn, priority)
        
        advices.append({
            'created_at': created_at,
            'advice_type': advice_type,
            'priority': priority_en,
            'title': title,
            'description': description[:500],  # 限制长度
            'confidence': confidence,
            'related_codes': related_codes,
            'source': 'smart_report',
            'status': 'pending',
            'report_date': report_date,
        })
    
    # 如果主正则没匹配到，用备用正则
    if not advices:
        for m in advice_pattern2.finditer(content):
            priority_cn = m.group(1).strip()
            title = m.group(2).strip()
            advice_type = m.group(3).strip()
            priority = m.group(4).strip()
            confidence = float(m.group(5))
            
            priority_map = {'高': 'high', '中': 'medium', '低': 'low'}
            priority_en = priority_map.get(priority_cn, priority)
            
            advices.append({
                'created_at': created_at,
                'advice_type': advice_type,
                'priority': priority_en,
                'title': title,
                'description': '',
                'confidence': confidence,
                'related_codes': '',
                'source': 'smart_report',
                'status': 'pending',
                'report_date': report_date,
            })
    
    return created_at, advices


# 主流程
conn = sqlite3.connect(DB)

# 确保status列存在
cols = [r[1] for r in conn.execute('PRAGMA table_info(advice_history)').fetchall()]
if 'status' not in cols:
    conn.execute('ALTER TABLE advice_history ADD COLUMN status TEXT DEFAULT "pending"')
    conn.commit()

# 获取已有记录的日期，避免重复
existing = set()
for row in conn.execute('SELECT DISTINCT created_at FROM advice_history').fetchall():
    existing.add(row[0][:10])  # 只取日期部分

# 遍历所有报告文件
report_files = sorted(glob.glob(os.path.join(REPORT_DIR, 'smart_report_*.md')))
print(f'找到 {len(report_files)} 份报告')
print(f'已有记录日期: {sorted(existing)}')

total_inserted = 0
skipped = 0
reports_processed = 0

for rf in report_files:
    created_at, advices = parse_report(rf)
    if not advices:
        print(f'  {os.path.basename(rf)}: 无建议或解析失败')
        continue
    
    report_date = created_at[:10] if created_at else 'unknown'
    
    # 跳过已有日期的记录（但保留6/2-6/3的原始记录）
    if report_date in existing:
        skipped += 1
        continue
    
    reports_processed += 1
    count = 0
    for a in advices:
        try:
            conn.execute(
                'INSERT INTO advice_history (created_at, advice_type, priority, title, description, confidence, related_codes, source, status) VALUES (?,?,?,?,?,?,?,?,?)',
                (a['created_at'], a['advice_type'], a['priority'], a['title'],
                 a['description'], a['confidence'], a['related_codes'],
                 a['source'], a['status'])
            )
            count += 1
            total_inserted += 1
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            pass
    
    print(f'  {os.path.basename(rf)}: {count} 条 (日期={report_date})')

conn.commit()

# 汇总
result = conn.execute('SELECT COUNT(*) as n, MIN(created_at) as mn, MAX(created_at) as mx FROM advice_history').fetchone()
print(f'\n=== 回填完成 ===')
print(f'  本次处理: {reports_processed} 份报告')
print(f'  跳过(已有): {skipped} 份')
print(f'  新增记录: {total_inserted} 条')
print(f'  表总计: {result[0]} 条')
print(f'  日期范围: {result[1]} ~ {result[2]}')

# 按日期统计
print(f'\n=== 按日期统计 ===')
daily = conn.execute('''
    SELECT substr(created_at, 1, 10) as d, COUNT(*) as n, 
           GROUP_CONCAT(DISTINCT priority) as pri
    FROM advice_history GROUP BY d ORDER BY d
''').fetchall()
for d, n, pri in daily:
    print(f'  {d}: {n} 条 ({pri})')

conn.close()
