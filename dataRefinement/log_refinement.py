import pandas as pd
import os
from typing import Optional
import glob
from dataRefinement.drain.drain_template_extractor import extract_templates
import re

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _filter_logs_by_timerange(start_timestamp: int, end_timestanp: int, df_log: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    从匹配的日志文件中筛选出在指定时间范围内的日志记录。

    参数:
        start_timestamp: 开始时间戳
        end_timestamp: 结束时间戳
        df_log: 包含日志数据的DataFrame

    返回:
        pd.DataFrame: 包含在指定时间范围内的日志记录的DataFrame
    """
    if df_log is None:
        print("错误：输入的日志DataFrame为空") 
        return None
    if 'timestamp_ns' not in df_log.columns:
        print("日志数据中缺少timestamp_ns列")
        return None
    filtered_df = df_log[(df_log['timestamp_ns'] >= start_timestamp) & (df_log['timestamp_ns'] <= end_timestanp)]
    return filtered_df

def _filter_logs_by_error(df: Optional[pd.DataFrame], column: str = 'message') -> Optional[pd.DataFrame]:
    """
    过滤包含'error'（不区分大小写）的日志数据
    
    参数:
        df: 输入的DataFrame
        column: 要检查的列名，默认为'message'
        
    返回:
        DataFrame: 包含error的日志数据；如果输入为None或列不存在则返回None
    """
    if df is None:
        print("输入数据为空")
        return None
    
    if column not in df.columns:
        print(f"列{column}不存在")
        return None
    
    error_logs = df[df[column].str.contains(['error', 'failed', 'exception'], case=False, na=False)]
    print(f"找到{len(error_logs)}条包含error的日志")
    return error_logs

def _filter_logs_by_columns(df: Optional[pd.DataFrame], columns: Optional[list[str]] = None) -> Optional[pd.DataFrame]:
    """
    从已过滤的日志数据中进一步筛选指定的列
    
    参数:
        filtered_df: 已经过时间范围过滤的DataFrame
        columns: 需要保留的列名列表，如果为None则返回所有列
        
    返回:
        DataFrame: 只包含指定列的数据；如果输入为None则返回None
    """
    if df is None:
        print("输入数据为空")
        return None
    
    if columns is None:
        print("未指定列，返回所有列")
        return df
    
    # 检查请求的列是否存在于DataFrame中
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        print(f"警告: 以下列不存在: {missing_cols}")
    
    # 只保留存在的列
    valid_cols = [col for col in columns if col in df.columns]
    if not valid_cols:
        print("没有有效的列名")
        return None
    # 保证返回DataFrame而不是Series
    return df.loc[:, valid_cols]

def _extract_log_templates(df: Optional[pd.DataFrame], column: str = 'message') -> Optional[pd.DataFrame]:
    """
    从日志数据中提取模板，添加新列模板ID和模板内容列
    
    参数:
        df: 输入的DataFrame
        column: 包含日志消息的列名，默认为'message'
        
    返回:
        DataFrame: 添加了template_id和template列的DataFrame
    """
    if df is None:
        print("输入数据为空")
        return None
    
    if column not in df.columns:
        print(f"列{column}不存在")
        return None
    
    try:
        print("初始化Drain模型...")
        miner = extract_templates(
            log_list = df[column].values.tolist(),
        )
        print("成功加载Drain模型")
        template = []
        for log in df[column]:
            log_txt = log.rstrip()
            result = miner.match(log_txt)
            if result is None:
                template.append(None)
            else:
                template.append(result.get_template())
        df['template'] = template
        print(f"成功为{len(template)}条日志提取模板")
        return df
    except Exception as e:
        print(f"提取模板时出错: {e}")
        return None

def _deduplicate_pod_template_combination(df: pd.DataFrame, pod_column: str = 'k8_pod', node_column: str = 'k8_node_name',template_column: str = 'template') -> pd.DataFrame:
    """
    去重日志数据，保留每个pod和template组合的第一条日志，并添加计数列occurrence_count
    参数:
        df: 包含pod和模板列的DataFrame
        pod_col: pod列的名称，默认为'k8_pod'
        node_col: node列的名称，默认为'k8_node_name'
        template_col: 模板列的名称，默认为'template'

    返回:
        DataFrame: 去重后的DataFrame，只保留每种pod和模板组合的第一次出现，并添加occurrence_count列
    """
    if df is None or len(df) == 0:
        print("输入数据为空")
        return None
    if pod_column not in df.columns:
        print(f"列{pod_column}不存在")
        return None
    if node_column not in df.columns:
        print(f"列{node_column}不存在")
        return None
    if template_column not in df.columns:
        print(f"列{template_column}不存在")
        return None
    
    try:
        original_count = len(df)
        df_dedup = df.groupby([pod_column, node_column, template_column]).first().reset_index()
        df_dedup['occurrence_count'] = df.groupby([pod_column, node_column, template_column]).size().values
        dedup_count = len(df_dedup)
        print(f"去重前数据量: {original_count}, 去重后数据量: {dedup_count}")
        print(f"减少了{original_count - dedup_count}条日志")
        return df_dedup
    except Exception as e:
        print(f"去重日志时出错: {e}")
        return None

def _extract_service_name(pod_name: str) -> str:
    """
    从pod名称中提取服务名称，（如frontend-1 -> frontend）
    
    参数:
        pod_name: 完整的pod名称
        
    返回:
        str: 提取出的服务名称
    """
    if pod_name is None:
        print("输入的pod名称为空")
        return None
    match = re.match(r'([a-zA-Z0-9]+)', pod_name)
    if not match:
        print(f"警告: pod名称 {pod_name} 格式不符合预期")
        return pod_name
    
    service_name = match.group(1)
    return service_name

def log_refinement(start_time_hour: str, start_timestamp: int, end_timestamp: int) -> Optional[pd.DataFrame]:
    """
    加载并过滤日志数据，返回过滤后的DataFrame
    
    参数:
        start_time_hour: 日志文件名中的时间部分
        start_timestamp: 故障开始时间戳（纳秒级）
        end_timestamp: 故障结束时间戳（纳秒级）
        
    返回:
        DataFrame: 过滤后的日志DataFrame；如果没有匹配文件或处理过程中出错则返回None
    """
    matched_files = glob.glob(os.path.join(project_root, 'data', '*', 'log-parquet', f'*{start_time_hour}*'))
    if not matched_files:
        print(f"未找到匹配的日志文件: {start_time_hour}")
        return None
    
    df_log = pd.read_parquet(matched_files[0])
    print("原始日志文件的数据量：", len(df_log))

    df_filtered_logs = _filter_logs_by_timerange(start_timestamp, end_timestamp, df_log)
    if df_filtered_logs is None:
        print("时间过滤后的日志文件为空")
        return None
    print("时间过滤后的日志文件的数据量：", len(df_filtered_logs))

    df_filtered_logs = _filter_logs_by_error(df_filtered_logs, column='message')
    if df_filtered_logs is None:
        print("错误过滤后的日志文件为空")
        return None
    print("错误过滤后的日志文件的数据量：", len(df_filtered_logs))

    df_filtered_logs = _filter_logs_by_columns(df_filtered_logs, columns=['time_beijing', 'k8_pod', 'message', 'k8_node_name'])
    if df_filtered_logs is None:
        print("列过滤后日志文件为空")
        return None
    print("列过滤后日志文件的数据量：", len(df_filtered_logs))

    df_filtered_logs = _extract_log_templates(df_filtered_logs, column = 'message') 
    if df_filtered_logs is None:
        print("模板提取后日志文件为空")
        return None
    print("模板提取后日志文件的数据量：", len(df_filtered_logs))

    df_filtered_logs = _deduplicate_pod_template_combination(df_filtered_logs)
    if df_filtered_logs is None:
        print("去重后日志文件为空")
        return None
    print("去重后日志文件的数据量：", len(df_filtered_logs))

    # 提取service_name列
    df_filtered_logs['service_name'] = df_filtered_logs['k8_pod'].apply(_extract_service_name)
    # pod_name和node_name重命名
    df_filtered_logs.rename(columns={'k8_pod': 'pod_name', 'k8_node_name': 'node_name'}, inplace=True)
    # 重新排序列，保留node_name, service_name, pod_name, message, occurrence_count列
    df_filtered_logs = df_filtered_logs[['node_name', 'service_name', 'pod_name', 'message', 'occurrence_count']]
    # 按出现次数降序排序，使高频错误排在前面
    df_filtered_logs = df_filtered_logs.sort_values(by='occurrence_count', ascending=False)

    return df_filtered_logs.to_csv(index=False)