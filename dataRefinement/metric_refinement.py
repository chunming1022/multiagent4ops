import pandas as pd
import os
from typing import Optional, List, Tuple, Dict
import json
from datetime import datetime
import sys
import asyncio
import ast

from scipy.integrate._ivp.dop853_coefficients import D

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from agent.agent import metrics_agent

# 定义要分析的关键指标列 
key_metrics = ['client_error_ratio', 'error_ratio', 'request', 'response', 'rrt', 'server_error_ratio', 'timeout']

# 定义要分析的节点指标
node_metrics = ['node_cpu_usage_rate',
                'node_disk_read_bytes_total',
                'node_disk_read_time_seconds_total',
                'node_disk_write_time_seconds_total',
                'node_disk_written_bytes_total',
                'node_filesystem_free_bytes',
                'node_filesystem_usage_rate',
                'node_memory_MemAvailable_bytes',
                'node_memory_MemTotal_bytes',
                'node_memory_usage_rate',
                'node_network_receive_bytes_total',
                'node_network_receive_packets_total',
                'node_network_transmit_bytes_total',
                'node_network_transmit_packets_total',
                'node_sockstat_TCP_inuse']

pod_metrics = [
    'pod_cpu_usage', 'pod_fs_reads_bytes', 'pod_fs_writes_bytes',
    'pod_memory_working_set_bytes', 'pod_network_receive_bytes',
    'pod_network_receive_packets', 'pod_network_transmit_bytes',
    'pod_network_transmit_packets', 'pod_processes'
]

def get_tidb_core_metrics() -> Dict[str, List[str]]:
    """
    获取TiDB服务的核心指标列表（基于您的筛选建议）

    返回:
        服务名到核心指标列表的映射字典
    """
    return {
        'tidb-tidb': [
            'failed_query_ops',  # 失败请求数 - 错误率指标
            'duration_99th',  # 99分位请求延迟 - 关键性能指标
            'connection_count',  # 连接数 - 负载指标
            'server_is_up',  # 服务存活节点数 - 可用性指标
            'cpu_usage',  # CPU使用率 - 资源饱和度
            'memory_usage'  # 内存使用量 - 资源使用
        ],
        'tidb-pd': [
            'store_up_count',  # 健康Store数量 - 集群健康度
            'store_down_count',  # Down Store数量 - 故障指标
            'store_unhealth_count',  # Unhealth Store数量 - 异常指标
            'storage_used_ratio',  # 已用容量比 - 容量指标
            'cpu_usage',  # CPU使用率 - 资源使用
            'memory_usage'  # 内存使用量 - 资源使用
        ],
        'tidb-tikv': [
            'cpu_usage',  # CPU使用率 - 资源使用
            'memory_usage',  # 内存使用量 - 资源使用
            'server_is_up',  # 服务存活节点数 - 可用性
            'available_size',  # 可用存储容量 - 容量预警
            'raft_propose_wait',  # RaftPropose等待延迟P99 - 性能指标
            'raft_apply_wait',  # RaftApply等待延迟P99 - 性能指标
            'rocksdb_write_stall'  # RocksDB写阻塞次数 - 关键异常指标
        ]
    }

def get_service_files(date: str) -> List[str]:
    """
    获取当前索引对应的SERVICE文件列表
    参数：
    - date: 故障日期
    返回：
    - service_files: 包含SERVICE文件的列表
    """

    # 构建service数据目录路径
    service_dir = os.path.join(project_root, 'data', f'{date}', 'metric-parquet', 'apm', 'service')
    service_files = os.listdir(service_dir)

    return service_files

def get_normal_periods(df_fault_timestamps: pd.DataFrame, current_index: int) -> List[Tuple[str, str]]:
    """
    获取正常时间段（故障前后的正常时间段）
    参数：
    - df_fault_timestamps: 故障时间戳DataFrame
    - index: 当前故障索引
    返回：
    - normal_periods: 包含正常时间段的列表，每个元素为(start_time, end_time)
    """
    current_row = df_fault_timestamps.iloc[current_index]
    current_start = current_row['start_timestamp']
    current_end = current_row['end_timestamp']
    
    normal_periods  = []
    # 获取当前故障前的正常时间段（上一个故障结束后10分钟到当前故障开始）
    if current_index > 0:
        prev_row = df_fault_timestamps.iloc[current_index - 1]
        prev_end = prev_row['end_timestamp']
        normal_periods.append((prev_end + 10 * 60 * 1_000_000_000, current_start))

    # 获取当前故障后的正常时间段（当前故障结束后10分钟到下一个故障开始）
    if current_index < len(df_fault_timestamps) - 1:
        next_row = df_fault_timestamps.iloc[current_index + 1]
        next_start = next_row['start_timestamp']
        normal_periods.append((current_end + 10 * 60 * 1_000_000_000, next_start))

    return normal_periods

def get_metrics_stats(df: pd.DataFrame, metrics: List[str]) -> Dict[str, Dict]:
    """
    计算DataFrame中指标的统计信息
    参数：
    - df: 包含指标数据的DataFrame
    - metrics: 要分析的指标列表
    返回：
    - stats: 包含指标统计信息的字典
    """
    stats = {}
    for metric in metrics:
        if metric in df.columns:
            col_data = df[metric].dropna().sort_values()
            if len(col_data) <= 2:
                desc = col_data.describe(percentiles=[0.25, 0.5, 0.75, 0.95, 0.99])
            else:
                #去掉最小值和最大值
                trimmed_data = col_data.iloc[1:-1]
                desc = trimmed_data.describe(percentiles=[0.25, 0.5, 0.75, 0.95, 0.99])

            #计算非零比例
            non_zero_ratio = (col_data > 0).mean()
            desc['non_zero_ratio'] = round(non_zero_ratio, 3)
            stats[metric] = desc.to_dict()

    return stats

async def get_abnormal_metrics(normal_stats: Dict[str, Dict], fault_stats: Dict[str, Dict]) -> List[str]:
    """
    调用metrics_agent对比正常时间段和故障时间段的指标差异，返回关键异常指标
    参数：
    - normal_stats: 正常时间段指标统计信息
    - fault_stats: 故障时间段指标统计信息
    返回：
    - abnormal_metrics: 包含异常指标的列表
    """
    refined_metrics = await metrics_agent.run(task=f"请对比正常时间段和故障时间段的指标差异，返回需要注意的异常指标列表(格式为['指标1','指标2'])，不要包含其它任何解释和文本。正常时间段指标统计信息：{normal_stats}，故障时间段指标统计信息：{fault_stats}")
    refined_metrics = refined_metrics.messages[-1].content
    abnormal_metrics = ast.literal_eval(refined_metrics.strip())
    return abnormal_metrics


async def analyze_service_metrics(fault_date: str, normal_periods: List[Tuple[str, str]], fault_period: Tuple[str, str]) -> Dict:
    """
    分析SERVICE文件中的指标数据，计算正常时间段和故障时间段的指标差异
    参数：
    - service_paths: 包含SERVICE文件路径的列表
    - normal_periods: 正常时间段列表，每个元素为(start_time, end_time)
    - fault_period: 故障时间段，为(start_time, end_time)
    返回：
    - service_results: 包含SERVICE级别分析结果的字典
    """
    service_files = get_service_files(fault_date)
    service_paths = [os.path.join(project_root, 'data', f'{fault_date}', 'metric-parquet', 'apm', 'service', service_file) for service_file in service_files]
    service_analysis = {}

    for service_path in service_paths:
        service_name = os.path.basename(service_path).split('_')[1] if '_' in os.path.basename(service_path) else os.path.basename(service_path).split('.')[0]
        df_service = pd.read_parquet(service_path)

        if len(df_service) == 0:
            print(f"服务 {service_name} 没有数据")
            continue

        all_normal_data = []
        
        for start, end in normal_periods:
            normal_data = df_service[(df_service['timestamp_ns'] >= int(start)) & (df_service['timestamp_ns'] <= int(end))]
            if len(normal_data) == 0:
                print(f"服务 {service_name} 在正常时间段 {start} 到 {end} 没有数据")
                continue
            all_normal_data.append(normal_data)
        #正常时间段指标统计
        if all_normal_data:
            combined_normal_data = pd.concat(all_normal_data, ignore_index=True)
            print(f"合并正常时间段总数据行数：{len(combined_normal_data)}")

            # 计算正常时间段指标统计
            normal_stats = get_metrics_stats(combined_normal_data, key_metrics)
            # print(json.dumps(normal_stats, ensure_ascii=False, indent=4))
            # exit()

        #故障时间段指标统计
        fault_data = df_service[(df_service['timestamp_ns'] >= int(fault_period[0])) & (df_service['timestamp_ns'] <= int(fault_period[1]))]
        if len(fault_data):
            # 计算故障时间段指标统计
            print(f"故障时间段数据行数：{len(fault_data)}")
            fault_stats = get_metrics_stats(fault_data, key_metrics)
            # print(json.dumps(fault_stats, ensure_ascii=False, indent=4))
            # exit()

        # 调用metrics_agent对比正常时间段和故障时间段的指标差异，返回关键异常指标
        abnormal_metrics = await get_abnormal_metrics(normal_stats, fault_stats)
        print(f"异常指标列表：{abnormal_metrics}")
        if len(abnormal_metrics):
            #下钻pod分析
            pod_paths = os.path.join(os.path.dirname(os.path.dirname(service_path)), 'pod')
            pod_files = os.listdir(pod_paths)
            for pod_file in pod_files:
                #获取pod名
                pod_name = pod_file.split('_')[1] if '_' in pod_file else pod_file.split('.')[0]
                #找到service对应pod文件
                if pod_name.startswith(service_name):
                    pod_path = os.path.join(pod_paths, pod_file)
                    df_pod = pd.read_parquet(pod_path)

                    if len(df_pod) == 0:
                        print(f"服务 {service_name} 在故障时间段 {fault_period[0]} 到 {fault_period[1]} 没有数据")
                        continue

                    all_normal_data = []
                    for start, end in normal_periods:
                        normal_data = df_pod[(df_pod['timestamp_ns'] >= int(start)) & (df_pod['timestamp_ns'] <= int(end))]
                        if len(normal_data) == 0:
                            print(f"服务 {service_name} 在正常时间段 {start} 到 {end} 没有数据")
                            continue
                        all_normal_data.append(normal_data)

                    #正常时间段指标统计
                    if all_normal_data:
                        combined_normal_data = pd.concat(all_normal_data, ignore_index=True)
                        print(f"合并正常时间段总数据行数：{len(combined_normal_data)}")
                        # 计算正常时间段指标统计
                        normal_stats = get_metrics_stats(combined_normal_data, abnormal_metrics)
                        # print(json.dumps(normal_stats, ensure_ascii=False, indent=4))
                        # exit()

                    #故障时间段指标统计
                    fault_data = df_pod[(df_pod['timestamp_ns'] >= int(fault_period[0])) & (df_pod['timestamp_ns'] <= int(fault_period[1]))]
                    if len(fault_data):
                        # 计算故障时间段指标统计
                        print(f"故障时间段数据行数：{len(fault_data)}")
                        fault_stats = get_metrics_stats(fault_data, abnormal_metrics)

                    service_analysis[service_name] = {}
                    service_analysis[service_name][pod_name] = {
                        'normal_stats': normal_stats,
                        'fault_stats': fault_stats,
                    }
    return service_analysis

def get_tidb_services_directories() -> Dict[str, str]:
    """
    获取TiDB服务的数据目录映射

    返回:
        服务名到目录路径的映射字典
    """
    return {
        'tidb-tidb': 'infra/infra_tidb',
        'tidb-pd': 'other',
        'tidb-tikv': 'other'
    }

def get_tidb_services_files_mapping(date: str) -> Dict[str, Dict[str, str]]:
    """
    获取TiDB服务的文件名映射，返回服务名到指标文件的映射关系

    参数:
        date: 日期，格式如 "2025-06-06"

    返回:
        服务名到指标文件映射的字典 {service_name: {metric_name: file_name}}
    """
    return {
        'tidb-tidb': {
            'failed_query_ops': f'infra_tidb_failed_query_ops_{date}.parquet',
            'duration_99th': f'infra_tidb_duration_99th_{date}.parquet',
            'connection_count': f'infra_tidb_connection_count_{date}.parquet',
            'server_is_up': f'infra_tidb_server_is_up_{date}.parquet',
            'cpu_usage': f'infra_tidb_cpu_usage_{date}.parquet',
            'memory_usage': f'infra_tidb_memory_usage_{date}.parquet'
        },
        'tidb-pd': {
            'store_up_count': f'infra_pd_store_up_count_{date}.parquet',
            'store_down_count': f'infra_pd_store_down_count_{date}.parquet',
            'cpu_usage': f'infra_pd_cpu_usage_{date}.parquet',
            'memory_usage': f'infra_pd_memory_usage_{date}.parquet',
            'storage_used_ratio': f'infra_pd_storage_used_ratio_{date}.parquet',
            'store_unhealth_count': f'infra_pd_store_unhealth_count_{date}.parquet'
        },
        'tidb-tikv': {
            'cpu_usage': f'infra_tikv_cpu_usage_{date}.parquet',
            'memory_usage': f'infra_tikv_memory_usage_{date}.parquet',
            'server_is_up': f'infra_tikv_server_is_up_{date}.parquet',
            'available_size': f'infra_tikv_available_size_{date}.parquet',
            'raft_propose_wait': f'infra_tikv_raft_propose_wait_{date}.parquet',
            'raft_apply_wait': f'infra_tikv_raft_apply_wait_{date}.parquet',
            'rocksdb_write_stall': f'infra_tikv_rocksdb_write_stall_{date}.parquet'
        }
    }

def load_tidb_service_data(fault_date: str, service_name: str, metric_name: str) -> pd.DataFrame:
    """
    加载TiDB服务指标数据
    参数：
    - fault_date: 故障日期
    - service_name: 服务名称
    - metric_name: 指标名称
    返回：
    - df_metric: 包含指标数据的DataFrame
    """
    # 获取目录映射
    directories = get_tidb_services_directories()
    if service_name not in directories:
        print(f"未知的TiDB服务名称: {service_name}")
        return None
    
    # 构建数据目录路径
    data_dir = os.path.join(project_root, 'data', f'{fault_date}', 'metric-parquet', directories[service_name])
    # 获取文件映射
    file_mapping = get_tidb_services_files_mapping(fault_date)
    if service_name not in file_mapping or metric_name not in file_mapping[service_name]:
        print(f"未找到服务 {service_name} 的指标 {metric_name} 的文件映射")
        return None

    file_path = os.path.join(data_dir, file_mapping[service_name][metric_name])
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        return None

    df = pd.read_parquet(file_path)

    if len(df) == 0:
        print(f"文件 {file_path} 中无数据")
        return None

    return df

def analyze_tidb_metrics(fault_date: str, normal_periods: list[Tuple[str, str]], fault_period: Tuple[str, str]) -> Dict:
    """
    分析TiDB服务的异常指标
    参数：
    - fault_date: 故障日期
    - normal_periods: 正常时间段列表
    - fault_period: 故障时间段
    返回：
    - tidb_result: 包含TiDB服务级别分析结果的字典
    """
    tidb_analysis = {}
    # 获取tidb服务和核心指标
    core_metrics = get_tidb_core_metrics()
    for service_name, metrics_list in core_metrics.items():
        tidb_analysis[service_name] = {}
        for metric_name in metrics_list:
            # 加载TiDB服务指标数据
            df_metric = load_tidb_service_data(fault_date, service_name, metric_name)
            if len(df_metric) == 0:
                print(f"服务 {service_name} 在故障日期 {fault_date} 没有指标数据")
                continue
            
            # 初始化指标结构
            tidb_analysis[service_name][metric_name] = {
                'normal_stats': None,
                'fault_stats': None
            }

            # 1. 合并所有正常时间段数据进行统计
            all_normal_data = []

            for start, end in normal_periods:
                normal_data = df_metric[(df_metric['timestamp_ns'] >= int(start)) & (df_metric['timestamp_ns'] <= int(end))]

                if len(normal_data) > 0:
                    all_normal_data.append(normal_data)

            # 合并正常时间段数据并统计
            if all_normal_data:
                combined_normal_data = pd.concat(all_normal_data, ignore_index=True)
                print(f"    合并后正常时间段总数据行数: {len(combined_normal_data)}")

                # 获取统计（移除异常值）
                normal_desc = get_metrics_stats(
                    combined_normal_data,
                    [metric_name]
                )
                # print(json.dumps(normal_desc, ensure_ascii=False, indent=4))
                # exit()
                tidb_analysis[service_name][metric_name]['normal_stats'] = normal_desc.get(metric_name, None)

            # 故障时间段统计
            abnormal_data = df_metric[(df_metric['timestamp_ns'] >= int(fault_period[0])) &
                                      (df_metric['timestamp_ns'] <= int(fault_period[1]))]
            if len(abnormal_data):
                fault_desc = get_metrics_stats(
                    abnormal_data,
                    [metric_name]
                )
                tidb_analysis[service_name][metric_name]['fault_stats'] = fault_desc.get(metric_name, None)
                print(f"    故障时间段数据行数: {len(abnormal_data)}")

    # print(json.dumps(tidb_analysis, ensure_ascii=False, indent=4))
    # exit()
    return tidb_analysis

def get_target_nodes() -> List[str]:
    """
    获取目标分析节点列表（只分析aiops-k8s-01到aiops-k8s-08这8个节点）

    返回:
        目标节点名称列表
    """
    return [f'aiops-k8s-{i:02d}' for i in range(1, 9)]  # aiops-k8s-01 到 aiops-k8s-08

def get_node_metrics_files_mapping(date: str) -> Dict[str, str]:
    """
    获取节点指标文件名映射，返回指标名称到文件名的映射关系

    参数:
        date: 日期，格式如 "2025-06-06"

    返回:
        指标名到文件名的映射字典
    """
    return {
        'node_cpu_usage_rate': f'infra_node_node_cpu_usage_rate_{date}.parquet',
        'node_disk_read_bytes_total': f'infra_node_node_disk_read_bytes_total_{date}.parquet',
        'node_disk_read_time_seconds_total': f'infra_node_node_disk_read_time_seconds_total_{date}.parquet',
        'node_disk_write_time_seconds_total': f'infra_node_node_disk_write_time_seconds_total_{date}.parquet',
        'node_disk_written_bytes_total': f'infra_node_node_disk_written_bytes_total_{date}.parquet',
        'node_filesystem_free_bytes': f'infra_node_node_filesystem_free_bytes_{date}.parquet',
        'node_filesystem_size_bytes': f'infra_node_node_filesystem_size_bytes_{date}.parquet',
        'node_filesystem_usage_rate': f'infra_node_node_filesystem_usage_rate_{date}.parquet',
        'node_memory_MemAvailable_bytes': f'infra_node_node_memory_MemAvailable_bytes_{date}.parquet',
        'node_memory_MemTotal_bytes': f'infra_node_node_memory_MemTotal_bytes_{date}.parquet',
        'node_memory_usage_rate': f'infra_node_node_memory_usage_rate_{date}.parquet',
        'node_network_receive_bytes_total': f'infra_node_node_network_receive_bytes_total_{date}.parquet',
        'node_network_receive_packets_total': f'infra_node_node_network_receive_packets_total_{date}.parquet',
        'node_network_transmit_bytes_total': f'infra_node_node_network_transmit_bytes_total_{date}.parquet',
        'node_network_transmit_packets_total': f'infra_node_node_network_transmit_packets_total_{date}.parquet',
        'node_sockstat_TCP_inuse': f'infra_node_node_sockstat_TCP_inuse_{date}.parquet'
    }

def load_node_metric_data(date: str, metric_name: str) -> Optional[pd.DataFrame]:
    """
    加载指定日期和指标的节点数据

    参数:
        date: 日期，格式如 "2025-06-06"
        metric_name: 指标名称，如 "node_cpu_usage_rate"

    返回:
        节点指标数据DataFrame，如果文件不存在则返回None
    """
    node_dir = os.path.join(project_root, 'data', f'{date}', 'metric-parquet', 'infra', 'infra_node')

    file_mapping = get_node_metrics_files_mapping(date)

    if metric_name not in file_mapping:
        print(f"故障的指标名称: {metric_name}")
        return None

    file_path = os.path.join(node_dir, file_mapping[metric_name])

    try:
        if not os.path.exists(file_path):
            print(f"文件不存在: {file_path}")
            return None

        df = pd.read_parquet(file_path)

        # 只保留目标节点数据
        target_nodes = get_target_nodes()
        df_filtered = df[df['kubernetes_node'].isin(target_nodes)]

        if len(df_filtered) == 0:
            print(f"文件 {file_path} 中未找到目标节点数据")
            return None

        return df_filtered

    except Exception as e:
        print(f"加载文件 {file_path} 时出错: {e}")
        return None

def analyze_node_metrics(fault_date: str, normal_periods: List[Tuple[str, str]], fault_period: Tuple[str, str]) -> Dict[str, List[Dict]]:
    """
    分析Node节点的指标异常，分析结果按 node -> pod -> metric 组织
    参数:
        fault_date: 故障日期，格式如 "2025-06-06"
        normal_periods: 正常时间段列表，每个元素为 (start_ns, end_ns)
        fault_period: 故障时间段，格式为 (start_ns, end_ns)

    返回:
        异常Node指标列表
    """
    nodes_analysis = {}
    target_nodes = get_target_nodes()
    for node_name in target_nodes:
        print(f"\n=== 处理节点: {node_name} ===")
        for metric_name in node_metrics:
            df_metric = load_node_metric_data(fault_date, metric_name)
            if df_metric is None:
                continue
            df_node = df_metric[df_metric['kubernetes_node'] == node_name]
            if len(df_node) == 0:
                continue

            all_normal_data = []

            for start, end in normal_periods:
                normal_data = df_node[(df_node['timestamp_ns'] >= int(start)) & (df_node['timestamp_ns'] <= int(end))]
                if len(normal_data) > 0:
                    all_normal_data.append(normal_data)

            # 合并正常时间段数据并统计
            if all_normal_data:
                combined_normal_data = pd.concat(all_normal_data, ignore_index=True)
                print(f"    合并后正常时间段总数据行数: {len(combined_normal_data)}")

                # 获取统计（移除异常值）
                normal_desc = get_metrics_stats(
                    combined_normal_data,
                    [metric_name]
                )                   

            # 2. 故障时间段统计
            print(f"    故障时间段分析:")
            abnormal_data = df_node[(df_node['timestamp_ns'] >= int(fault_period[0])) &
                                    (df_node['timestamp_ns'] <= int(fault_period[1]))]
            if len(abnormal_data):
                fault_desc = get_metrics_stats(
                    abnormal_data,
                    [metric_name]
                )
                print(f"    故障时间段数据行数: {len(abnormal_data)}")

            if normal_desc is not None and fault_desc is not None:#过滤掉变化倍数在 0.95 到 1.05 之间的指标
                normal_mean = normal_desc[metric_name]['mean']
                fault_mean = fault_desc[metric_name]['mean']
                epsilon = 1e-9  # 极小数，防止除零
                ratio = (fault_mean + epsilon) / (normal_mean + epsilon)
                if 0.95 <= ratio <= 1.05:
                    print(f"    指标 {metric_name} 变化倍数 {ratio:.2f} 在 0.95~1.05 之间，跳过保存")
                    continue

            nodes_analysis[node_name] = {}
            nodes_analysis[node_name][metric_name] = {}
            nodes_analysis[node_name][metric_name]['normal_stats'] = normal_desc.get(metric_name, {})
            nodes_analysis[node_name][metric_name]['fault_stats'] = fault_desc.get(metric_name, {})

    return nodes_analysis

def get_target_pods() -> List[str]:
    """
    获取目标分析 Pod 列表
    """
    services = [
        "adservice-0", "adservice-1", "adservice-2",
        "cartservice-0", "cartservice-1", "cartservice-2",
        "checkoutservice-0", "checkoutservice-1", "checkoutservice-2",
        "currencyservice-0", "currencyservice-1", "currencyservice-2",
        "emailservice-0", "emailservice-1", "emailservice-2",
        "frontend-0", "frontend-1", "frontend-2",
        "paymentservice-0", "paymentservice-1", "paymentservice-2",
        "productcatalogservice-0", "productcatalogservice-1", "productcatalogservice-2",
        "recommendationservice-0", "recommendationservice-1", "recommendationservice-2",
        "redis-cart-0",
        "shippingservice-0", "shippingservice-1", "shippingservice-2"
    ]
    return services

def get_pod_metrics_files_mapping(date: str) -> Dict[str, str]:
    """
    获取 Pod 指标文件名映射，返回指标名称到文件名的映射关系

    参数:
        date: 日期，格式如 "2025-06-06"

    返回:
        指标名到文件名的映射字典
    """
    return {
        'pod_cpu_usage': f'infra_pod_pod_cpu_usage_{date}.parquet',
        'pod_fs_reads_bytes': f'infra_pod_pod_fs_reads_bytes_{date}.parquet',
        'pod_fs_writes_bytes': f'infra_pod_pod_fs_writes_bytes_{date}.parquet',
        'pod_memory_working_set_bytes': f'infra_pod_pod_memory_working_set_bytes_{date}.parquet',
        'pod_network_receive_bytes': f'infra_pod_pod_network_receive_bytes_{date}.parquet',
        'pod_network_receive_packets': f'infra_pod_pod_network_receive_packets_{date}.parquet',
        'pod_network_transmit_bytes': f'infra_pod_pod_network_transmit_bytes_{date}.parquet',
        'pod_network_transmit_packets': f'infra_pod_pod_network_transmit_packets_{date}.parquet',
        'pod_processes': f'infra_pod_pod_processes_{date}.parquet'
    }

def load_pod_metric_data(date: str, metric_name: str) -> Optional[pd.DataFrame]:
    """
    加载指定日期和指标的 Pod 数据

    参数:
        date: 日期，格式如 "2025-06-06"
        metric_name: 指标名称，如 "pod_cpu_usage"

    返回:
        Pod 指标数据 DataFrame，如果文件不存在则返回 None
    """
    pod_dir = os.path.join(project_root, 'data', f'{date}', 'metric-parquet', 'infra', 'infra_pod')

    file_mapping = get_pod_metrics_files_mapping(date)

    if metric_name not in file_mapping:
        print(f"故障的指标名称: {metric_name}")
        return None

    file_path = os.path.join(pod_dir, file_mapping[metric_name])

    try:
        if not os.path.exists(file_path):
            print(f"文件不存在: {file_path}")
            return None

        df = pd.read_parquet(file_path)

        # 只保留目标 pod 数据
        target_pods = get_target_pods()
        df_filtered = df[df['pod'].isin(target_pods)]

        if len(df_filtered) == 0:
            print(f"文件 {file_path} 中未找到目标 pod 数据")
            return None

        return df_filtered

    except Exception as e:
        print(f"加载文件 {file_path} 时出错: {e}")
        return None

def analyze_pod_metrics(fault_date: str, normal_periods: List[Tuple[str, str]], fault_period: Tuple[str, str]) -> Dict[str, List[Dict]]:
    """
    分析Pod节点的指标异常，分析结果按 node -> pod -> metric 组织
    参数:
        fault_date: 故障日期，格式如 "2025-06-06"
        normal_periods: 正常时间段列表，每个元素为 (start_ns, end_ns)
        fault_period: 故障时间段，格式为 (start_ns, end_ns)
    """
    pods_analysis = {}

    for metric_name in pod_metrics:
        print(f"\n=== 处理指标: {metric_name} ===")
        df_metric = load_pod_metric_data(fault_date, metric_name)
        if df_metric is None:
            continue
        # 按 instance-pod 分组
        df_metric = df_metric.groupby(['instance', 'pod'])
        for (node, pod), group in df_metric:
            print(f"\n=== 处理 Node: {node}, Pod: {pod} ===")
            if len(group):

                # 1. 合并所有正常时间段数据
                print(f"    正常时间段分析:")
                all_normal_data = []

                for start, end in normal_periods:
                    normal_data = group[(group['timestamp_ns'] >= int(start)) & (group['timestamp_ns'] <= int(end))]
                    if len(normal_data) > 0:
                        all_normal_data.append(normal_data)

                # 合并正常时间段数据并统计
                if all_normal_data:
                    combined_normal_data = pd.concat(all_normal_data, ignore_index=True)
                    print(f"    合并后正常时间段总数据行数: {len(combined_normal_data)}")

                    normal_desc = get_metrics_stats(
                        combined_normal_data,
                        [metric_name]
                    )

                # 2. 故障时间段统计
                print(f"    故障时间段分析:")
                df_pod_fault = group[(group['timestamp_ns'] >= int(fault_period[0])) &
                                    (group['timestamp_ns'] <= int(fault_period[1]))]
                if len(df_pod_fault):
                    fault_desc = get_metrics_stats(
                        df_pod_fault,
                        [metric_name]
                    )
                    print(f"    故障时间段数据行数: {len(df_pod_fault)}")
                if len(normal_desc) > 0 and len(fault_desc) > 0:#过滤掉变化倍数在 0.95 到 1.05 之间的指标
                    normal_mean = normal_desc[metric_name]['mean']
                    fault_mean = fault_desc[metric_name]['mean']
                    epsilon = 1e-9  # 极小数，防止除零
                    ratio = (fault_mean + epsilon) / (normal_mean + epsilon)
                
                    if 0.95 <= ratio <= 1.05:
                        print(f"    指标 {metric_name} 变化倍数 {ratio:.2f} 在 0.95~1.05 之间，跳过保存")
                        continue

                if node not in pods_analysis:
                    pods_analysis[node] = {}
                pods_analysis[node][pod] = {}
                pods_analysis[node][pod][metric_name] = {}
                pods_analysis[node][pod][metric_name]['fault_stats'] = fault_desc.get(metric_name, {})
                pods_analysis[node][pod][metric_name]['normal_stats'] = normal_desc.get(metric_name, {})
    # print(json.dumps(pods_analysis, indent=2, ensure_ascii=False))
    # exit(0)
    return pods_analysis


async def metric_refinement(df_fault_timestamps: pd.DataFrame, index: int, fault_start: str, fault_end: str) -> str:
    """
    对指定索引的故障时间戳进行指标分析
    参数：
    - df_fault_timestamps: 故障时间戳DataFrame
    - index: 当前故障索引
    - fault_start: 当前故障开始时间戳
    - fault_end: 当前故障结束时间戳
    返回：
    - service_results: 包含SERVICE和TiDB服务级别分析结果的JSON字符串
    """
    # 获取当前故障日期
    fault_date = df_fault_timestamps.iloc[index]['date']
    # 获取正常时间段与故障时间段
    normal_periods = get_normal_periods(df_fault_timestamps, index)
    fault_period = (fault_start, fault_end)

    print(f"开始分析故障索引：{index}")
    print("=" * 80)

    # 分析普通微服务
    service_result = await analyze_service_metrics(fault_date, normal_periods, fault_period)
    if len(service_result) == 0:
        print("无异常Service指标")
    else:
        print(f"成功分析了{len(service_result)}个异常Service指标")

    # 分析TiDB服务
    tidb_result = analyze_tidb_metrics(fault_date, normal_periods, fault_period)
    if len(tidb_result) == 0:
        print("无异常TiDB指标")
    else:
        print(f"成功分析了{len(tidb_result)}个异常TiDB指标")

    # 分析 infra/node
    node_result = analyze_node_metrics(fault_date, normal_periods, fault_period)
    if len(node_result) == 0:
        print("无异常Node指标")
    else:
        print(f"成功分析了{len(node_result)}个异常Node指标")

    # 分析 infra/pod
    pod_result = analyze_pod_metrics(fault_date, normal_periods, fault_period)
    if len(pod_result) == 0:
        print("无异常Pod指标")
    else:
        print(f"成功分析了{len(pod_result)}个异常Pod指标")

    return json.dumps({
        "service": service_result,
        "tidb": tidb_result,
        "node": node_result,
        "pod": pod_result
    }, indent=2, ensure_ascii=False)