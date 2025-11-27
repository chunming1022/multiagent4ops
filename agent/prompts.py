"""
存放各种prompt模板的模块
"""
from typing import Dict


def get_trace_refinement_prompt(
    latency_traces: str | None,
    status_traces: str | None
) -> str:
    pre = """
Strictly follow these instructions to generate a text description for each trace. The description must be accurate, complete, and retain key entries based on semantic meaning, removing duplicates. Return the description as a list, such as ["trace1 description", "trace2 description"...]. Do not include any other explanations or text.
Explanation of the trace field for delay exceptions：
- 'node_name':The name of the Kubernetes worker node, which is also the node where the child_pod runs.
- 'service_name': The microservice name corresponding to the child_pod.
- 'parent_pod': The Pod instance name initiating the call (upstream).
- 'child_pod': The Pod instance name receiving the call (downstream).
- 'operation_name': The specific API method or operation called (e.g., ServiceName/MethodName).
- 'normal_avg_duration': The average duration of the operation during normal periods (usually in nanoseconds or microseconds).
- 'anomaly_avg_duration': The average duration of the operation during periods identified as anomalous by the system.
- 'anomaly_count': The number of times this anomaly occurred.
Explanation of the trace field for status code errors：
- 'node_name': The name of the Kubernetes worker node, which is also the node where the child_pod runs.
- 'service_name': The microservice name corresponding to the child_pod.
- 'parent_pod': The Pod instance name initiating the call (upstream).
- 'child_pod': The Pod instance name receiving the call (downstream).
- 'operation_name': The specific API method or operation called (e.g., ServiceName/MethodName).
- 'status_message': Detailed information about the error.
- 'abnormal_count': The number of times this abnormal status code operation occurred.   
"""
    last = f"""
Trace of delay anomaly:\n{latency_traces}    
Trace of status code errors:\n{status_traces}
"""
    return pre + last

def get_log_refinement_prompt(
    logs: str | None
) -> str:
    pre = """
Strictly follow these instructions to generate a text description for each log entry. The description must be accurate, complete, and retain key entries based on semantic meaning, removing duplicates. Return the description as a list, such as ["Log 1 Description", "Log 2 Description"...]. Do not include any other explanations or text.
Explanation of the log fields：
- 'node_name': The name of the Kubernetes worker node. Indicates which physical/virtual machine the Pod generating this log is running on.
- 'service_name': The name of the microservice. Indicates which application service recorded this error.
- 'pod_name': The name of the Pod generating the log. It is the specific instance of the microservice in Kubernetes.
- 'message': The main content of the log. Usually a JSON string containing detailed error information, which is the most valuable part.
- 'occurrence_count': The number of times this log pattern occurred.
    """
    last = f"""
Error Log：\n{logs}
    """
    return pre + last

def get_node_pod_analysis_prompt(
    normal_stats: str,
    fault_stats: str
) -> str:
    pre = """
Strictly follow the instructions below to compare the statistical information of metrics during normal and fault periods, determine which metrics were abnormal during the fault period, and indicate their abnormal trends. The returned format should be {"metric 1":"Abnormal Trend","metric 2":"Abnormal Trend"...}; If normal, return an empty dictionary {}. Do not include any other explanations or text.
## Infrastructure metric Classification Explanation
### Computing resource metrics(kpi_key)：
- 'node_cpu_usage_rate': CPU utilization reflects the node's CPU utilization.  
- 'node_memory_usage_rate': Memory utilization reflects the node's memory utilization.  
- 'pod_cpu_usage': Pod CPU utilization  
- 'pod_memory_working_set_bytes': Pod working set memory usage  
- 'pod_processes': Number of processes running in the Pod  
### Storage resource metrics(kpi_key)：
- 'node_filesystem_usage_rate': Filesystem usage rate reflects node storage usage  
- 'node_disk_read_bytes_total / node_disk_read_time_seconds_total': Disk read bytes/time reflects disk read performance  
- 'node_disk_written_bytes_total / node_disk_write_time_seconds_total': Disk write bytes/time reflects disk write performance  
- 'pod_fs_reads_bytes': Pod filesystem read bytes  
- 'pod_fs_writes_bytes': Pod filesystem write bytes  

### Network resource metrics(kpi_key)：
- 'node_network_receive_bytes_total': Network receive bytes, reflects node network receive traffic  
- 'node_network_transmit_bytes_total': Network transmit bytes, reflects node network transmit traffic  
- 'node_network_receive_packets_total': Network receive packets per second for each interface  
- 'node_network_transmit_packets_total': Network transmit packets per second for each interface  
- 'node_sockstat_TCP_inuse': TCP connections, reflects node TCP connection activity  
- 'pod_network_receive_bytes': Pod network receive bytes  
- 'pod_network_receive_packets': Pod network receive packets  
- 'pod_network_transmit_bytes': Pod network transmit bytes  
- 'pod_network_transmit_packets': Pod network transmit packets  

### Explanation of metric statistics：
- `mean`: Average value of the metric - reflects the average value of the metric during normal and fault periods
- `std`: Standard deviation of the metric - reflects the distribution of the metric values during normal and fault periods
- `min`: Minimum value of the metric - reflects the minimum value of the metric during normal and fault periods
- `25%`: 25th percentile of the metric - reflects the 25th percentile value of the metric during normal and fault periods
- `50%`: 50th percentile of the metric - reflects the 50th percentile value of the metric during normal and fault periods
- `75%`: 75th percentile of the metric - reflects the 75th percentile value of the metric during normal and fault periods
- `95%`: 95th percentile of the metric - reflects the 95th percentile value of the metric during normal and fault periods
- `99%`: 99th percentile of the metric - reflects the 99th percentile value of the metric during normal and fault periods
- `max`: Maximum value of the metric - reflects the maximum value of the metric during normal and fault periods
- `non_zero_rate`: Non-zero rate - proportion of non-zero values of the metric during normal and fault periods

### Abnormal judgment rules：
- 'node_cpu_usage_rate, node_memory_usage_rate, node_filesystem_usage_rate': If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- 'pod_cpu_usage, pod_memory_working_set_bytes, pod_processes':  If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- 'node_disk_read_bytes_total, node_disk_written_bytes_total / node_disk_read_time_seconds_total, node_disk_write_time_seconds_total': If the metric value shows a decrease in throughput or a surge in read/write latency during the fault period, it is judged as an abnormal metric
- 'pod_fs_reads_bytes, pod_fs_writes_bytes': If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- 'node_network_receive_bytes_total, node_network_transmit_bytes_total': If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- 'node_network_receive_packets_total, node_network_transmit_packets_total': If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- 'node_sockstat_TCP_inuse': If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- 'pod_network_receive_bytes, pod_network_receive_packets': If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- 'pod_network_transmit_bytes, pod_network_transmit_packets': If the metric value is abnormally high during the fault period, it is judged as an abnormal metric
- metrics that do not meet the above criteria are judged as normal metrics
    """
    last = f"""
Normal time period metric statistics information：{normal_stats}
Fault time period metric statistics information：{fault_stats}
    """ 

    return pre + last

def get_tidb_analysis_prompt(
    normal_stats: str,
    fault_stats: str
) -> str:
    pre = """
Strictly follow the instructions below to compare the statistical information of metrics during normal and fault periods, determine which metrics were abnormal during the fault period, and indicate their abnormal trends. The returned format should be {"metric 1":"Abnormal Trend","metric 2":"Abnormal Trend"...}; If normal, return an empty dictionary {}. Do not include any other explanations or text.
## TiDB Key Metrics Explanation (Database Components)
### TiDB Key Metrics:
- `failed_query_ops`: Number of failed requests - Database request error rate metric
- `duration_99th`: 99th percentile request latency - Key database performance metric
- `connection_count`: Number of connections - Database load metric
- `server_is_up`: Number of live service nodes - Database availability metric
- `cpu_usage`: CPU utilization - Database resource saturation metric
- `memory_usage`: Memory usage - Database resource usage metric

### TiKV Component Specifications：
- `cpu_usage`: CPU utilization - Storage layer resource usage
- `memory_usage`: Memory usage - Storage layer resource usage
- `server_is_up`: Number of live service nodes - Storage layer availability
- `available_size`: Available storage capacity - Storage capacity warning
- `raft_propose_wait`: RaftPropose wait latency P99 - Distributed consistency performance
- `raft_apply_wait`: RaftApply wait latency P99 - Distributed consistency performance
- `rocksdb_write_stall`: RocksDB write stall count - Storage engine anomaly metric

### PD component metrics：
- `store_up_count`: Number of healthy Stores - Cluster health
- `store_down_count`: Number of Down Stores - Cluster fault metric
- `store_unhealth_count`: Number of Unhealthy Stores - Cluster anomaly metric
- `storage_used_ratio`: Used storage ratio - Cluster capacity metric
- `cpu_usage`: CPU utilization - Scheduler resource usage
- `memory_usage`: Memory usage - Scheduler resource usage

### Explanation of metric statistics：
- `mean`: Average value of the metric - Reflects the average value of the metric during normal and fault periods
- `std`: Standard deviation of the metric - Reflects the distribution of the metric values during normal and fault periods
- `min`: Minimum value of the metric - Reflects the minimum value of the metric during normal and fault periods
- `25%`: 25th percentile of the metric - Reflects the 25th percentile value of the metric during normal and fault periods
- `50%`: 50th percentile of the metric - Reflects the 50th percentile value of the metric during normal and fault periods
- `75%`: 75th percentile of the metric - Reflects the 75th percentile value of the metric during normal and fault periods
- `95%`: 95th percentile of the metric - Reflects the 95th percentile value of the metric during normal and fault periods
- `99%`: 99th percentile of the metric - Reflects the 99th percentile value of the metric during normal and fault periods
- `max`: Maximum value of the metric - Reflects the maximum value of the metric during normal and fault periods
- `non_zero_rate`: Non-zero rate - Reflects the proportion of non-zero values of the metric during normal and fault periods

### Abnormal judgment rules：
- `failed_query_ops, duration_99th`：If the metric value suddenly or continuously increases during the fault period, it is judged as an abnormal metric.
- `connection_count`：If the metric value increases or decreases during the fault period, it is judged as an abnormal metric.
- `server_is_up`: If the metric value significantly decreases during the fault period, it is judged as an abnormal metric.
- `cpu_usage`: If the metric value continuously approaches abnormal increase during the fault period, it is judged as an abnormal metric.
- `memory_usage`: If the metric value abnormally increases during the fault period, it is judged as an abnormal metric.
- `available_size`: If the metric value abnormally decreases during the fault period, it is judged as an abnormal metric.
- `raft_propose_wait, raft_apply_wait`：If the metric value significantly increases during the fault period, it is judged as an abnormal metric.
- `rocksdb_write_stall`: If the metric value continuously appears or surges during the fault period, it is judged as an abnormal metric.
- `store_up_count`: If the metric value significantly decreases during the fault period, it is judged as an abnormal metric.
- `store_down_count, store_unhealth_count`: If the metric value is greater than 0 during the fault period, it is judged as an abnormal metric.
- `storage_used_ratio`: If the metric value abnormally increases during the fault period, it is judged as an abnormal metric.
- metrics that do not meet the above criteria are judged as normal metrics.
    """ 
    last = f"""
Normal time period metric statistics information：{normal_stats}
Fault time period metric statistics information：{fault_stats}
    """ 

    return pre + last

def get_service_analysis_prompt(
    normal_stats: str,
    fault_stats: str
) -> str:
    """
    获取指标筛选的prompt模板

    参数:
        normal_stats: 正常时间段指标统计信息
        fault_stats: 故障时间段指标统计信息

    返回:
        构建好的指标筛选prompt字符串
    """
    pre = """
Strictly follow the instructions below to compare the statistical information of indicators during normal and fault periods, determine which indicators were abnormal during the fault period, and indicate their abnormal trends. The returned format should be {"Indicator 1":"Abnormal Trend","Indicator 2":"Abnormal Trend"...}; If normal, return an empty dictionary {}. Do not include any other explanations or text.
## APM Key Metrics Explanation (Microservices)
### Request-Response Metrics:
- `request`: Number of requests - Reflects the total number of business requests received by the service
- `response`: Number of responses - Reflects the total number of requests successfully processed and responded to by the service
- `rrt`: Average response time - Reflects the average response time for processing requests by the service
### Exception Metrics:
- `timeout`: Number of timeouts - Reflects the number of times the service timed out while processing requests
- `error_ratio`: Error ratio - The proportion of error requests to total requests
- `client_error_ratio`: Client error ratio - The proportion of client errors to total requests
- `server_error_ratio`: Server error ratio - The proportion of server errors to total requests

### Metric Statistics Explanation:
- `mean`: Average value of the metric - Reflects the average value of the metric during normal and fault periods
- `std`: Standard deviation of the metric - Reflects the distribution of the metric values during normal and fault periods
- `min`: Minimum value of the metric - Reflects the minimum value of the metric during normal and fault periods
- `25%`: 25th percentile of the metric - Reflects the 25th percentile value of the metric during normal and fault periods
- `50%`: 50th percentile of the metric - Reflects the 50th percentile value of the metric during normal and fault periods
- `75%`: 75th percentile of the metric - Reflects the 75th percentile value of the metric during normal and fault periods
- `95%`: 95th percentile of the metric - Reflects the 95th percentile value of the metric during normal and fault periods
- `99%`: 99th percentile of the metric - Reflects the 99th percentile value of the metric during normal and fault periods
- `max`: Maximum value of the metric - Reflects the maximum value of the metric during normal and fault periods
- `non_zero_rate`: Non-zero rate - The proportion of non-zero values of the metric during normal and fault periods

### Abnormality Determination Rules:
- `request, response`: If the metric value suddenly increases or decreases during the fault period, it is judged as an abnormal metric
- `rrt`: If the metric value significantly increases during the fault period, it is judged as an abnormal metric
- `timeout, error_ratio, client_error_ratio, server_error_ratio`: If the non-zero values of the metric increase during the fault period, it is judged as an abnormal metric
- Metrics that do not meet the above criteria are judged as normal metrics, with special attention to missing and empty data, which represent minimal data fluctuations and are generally considered normal:
    """ 
    last = f"""
Normal time period metric statistics information：{normal_stats}
Fault time period metric statistics information：{fault_stats}
    """
    return pre + last


def get_multimodal_analysis_prompt(
    candidates: Dict,
    pods_on_node: Dict,
    metric_data: Dict,
    log_data: str | None = None,
    trace_data: str | None = None,
) -> str:
    """
    获取多模态分析的prompt模板，支持缺失部分模态数据

    参数:
        log_data: (filtered_logs_csv, log_unique_dict) 或 None
        trace_data: (filtered_traces_csv, trace_unique_dict, status_combinations_csv) 或 None
        metric_data: 字符串类型的metric分析结果

    返回:
        构建好的多模态分析prompt字符串
    """
    available_modalities = []
    data_sections = []

    # 处理日志数据
    if log_data:  # 检查是否有有效的CSV数据
        available_modalities.append("Log Data")
        data_sections.append(f"""
### Log Anomaly Data:
{log_data}""")

    # 处理trace数据
    if trace_data:  # 检查是否有有效的CSV数据
        available_modalities.append("Trace Data")
        data_sections.append(f"""
### Trace Anomaly Data:
{trace_data}""")

    # 处理指标数据
    service_data = metric_data["service"]
    tidb_data = metric_data["tidb"]
    node_data = metric_data["node"]
    pod_data = metric_data["pod"]
    flag = 0
    if len(service_data) > 0:  # 检查是否有有效的字符串数据
        flag = 1
        data_sections.append(f"""
### Microservice Metric Anomaly Data:
{service_data}""")
    if len(tidb_data) > 0:  # 检查是否有有效的字符串数据
        flag = 1
        data_sections.append(f"""
### TiDB Metric Anomaly Data:
{tidb_data}""")
    if len(node_data) > 0:  # 检查是否有有效的字符串数据
        flag = 1
        data_sections.append(f"""
### Node Metric Anomaly Data:
{node_data}""")
    if len(pod_data) > 0:  # 检查是否有有效的字符串数据
        flag = 1
        data_sections.append(f"""
### Pod Metric Anomaly Data:
{pod_data}""")
    if flag == 1:
        available_modalities.append("System Metric Data")



    # # 如果没有任何有效数据，返回错误提示
    # if not data_sections:
    #     return "错误：未提供任何有效的监控数据，无法进行故障分析。"

    # 构建数据部分
    data_content = "\n".join(data_sections)

    # 构建包含三种类型组件的列表
    # components_list = []
    # components_list.extend(all_node_names)
    # components_list.extend(all_service_names)
    # components_list.extend(all_pod_names)
    modalities_text = "、".join(available_modalities)

    # 构建候选根因组件列表
    components = []
    for component_type, component_names in candidates.items():
        components.extend(component_names)
    return f"""
### Language Enforcement
Please perform a comprehensive fault analysis based on the provided {modalities_text}, completing anomaly detection, fault classification, and root cause localization.
Special note: **Missing and empty data represent minimal data fluctuations and are generally considered normal.**
Available monitoring data:
{data_content}
Pod deployment information, reflecting the Pod instances deployed on nodes:
{pods_on_node}
Candidate root cause components:
{components}
""" 