"""
存放各种prompt模板的模块
"""

def get_multimodal_analysis_prompt(
    log_data: str | None = None,
    trace_data: str | None = None,
    metric_data: str | None = None
) -> str:
    """
    获取多模态分析的prompt模板，支持缺失部分模态数据

    参数:
        log_data: (filtered_logs_csv, log_unique_dict) 或 None
        trace_data: (filtered_traces_csv, trace_unique_dict, status_combinations_csv) 或 None
        metric_data: 字符串类型的metric分析结果 或 None

    返回:
        构建好的多模态分析prompt字符串
    """

    # 固定的组件列表，不再从数据中动态提取
    # all_node_names = ['aiops-k8s-01', 'aiops-k8s-02', 'aiops-k8s-03', 'aiops-k8s-04',
    #                   'aiops-k8s-05', 'aiops-k8s-06', 'aiops-k8s-07', 'aiops-k8s-08']

    # all_service_names = ['cartservice', 'currencyservice', 'frontend', 'adservice',
    #                      'recommendationservice', 'shippingservice', 'checkoutservice',
    #                      'paymentservice', 'emailservice', 'redis-cart', 'productcatalogservice', 'tidb-tidb', 'tidb-pd', 'tidb-tikv']

    # all_pod_names = ['cartservice-0', 'cartservice-1', 'cartservice-2', 'currencyservice-0',
    #                  'currencyservice-1', 'currencyservice-2', 'frontend-0', 'frontend-1',
    #                  'frontend-2', 'adservice-0', 'adservice-1', 'adservice-2',
    #                  'recommendationservice-0', 'recommendationservice-1', 'recommendationservice-2',
    #                  'shippingservice-0', 'shippingservice-1', 'shippingservice-2',
    #                  'checkoutservice-0', 'checkoutservice-1', 'checkoutservice-2',
    #                  'paymentservice-0', 'paymentservice-1', 'paymentservice-2',
    #                  'emailservice-0', 'emailservice-1', 'emailservice-2',
    #                  'productcatalogservice-0', 'productcatalogservice-1', 'productcatalogservice-2',
    #                  'redis-cart-0']

    available_modalities = []
    data_sections = []

    # 处理日志数据
    if log_data:  # 检查是否有有效的CSV数据
        available_modalities.append("日志数据")
        data_sections.append(f"""
### 日志异常数据:
{log_data}""")

    # 处理trace数据
    if trace_data:  # 检查是否有有效的CSV数据
        available_modalities.append("链路追踪数据")
        data_sections.append(f"""
### 微服务链路异常数据（孤立森林检测结果，前20个）:
{trace_data}""")

    # 处理指标数据
    if metric_data:  # 检查是否有有效的字符串数据
        available_modalities.append("系统指标数据")
        data_sections.append(f"""
### 系统指标异常数据:
{metric_data}""")

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

    return f"""
        ### Language Enforcement
        -Input may contain Chinese, **but output MUST be entirely in English** (no Chinese characters).
        请根据提供的{modalities_text}，进行综合故障分析，完成异常检测、故障分类、根因定位。
        特别注意**缺失数据和空数据,代表数据波动极小,通常情况默认正常,严禁分析和定位为根因**
        可用的监控数据:
        {data_content}
        """
