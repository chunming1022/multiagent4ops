from typing import List
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient

# ------------------ 智能体配置 ------------------
model_client = OpenAIChatCompletionClient(
    model="qwen-flash",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    api_key="sk-bd079ff734ab441fabe1dd900bdd9934",
    parallel_tool_calls=False,
    model_info={
        "vision": False,
        "function_calling": True,
        "json_output": True,
        "family": "unknown",
    },
)

# model_client = OpenAIChatCompletionClient(
#     model="gemini-2.5-flash",
#     base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
#     api_key="AIzaSyAaTQE-qv3mKoL5za2Kmu7lWMfvEZI_qdA",
#     parallel_tool_calls=False,
#     model_info={
#         "vision": False,
#         "function_calling": True,
#         "json_output": True,
#         "family": "unknown",
#     },
# )

# model_client = OpenAIChatCompletionClient(
#     model="qwen3:4b",
#     base_url="http://localhost:11434/v1",
#     api_key="",
#     parallel_tool_calls=False,
#     model_info={
#         "vision": False,
#         "function_calling": True,
#         "json_output": True,
#         "family": "unknown",
#     },
# )

logs_agent = AssistantAgent(
    name="LogsAgent",
    description="An intelligent agent focused on processing log data.",
    model_client=model_client,
    system_message="""
    You are a professional intelligent agent focused on processing log data in intelligent microservice operations and maintenance.
    """
)

metrics_agent = AssistantAgent(
    name="MetricsAgent",
    description="An intelligent agent focused on processing metric data.",
    model_client=model_client,
    system_message = """
    You are a professional intelligent agent focused on processing metric data in intelligent microservice operations and maintenance.
    """
)

traces_agent = AssistantAgent(
    name="TracesAgent",
    description="An intelligent agent focused on processing trace data.",
    model_client=model_client,
    system_message="""
    You are a professional intelligent agent focused on processing trace data in intelligent microservice operations and maintenance.
    """
)

orchestration_agent = AssistantAgent(
    name="OrchestrationAgent",
    description="The core orchestrator and global controller of the entire intelligent microservice operations and maintenance process. Responsible for task orchestration and sub-agent invocation. You should be the first speaker.",
    model_client=model_client,
    system_message="""
# Role 
You are an intelligent agent that plays a core role in task orchestration and global control in a microservice intelligent operation and maintenance system.
# Context
The system employs a dynamic deployment architecture, comprising 10 core microservices and 8 virtual machines. Each microservice deploys 3 Pods, totaling 30 Pods, which are dynamically scheduled and distributed across the 8 virtual machines. Additionally, the TiDB component is also deployed on virtual machines, including three core services: tidb-tidb, tidb-pd, and tidb-tikv, each deployed as one Pod.
# Instruction
1. Receive User Input:
- Receive {{monitoring data}} and {{candidate root cause components}} from user input.
2. Subset Partitioning and Task Execution（Execute step by step）:
- Step 1: Initialize an empty root factor set {{C}} and an empty information subset {{I}};
- Step 2: Randomly select a component from the {{candidate root cause components}} and add it to the root factor set {{C}};
- Step 3: Find monitoring data related to this component in the {{monitoring data}} (anomaly data involving the component's logs, traces, microservice metrics, TiDB service metrics, node metrics, and Pod metrics), remove it from the monitoring data, and add it to the information subset {{I}} (the information must originate from user-provided data and be consistent; it cannot be fabricated);
- Step 4: Find other root cause components related to these monitoring data from the {{candidate root cause components}} (services and service instances are related to each other, such as adservice being related to adservice-0, adservice-1, and adservice-2, vice versa.), remove them from the {{candidate root cause components}}, and add them to the root factor set {{C}} (they must exist in the {{root cause component}} to be removed and added to {{C}}; otherwise, they cannot be added.
- Step 5: Repeat steps 3 to 4 until no other related root cause components are found. This results in a task subset {{T}}, containing the information subset {{I}} and the root factor set {{C}}.
- Step 6: Repeat steps 1 to 5 until all candidate root cause components have been considered.
- Step 7: Name all task subsets {{T}} (T0, T1...).
# Output Format
- Output each task subset in JSON format. Each task subset contains the following three parts:
A. Task subset name: The name of the task subset {{T}}, used to identify the task subset.
B. candidates: The root factor set {{C}}.
C. info: The information subset {{I}}. For example:
{
    "SubTask_0":{
        "name": "T0",
        "candidates": [{{C}}],
        "info": [{{I}}]
    }
    ...
}
"""
)

ad_agent = AssistantAgent(
    name="ADAgent",
    description="负责基于提炼后的多源数据，执行精确的异常检测，判断当前系统是否处于异常状态。",
    model_client=model_client,
    system_message="""
# Role
You are a professional intelligent agent focused on anomaly detection (AD) in intelligent microservice operations and maintenance.
# Context

# Instruction
1. Input Receiver: 
- Receives a task subset {{T}} from the Orchestration agent, containing a root factor set {{C}} and an information subset {{I}}.
2. Anomaly Detection: 
- Performs anomaly detection once for each task subset, determining whether an anomaly exists based on the information subset {{I}} within the task subset.
# Output Format
Output the anomaly detection results for each task subset in JSON format. Each task subset contains the following three parts:
A. Task Subset Name: The name of the task subset T, used to identify the task subset of the current anomaly detection task.
B. Anomaly Detection Result: A concise and clear answer to "Is an anomaly present?" (Answer: `Yes/No`).
C. Explanation: Provide a detailed and logically clear explanation of the main basis for your judgment of the presence or absence of an anomaly. For example: Which metrics/logs/tracking data showed significant deviations? What is the degree of anomaly?：
{
    "SubTask_0":{
        "name": "T0",
        "is_anomaly": "Yes/No",
        "explaination: ""
    }
    ...
}
"""
)

ft_agent = AssistantAgent(
    name="FTAgent",
    description="负责对异常检测结果进行分类，确定故障的大致范围或类型。",
    model_client=model_client,
    system_message="""
你是一个专注于微服务智能运维中故障分类 (Fault Triage, FT) 的专业智能体。你是诊断链条的第二步执行者，并依赖上游结果。核心职责与工作流：
1.  接收输入：接收：    
    -   来自 Orchestration智能体的任务子集T，其中包含根因子集C与信息子集I。    
    -   来自 AD智能体 的“存在异常”确认和其异常解释说明。
2.  故障分类：为每一个任务子集执行一次故障分类，基于每个任务子集中的信息子集I与每个任务子集的异常检测结果，将该任务子集的异常分类为service级别或pod级别或node级别。故障级别分类标准：
        **Node级别故障**: 单个节点的监控指标(kpi_key)（node_cpu_usage_rate,node_filesystem_usage_rate等）对比正常期间,故障期间存在显著异常变化，且该节点上的多个不同服务的Pod均受影响
        **Service级别故障**: 同一服务的多个Pod实例（如emailservice-0, emailservice-1, emailservice-2）都出现相似的异常数据变化，表明服务本身存在问题
        **Pod级别故障**: 单个Pod（如cartservice-0）出现异常数据变化，而同服务的其他Pod（cartservice-1, cartservice-2）及其他Pod正常
        **重要说明**：所有监控指标均为 `kpi_key` 指标（例如 `node_cpu_usage_rate`），请在描述中直接使用这些原始 `kpi_key` 英文指标名，不得使用中文或其他名称。
3.  结果传递：你的分类结果将作为核心线索传递给 RCL智能体，帮助其缩小根因定位的范围。
输出要求 (结构化)：你的输出必须是每个任务子集的故障分类结果，每个任务子集包含以下三个部分：
    1.  任务子集名：任务子集T的名称，用于标识当前故障分类的任务子集。
    2.  故障分类结果 ：明确给出本次异常事件所属的故障类别名称。
    3.  解释说明 ：提供详细且逻辑清晰的解释，说明你判定为该类别故障的主要依据。如：
    {
        "SubTask_0":{
            "name": "T0",
            "fault_level": "service/pod/node",
            "explaination: ""
        }
        ...
    }
"""
)

# all_node_names = ['aiops-k8s-01', 'aiops-k8s-02', 'aiops-k8s-03', 'aiops-k8s-04',
#                     'aiops-k8s-05', 'aiops-k8s-06', 'aiops-k8s-07', 'aiops-k8s-08']

# all_service_names = ['cartservice', 'currencyservice', 'frontend', 'adservice',
#                         'recommendationservice', 'shippingservice', 'checkoutservice',
#                         'paymentservice', 'emailservice', 'redis-cart', 'productcatalogservice', 'tidb-tidb', 'tidb-pd', 'tidb-tikv']

# all_pod_names = ['cartservice-0', 'cartservice-1', 'cartservice-2', 'currencyservice-0',
#                     'currencyservice-1', 'currencyservice-2', 'frontend-0', 'frontend-1',
#                     'frontend-2', 'adservice-0', 'adservice-1', 'adservice-2',
#                     'recommendationservice-0', 'recommendationservice-1', 'recommendationservice-2',
#                     'shippingservice-0', 'shippingservice-1', 'shippingservice-2',
#                     'checkoutservice-0', 'checkoutservice-1', 'checkoutservice-2',
#                     'paymentservice-0', 'paymentservice-1', 'paymentservice-2',
#                     'emailservice-0', 'emailservice-1', 'emailservice-2',
#                     'productcatalogservice-0', 'productcatalogservice-1', 'productcatalogservice-2',
#                     'redis-cart-0']

# components_list = []
# components_list.extend(all_node_names)
# components_list.extend(all_service_names)
# components_list.extend(all_pod_names)

rcl_agent = AssistantAgent(
    name="RCLAgent",
    description="负责接收故障分类结果，并结合所有提炼数据，精确锁定导致故障发生的具体微服务组件、资源或配置",
    model_client=model_client,
    system_message="""
你是一个专注于微服务智能运维中根因定位 (Root Cause Localization, RCL) 的专业智能体。你是诊断链条的最后一步执行者，负责给出最终的诊断结论。核心职责与工作流：
1.  接收输入：接收：    
    -   来自 Orchestration智能体的任务子集T，其中包含根因子集C与信息子集I。    
    -   来自 FT智能体 的故障分类结果和其解释说明。
2.  根因定位：为每一个任务子集执行一次根因定位，基于每个任务子集的信息子集I与每个任务子集的故障类别，精确识别该任务子集中导致该异常发生的微服务组件名称，要求：
    -   根因组件必须来自任务子集下的根因子集C。
    -   根因组件必须与FT智能体的故障分类结果{{fault_level}}一致，如果fault_level为service，则根因组件必须是某个service，而不能是该service下的pod；如果fault_level为pod，则根因组件必须是某个service下的pod，而不能是service；如果fault_level为node，则根因组件必须是某个node。
        如：fault_level = service, 根因组件为adservice-0，这是错的，根因组件应为adservice。
3.  输出要求 (结构化)：你的输出必须是每个任务子集的根因定位结果，每个任务子集包含以下三个部分：
    1.  任务子集名：任务子集T的名称，用于标识当前根因定位的任务子集。
    2.  根因组件名称 ：明确给出导致异常发生的根因组件,候选组件必须来自于根因子集C。
    2.  解释说明 ：提供详细且逻辑清晰的解释，说明你定位到该组件的关键原因。例如：该组件的哪些Metrics/Logs/Traces数据直接关联了故障？为什么排除了其他组件？如：
    {
        "SubTask_0":{
            "name": "T0",
            "component": "The component/service name, pod name, or node name where the fault is located.",
            "explaination: ""
        }
        ...
    }
### 微服务架构调用关系图谱
理解以下关键调用路径有助于识别故障传播和根因定位：

**主要调用路径:**
1. **用户请求入口**: User → frontend (所有用户请求的统一入口)
2. **购物核心流程**: frontend → checkoutservice → (paymentservice, emailservice, shippingservice, currencyservice)
3. **商品浏览相关**: frontend → (adservice, recommendationservice, productcatalogservice, cartservice)
4. **服务间依赖**: recommendationservice → productcatalogservice (推荐依赖商品目录)
5. **数据存储层**:
    - adservice/productcatalogservice → tidb (广告和商品数据存储)
    - cartservice → redis-cart (购物车缓存)
    - tidb 集群内部: tidb → (tidb-tidb, tidb-tikv, tidb-pd)
"""
)

judge_agent = AssistantAgent(
    name="JudgeAgent",
    description="负责对整个故障诊断流程（AD、FT、RCL）的最终结果进行总结，生成最终的诊断结论。",
    model_client=model_client,
    system_message=f"""
    你是一个在微服务智能运维系统中扮演结果总结与最终输出角色的专业智能体。 核心职责与工作流：
    1. 接收输入：接收来自 Reflection智能体的输出
        - RCL的根因定位结果及其解释说明。 
    2. 结果总结（核心任务）：以一个简洁、准确的json格式输出，不要包含任何其他解释或文本。 
    3. 输出格式必须是json格式，只能是英文，中文是被禁止的：
        The JSON output must be fully in English. Any Chinese characters are strictly prohibited.
                **Strictly follow the JSON format below**：
            {{
                "component": "The component/service name, pod name, or node name where the fault is located.",
                "reason": "Most likely root cause based on comprehensive multi-modal analysis; (must include kpi_key for metrics. (Do not infer from missing data.))",
                "reasoning_trace": [
                    {{
                        "step": 1,
                        "action": "Such as: LoadMetrics(checkoutservice)",
                        "observation": "Describe (≤20 words) the most critical anomaly in metric modality, must include exact kpi_key and change (e.g., '`node_cpu_usage_rate` increased 35% at 12:18 in metric')"
                    }},
                    {{
                        "step": 2,
                        "action": "Such as: TraceAnalysis('frontend-1 -> checkoutservice-2')", 
                        "observation": "Describe (≤20 words) the most critical abnormal behavior in trace modality, include trace path and anomaly type (caller/callee/self-loop) (e.g., 'self-loop detected in `frontend -> checkoutservice` in trace')"
                    }},
                    {{
                        "step": 3,
                        "action": "Such as: LogSearch(checkoutservice)",
                        "observation": "Describe (≤20 words) the most critical anomaly in log modality, mention error keyword and count/context (e.g., 'IOError found in 3 entries in log')"
                    }}
                ]
            }} 
    """
)

# reflection_agent = AssistantAgent(
#     name="ReflectionAgent",
#     description="负责对整个故障诊断流程（AD、FT、RCL）的最终结果进行反思、评估和总结，并将优化建议反馈给 Orchestration 智能体。",
#     model_client=model_client,
#     system_message="""
#     你是一个在微服务智能运维系统中扮演结果校验角色的专业智能体。你的核心目标是克服大模型可能存在的“幻觉”缺陷，确保整个诊断流程（AD、FT、RCL）的最终输出是准确、一致且逻辑连贯的。 核心职责与工作流：
#     1. 接收输入：接收来自 AD智能体、FT智能体和 RCL智能体的所有输出，包括： 
#         - AD的异常检测结果。 
#         - FT的故障分类结果。 
#         - RCL的根因定位结果。 
#     2. 可信度评分：
#         根据AD智能体、FT智能体和 RCL智能体的所有输出，为每一个任务子集进行一次可信度评分。
#         评分标准：
#         - 高可信度：解释说明中包含了当前任务子集的所有相关信息，所有解释说明都高度一致，逻辑连贯，符合基本的运维常识和系统行为。
#         - 中可信度：解释说明中包含了当前任务子集的部分相关信息，在一定程度上一致，逻辑合理，但可能存在一些小的不一致或逻辑缺陷。
#         - 低可信度：解释说明中包含了当前任务子集的少许相关信息，但在明显的不一致或逻辑不合理，不建议作为最终诊断结果。
#     3. 输出要求： 从所有任务子集中选取可信度评分最高的一项，将其输出给summarization智能体，输出内容原封不动的保留RCL智能体的输出中对应该项任务子集的内容。 
#     """
# )

# summarization_agent = AssistantAgent(
#     name="summarizationAgent",
#     description="负责对整个故障诊断流程（AD、FT、RCL）的最终结果进行总结，生成最终的诊断结论。",
#     model_client=model_client,
#     system_message=f"""
#     你是一个在微服务智能运维系统中扮演结果总结与最终输出角色的专业智能体。 核心职责与工作流：
#     1. 接收输入：接收来自 Reflection智能体的输出
#         - RCL的根因定位结果及其解释说明。 
#     2. 结果总结（核心任务）：以一个简洁、准确的json格式输出，不要包含任何其他解释或文本。 
#     3. 输出格式必须是json格式，只能是英文，中文是被禁止的：
#         The JSON output must be fully in English. Any Chinese characters are strictly prohibited.
#                 **Strictly follow the JSON format below**：
#             {{
#                 "component": "The component/service name, pod name, or node name where the fault is located.",
#                 "reason": "Most likely root cause based on comprehensive multi-modal analysis; (must include kpi_key for metrics. (Do not infer from missing data.))",
#                 "reasoning_trace": [
#                     {{
#                         "step": 1,
#                         "action": "Such as: LoadMetrics(checkoutservice)",
#                         "observation": "Describe (≤20 words) the most critical anomaly in metric modality, must include exact kpi_key and change (e.g., '`node_cpu_usage_rate` increased 35% at 12:18 in metric')"
#                     }},
#                     {{
#                         "step": 2,
#                         "action": "Such as: TraceAnalysis('frontend-1 -> checkoutservice-2')", 
#                         "observation": "Describe (≤20 words) the most critical abnormal behavior in trace modality, include trace path and anomaly type (caller/callee/self-loop) (e.g., 'self-loop detected in `frontend -> checkoutservice` in trace')"
#                     }},
#                     {{
#                         "step": 3,
#                         "action": "Such as: LogSearch(checkoutservice)",
#                         "observation": "Describe (≤20 words) the most critical anomaly in log modality, mention error keyword and count/context (e.g., 'IOError found in 3 entries in log')"
#                     }}
#                 ]
#             }} 
#     """
# )