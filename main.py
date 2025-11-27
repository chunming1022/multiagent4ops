import os
import re
import pandas as pd
from typing import Sequence, OrderedDict
import asyncio
import json

from dataRefinement.log_refinement import log_refinement
from dataRefinement.trace_refinement import trace_refinement
from dataRefinement.metric_refinement import metric_refinement

from agent.agent import *
from agent.prompts import *
from autogen_agentchat.teams import DiGraphBuilder, GraphFlow
from autogen_agentchat.conditions import TextMentionTermination, MaxMessageTermination
from autogen_agentchat.ui import Console
from autogen_agentchat.messages import AgentEvent, ChatMessage


project_root = os.path.dirname(os.path.abspath(__file__))

max_messages_termination = MaxMessageTermination(max_messages=20)
termination = max_messages_termination


async def main():
    input_path = os.path.join(project_root, 'input', 'input_timestamp.csv')
    df_input_timestamp = pd.read_csv(input_path, encoding='utf-8')
    
    for index, row in df_input_timestamp.iterrows():
        if index < 154:
            continue
        if index == 211:
            break
        print(">>" * 100)
        print(f"index: {index}")

        start_timestamp = row['start_timestamp']
        end_timestamp = row['end_timestamp']
        start_time_hour = row['start_time_hour']
        uuid = row['uuid']
    
        refined_logs, candidates_from_log = log_refinement(start_time_hour, start_timestamp, end_timestamp)
        # print(f"refined_logs: {refined_logs}\n\ncandidates_from_log: {json.dumps(candidates_from_log, indent = 4, ensure_ascii=False)}")
        # exit(0)
        if refined_logs is not None:
            print('//' * 20)
            log_prompt = get_log_refinement_prompt(refined_logs)
            refined_logs = await logs_agent.run(task=log_prompt)
            refined_logs = refined_logs.messages[-1].content
        else:
            print('log为空')
        print('logs refinement completed!')

        latency_traces, candidates_from_trace, status_traces = trace_refinement(start_time_hour, start_timestamp, end_timestamp)
        # print(f"latency_traces: {latency_traces}\n\nstatus_traces: {status_traces}\n\ncandidates_from_trace: {json.dumps(candidates_from_trace, indent = 4, ensure_ascii=False)}" )
        # exit(0)
        if latency_traces is not None or status_traces is not None:
            print('//' * 20)
            trace_prompt = get_trace_refinement_prompt(latency_traces, status_traces)
            refined_traces = await traces_agent.run(task=trace_prompt)
            refined_traces = refined_traces.messages[-1].content
        else:
            print('trace为空')
            refined_traces = None
        print('traces refinement completed!')

        refined_metrics, candidates_from_metric, pods_on_node = await metric_refinement(df_input_timestamp, index, start_timestamp, end_timestamp)
        print('//' * 20)
        print('metrics refinement completed!')

        candidates = {
            'candidate_services': [],
            'candidate_pods': [],
            'candidate_nodes': [],
        }
        candidate_nodes = []
        candidate_services = []
        candidate_pods = []

        candidate_nodes.extend(candidates_from_metric['candidate_nodes'])
        candidate_nodes.extend(candidates_from_trace['candidate_nodes'])
        candidate_nodes.extend(candidates_from_log['candidate_nodes'])
        
        candidate_services.extend(candidates_from_metric['candidate_services'])
        candidate_services.extend(candidates_from_trace['candidate_services'])
        candidate_services.extend(candidates_from_log['candidate_services'])

        candidate_pods.extend(candidates_from_metric['candidate_pods'])
        candidate_pods.extend(candidates_from_trace['candidate_pods'])
        candidate_pods.extend(candidates_from_log['candidate_pods'])
    
        candidate_nodes = list(set(candidate_nodes))
        candidate_services = list(set(candidate_services))
        candidate_pods = list(set(candidate_pods))

        new_services = []
        for service in candidate_services:
            match_services = []
            for index, pod in enumerate(candidate_pods):
                if pod.startswith(service):
                    match_services.append(pod)
            match_count = len(match_services)
            if match_count == 3:
                new_services.append(service)
                # for pod in match_services:
                #     candidate_pods.remove(pod)
            else:
                pass

        candidates['candidate_nodes'] = candidate_nodes
        candidates['candidate_services'] = new_services
        candidates['candidate_pods'] = candidate_pods

        # json.dump(candidates, open(f'output/candidates.json', 'w'), ensure_ascii=False, indent=4)

        # with open(f'output/refined_traces.txt', 'r', encoding='utf-8') as f:
        #     refined_traces = f.read()

        # with open(f'output/refined_logs.txt', 'r', encoding='utf-8') as f:
        #     refined_logs = f.read()

        # refined_metrics = json.load(open(f'output/refined_metrics.json', 'r', encoding='utf-8'))

        # candidates = json.load(open(f'output/candidates.json', 'r', encoding='utf-8'))

        multimodal_prompt = get_multimodal_analysis_prompt(
            candidates=candidates,
            pods_on_node = pods_on_node,
            metric_data=refined_metrics,
            log_data=refined_logs,
            trace_data=refined_traces,
        )
        # print(uuid)
        # print(multimodal_prompt)

        builder = DiGraphBuilder()
        builder.add_node(orchestration_agent)
        builder.add_node(ad_agent).add_node(ft_agent).add_node(rcl_agent)
        builder.add_node(judge_agent)

        builder.add_edge(orchestration_agent, ad_agent)
        builder.add_edge(orchestration_agent, ft_agent).add_edge(ad_agent, ft_agent)
        builder.add_edge(orchestration_agent, rcl_agent).add_edge(ft_agent, rcl_agent)
        builder.add_edge(rcl_agent, judge_agent)
        # builder.add_edge(ad_agent, reflection_agent).add_edge(ft_agent, reflection_agent).add_edge(rcl_agent, reflection_agent)
        # builder.add_edge(reflection_agent, orchestration_agent, condition=lambda msg: "APPROVE" not in msg.to_model_text())
        # builder.add_edge(reflection_agent, summarization_agent, condition=lambda msg: "APPROVE" in msg.to_model_text())
        # builder.add_edge(reflection_agent, summarization_agent)

        builder.set_entry_point(orchestration_agent)
        graph = builder.build()

        team = GraphFlow(
            participants = [orchestration_agent, ad_agent, ft_agent, rcl_agent, judge_agent],
            graph = graph,
            termination_condition=termination,
        )

#         multimodal_prompt = """
# ### Language Enforcement
# -Input may contain Chinese, **but output MUST be entirely in English** (no Chinese characters).
# 请根据提供的链路追踪数据、系统指标数据，进行综合故障分析，完成异常检测、故障分类、根因定位。
# 特别注意**缺失数据和空数据,代表数据波动极小,通常情况默认正常**
# 可用的监控数据:

# ### 微服务链路异常数据:
# ["checkoutservice服务在aiops-k8s-06节点上的checkoutservice-2实例调用hipstershop.EmailService/SendOrderConfirmation接口时，平均耗时从正常时期的18785.58纳秒异常升高至155081.47纳秒，该异常缓慢操作共出现36次。", "checkoutservice服务在aiops-k8s-06节点上的checkoutservice-2实例接收来自frontend-0的hipstershop.CheckoutService/PlaceOrder请求时，平均耗时从正常时期的134091.66纳秒异常升高至314446.04纳秒，该异常缓慢操作共出现20次。", "frontend服务在aiops-k8s-03节点上的frontend-0实例调用hipstershop.CheckoutService/PlaceOrder接口时，平均耗时从正常时期的169075.70纳秒异常升高至329035.74纳秒，该异常缓慢操作共出现17次。", "checkoutservice服务在aiops-k8s-06节点上的checkoutservice-2实例接收来自frontend-1的hipstershop.CheckoutService/PlaceOrder请求时，平均 耗时从正常时期的138021.87纳秒异常升高至307370.28纳秒，该异常缓慢操作共出现16次。", "frontend服务在aiops-k8s-07节点上的frontend-1实例调用hipstershop.CheckoutService/PlaceOrder接口时，平均耗时从正常时期的143008.62纳秒异常升高至311219.50纳秒，该异常缓慢操作共出现16次。", "checkoutservice服务在aiops-k8s-06节点上的checkoutservice-2实例接收来自frontend-2的hipstershop.CheckoutService/PlaceOrder请求时，平均耗时从正常时期的141935.04纳秒异常升高至307651.53纳秒，该异常缓慢操作共出现16次。", "frontend服务在aiops-k8s-07节点上的frontend-1实例调用hipstershop.ProductCatalogService/GetProduct接口时，平均耗时从正常时期的11120.96纳秒异常升高至13406.49纳秒，该异常缓慢操作共出现15次。", "frontend服务在aiops-k8s-04节点上的frontend-2实例调用hipstershop.CheckoutService/PlaceOrder接口时，平均耗时从正常时期的145817.93纳秒异常升高至320778.29纳秒，该异常缓慢操作共出现14次。", "frontend服务在aiops-k8s-03节点上的frontend-0实例调用hipstershop.ProductCatalogService/GetProduct接口时，平均耗时从正常时期的11225.94纳秒异常升高至13556.75纳秒，该异常缓慢操作共出现8次。", "emailservice服务在aiops-k8s-07节点上的emailservice-0实例调用/hipstershop.EmailService/SendOrderConfirmation接口时，平均耗时从正常时期的682.77纳秒异常升高至55669.43纳秒，该异常缓慢操作共出现7次。", "emailservice服务在aiops-k8s-06节点上的emailservice-2实例调用/hipstershop.EmailService/SendOrderConfirmation接口时，平均耗时从正常时期的680.04纳秒异常升高至81240.43纳秒，该异常缓慢操作共出现7次。", "productcatalogservice服务在aiops-k8s-05节点上的productcatalogservice-2实例调用hipstershop.ProductCatalogService/GetProduct接口时，平均耗时从正常时期的10544.44纳秒异 常升高至11972.01纳秒，该异常缓慢操作共出现6次。", "recommendationservice服务在aiops-k8s-03节点上的recommendationservice-0实 例调用/hipstershop.RecommendationService/ListRecommendations接口时，平均耗时从正常时期的4089.12纳秒异常升高至4811.59纳秒，该异常缓慢操作共出现6次。", "recommendationservice服务在aiops-k8s-03节点上的recommendationservice-0实例调用/hipstershop.ProductCatalogService/ListProducts接口时，平均耗时从正常时期的2958.26纳秒异常升高至3575.78纳秒，该异常缓慢操作共出现5次。", "productcatalogservice服务在aiops-k8s-05节点上的productcatalogservice-2实例调用hipstershop.ProductCatalogService/GetProduct接口时 ，平均耗时从正常时期的10813.35纳秒异常升高至16273.10纳秒，该异常缓慢操作共出现5次。", "frontend服务在aiops-k8s-04节点上的frontend-2实例调用hipstershop.ProductCatalogService/GetProduct接口时，平均耗时从正常时期的11231.27纳秒异常升高至13413.11纳秒， 该异常缓慢操作共出现4次。", "emailservice服务在aiops-k8s-01节点上的emailservice-1实例调用/hipstershop.EmailService/SendOrderConfirmation接口时，平均耗时从正常时期的687.56纳秒异常升高至72481.25纳秒，该异常缓慢操作共出现4次。", "productcatalogservice服务在aiops-k8s-01节点上的productcatalogservice-0实例调用hipstershop.ProductCatalogService/GetProduct接口时，平均耗时从正常 时期的10042.77纳秒异常升高至17495.67纳秒，该异常缓慢操作共出现3次。", "cartservice服务在aiops-k8s-08节点上的cartservice-1实 例处理POST /hipstershop.CartService/GetCart请求时，平均耗时从正常时期的1980.07纳秒异常升高至8644.35纳秒，该异常缓慢操作共出 现3次。", "productcatalogservice服务在aiops-k8s-06节点上的productcatalogservice-1实例调用hipstershop.ProductCatalogService/ListProducts接口时，平均耗时从正常时期的74.82纳秒异常升高至125.78纳秒，该异常缓慢操作共出现2次。"]

# ### 微服务指标异常数据:
# {"adservice": {"adservice-0": {"client_error_ratio": "异常升高", "error_ratio": "异常升高"}}, "cartservice": {"cartservice-0": {"rrt": "显著增加"}, "cartservice-1": {"rrt": "显著增加", "timeout": "非零值升高"}}, "checkoutservice": {"checkoutservice-2": {"rrt": "显著增加"}}, "currencyservice": {"currencyservice-1": {"rrt": "显著增加", "request": "突然降低", "response": "突然降低"}, "currencyservice-2": {"rrt": "显著增加"}}, "emailservice": {"emailservice-0": {"rrt": "显著增加"}, "emailservice-1": {"rrt": "显著增加"}, "emailservice-2": {"rrt": "显著增加"}}, "frontend": {"frontend-0": {"rrt": "显著增加"}, "frontend-1": {"rrt": "显著增加"}, "frontend-2": {"rrt": "显著增加"}}, "productcatalogservice": {"productcatalogservice-1": {"rrt": " 显著增加"}, "productcatalogservice-2": {"rrt": "显著增加"}}, "recommendationservice": {"recommendationservice-0": {"rrt": " 显著增加"}}, "redis-cart": {"redis-cart-0": {"rrt": "显著增加", "timeout": "非零值升高"}}}

# ### TiDB服务指标异常数据:
# {"tidb-tidb": {"duration_99th": "升高", "connection_count": "升高"}, "tidb-tikv": {"cpu_usage": "异常升高", "available_size": "异常下降"}}

# ### 节点指标异常数据:
# {"aiops-k8s-01": {"node_cpu_usage_rate": "升高"}, "aiops-k8s-02": {"node_disk_written_bytes_total": "异常升高"}, "aiops-k8s-03": {"node_memory_usage_rate": "升高", "node_network_receive_bytes_total": "升高", "node_network_receive_packets_total": " 升高", "node_network_transmit_bytes_total": "升高", "node_network_transmit_packets_total": "升高"}, "aiops-k8s-04": {"node_memory_usage_rate": "异常升高", "node_disk_written_bytes_total": "异常升高"}, "aiops-k8s-05": {"node_disk_read_bytes_total": "异常升高", "node_disk_written_bytes_total": "异常升高"}, "aiops-k8s-06": {"node_cpu_usage_rate": "升高", "node_memory_usage_rate": "升高", "node_disk_written_bytes_total": "升高"}, "aiops-k8s-07": {"node_cpu_usage_rate": "异常升高", "node_memory_usage_rate": "异常降低", "node_network_receive_bytes_total": "异常降低", "node_network_receive_packets_total": "异常降低", "node_network_transmit_bytes_total": "异常降低", "node_network_transmit_packets_total": "异常降低"}, "aiops-k8s-08": {"node_memory_usage_rate": "异常升高"}}

# ### Pod指标异常数据:
# {"aiops-k8s-01": {"emailservice-1": {"pod_cpu_usage": "异常升高", "pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高", "pod_processes": "异常升高"}, "productcatalogservice-0": {"pod_memory_working_set_bytes": " 异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "shippingservice-1": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}}, "aiops-k8s-03": {"adservice-0": {"pod_memory_working_set_bytes": "异常升高"}, "cartservice-2": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "frontend-0": {"pod_cpu_usage": "降低", "pod_memory_working_set_bytes": "降低", "pod_network_receive_bytes": "升高", "pod_network_receive_packets": "升高", "pod_network_transmit_bytes": "升高", "pod_network_transmit_packets": "升高"}, "paymentservice-1": {"pod_memory_working_set_bytes": "异常降低", "pod_network_receive_bytes": "异常降低", "pod_network_receive_packets": "异常降低", "pod_network_transmit_bytes": "异常降低", "pod_network_transmit_packets": "异常降低"}, "recommendationservice-0": {"pod_memory_working_set_bytes": "降低", "pod_network_receive_packets": "升高", "pod_network_transmit_packets": "升高"}}, "aiops-k8s-04": {"currencyservice-1": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "frontend-2": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}}, "aiops-k8s-05": {"currencyservice-0": {"pod_cpu_usage": "异常升高", "pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": " 异常升高"}, "paymentservice-2": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "productcatalogservice-2": {"pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}}, "aiops-k8s-06": {"checkoutservice-2": {"pod_cpu_usage": "异常升高", "pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "emailservice-2": {"pod_cpu_usage": "异常升高", "pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高", "pod_processes": "异常升高"}, "productcatalogservice-1": {"pod_memory_working_set_bytes": "异常升高"}}, "aiops-k8s-07": {"adservice-1": {"pod_fs_writes_bytes": "异常升高", "pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": " 异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "currencyservice-2": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "emailservice-0": {"pod_cpu_usage": "异常升高", "pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高", "pod_processes": "异常升高"}, "frontend-1": {"pod_memory_working_set_bytes": "异常升高"}, "shippingservice-0": {"pod_memory_working_set_bytes": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}}, "aiops-k8s-08": {"adservice-2": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": " 异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "cartservice-1": {"pod_memory_working_set_bytes": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "paymentservice-0": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异常升高"}, "redis-cart-0": {"pod_cpu_usage": "升高", "pod_memory_working_set_bytes": "升高", "pod_fs_writes_bytes": "升高"}, "shippingservice-2": {"pod_memory_working_set_bytes": "异常升高", "pod_network_receive_bytes": "异常升高", "pod_network_receive_packets": "异常升高", "pod_network_transmit_bytes": "异常升高", "pod_network_transmit_packets": "异 常升高"}}}
# node上部署的pod实例：
# {'aiops-k8s-01': ['checkoutservice-1', 'emailservice-1', 'productcatalogservice-0', 'productcatalogservice-1', 'recommendationservice-1', 'recommendationservice-2', 'shippingservice-0', 'shippingservice-1'], 'aiops-k8s-03': ['adservice-0', 'cartservice-2', 'frontend-0', 'paymentservice-1', 'productcatalogservice-2', 'recommendationservice-0', 'recommendationservice-1'], 'aiops-k8s-04': ['checkoutservice-0', 'currencyservice-1', 'frontend-2'], 'aiops-k8s-05': ['currencyservice-0', 'paymentservice-2', 'productcatalogservice-2'], 'aiops-k8s-06': ['checkoutservice-2', 'emailservice-2', 'productcatalogservice-0', 'productcatalogservice-1'], 'aiops-k8s-07': ['adservice-1', 'cartservice-0', 'currencyservice-2', 'emailservice-0', 'frontend-1', 'shippingservice-0', 'shippingservice-2'], 'aiops-k8s-08': ['adservice-2', 'cartservice-1', 'paymentservice-0', 'recommendationservice-0', 'recommendationservice-2', 'redis-cart-0', 'shippingservice-1', 'shippingservice-2']}
# 候选根因组件:
# ['productcatalogservice', 'emailservice', 'frontend', 'currencyservice', 'cartservice', 'adservice', 'emailservice-2', 'shippingservice-1', 'adservice-1', 'productcatalogservice-0', 'currencyservice-2', 'cartservice-2', 'redis-cart-0', 'adservice-0', 'paymentservice-2', 'frontend-1', 'frontend-0', 'emailservice-0', 'emailservice-1', 'shippingservice-0', 'frontend-2', 'currencyservice-0', 'paymentservice-1', 'productcatalogservice-2', 'paymentservice-0', 'recommendationservice-0', 'shippingservice-2', 'adservice-2', 'checkoutservice-2', 'currencyservice-1', 'cartservice-0', 'cartservice-1', 'productcatalogservice-1', 'aiops-k8s-05', 'aiops-k8s-08', 'aiops-k8s-07', 'aiops-k8s-02', 'aiops-k8s-01', 'aiops-k8s-04', 'aiops-k8s-03', 'aiops-k8s-06']        
# """
        await team.reset()
        # await Console(team.run_stream(task=f"{multimodal_prompt}"))
        # exit(0)

        respose = await team.run(task=f"{multimodal_prompt}")


        result = re.search(r'(\{.*\})', respose.messages[-1].content, re.DOTALL)
        if result:
            result = result.group(1)
        else:
            result = None
    
        json_result = json.loads(result)
        result_data = OrderedDict()
        result_data["component"] = json_result.get("component", "")
        result_data["uuid"] = uuid
        result_data["reason"] = json_result.get("reason", "")
        result_data["reasoning_trace"] = json_result.get("reasoning_trace", [])

        result_list_path = os.path.join(project_root, 'output', 'results_list.json')
        with open(result_list_path, 'a', encoding='utf-8') as f:
            json.dump(result_data, f)   
            f.write('\n')   
        print(f"第{index+1}条数据处理完成")
        # if counter != 0:
        #     counter += 1
        #     continue
        # break

        print("<<" * 100)


if __name__ == "__main__":
    asyncio.run(main())