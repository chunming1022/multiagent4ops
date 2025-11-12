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
from agent.prompts import get_multimodal_analysis_prompt
from autogen_agentchat.teams import SelectorGroupChat, DiGraphBuilder, GraphFlow
from autogen_agentchat.conditions import TextMentionTermination, MaxMessageTermination
from autogen_agentchat.ui import Console
from autogen_agentchat.messages import AgentEvent, ChatMessage


project_root = os.path.dirname(os.path.abspath(__file__))

max_messages_termination = MaxMessageTermination(max_messages=20)
termination = max_messages_termination

# def custom_selector_function(messages: Sequence[AgentEvent | ChatMessage]) -> str | None:
#     if len(messages) <= 1:
#         return orchestration_agent.name
#     elif messages[-1].source == orchestration_agent.name:
#         return ad_agent.name
#     elif messages[-1].source == ad_agent.name:
#         return ft_agent.name
#     elif messages[-1].source == ft_agent.name:
#         return rcl_agent.name
#     elif messages[-1].source == rcl_agent.name:
#         return reflection_agent.name
#     elif messages[-1].source == reflection_agent.name:
#         return orchestration_agent.name

async def main():
    input_path = os.path.join(project_root, 'input', 'input_timestamp.csv')
    df_input_timestamp = pd.read_csv(input_path, encoding='utf-8')
    
    for index, row in df_input_timestamp.iterrows():

        if index < 32:
            continue

        print(">>" * 100)
        print(f"index: {index}")

        start_timestamp = row['start_timestamp']
        end_timestamp = row['end_timestamp']
        start_time_hour = row['start_time_hour']
        uuid = row['uuid']
    
        refined_logs = log_refinement(start_time_hour, start_timestamp, end_timestamp)
        if refined_logs is not None:
            print('//' * 20)
            refined_logs = await logs_agent.run(task=f"请提炼出以下日志中对故障诊断最关键、最有价值的日志：\n{refined_logs}")
            refined_logs = refined_logs.messages[-1].content
        else:
            refined_logs = None
        print('logs refinement completed!')

        refined_traces, trace_unique_dict, status_combinations_csv = trace_refinement(start_time_hour, start_timestamp, end_timestamp)
        if refined_traces is not None or status_combinations_csv is not None:
            print('//' * 20)
            refined_traces = await traces_agent.run(task=f"请提炼出以下trace中对故障诊断最关键、最有价值的traces：\n{refined_traces}\n{status_combinations_csv}")
            refined_traces = refined_traces.messages[-1].content
        else:
            refined_traces = None
        print('traces refinement completed!')

        refined_metrics = await metric_refinement(df_input_timestamp, index, start_timestamp, end_timestamp)
        if refined_metrics is not None:
            print('//' * 20)
            refined_metrics = await metrics_agent.run(task=f"请提炼出以下metrics中对故障诊断最关键、最有价值的metrics：{refined_metrics}")
            refined_metrics = refined_metrics.messages[-1].content
        else:
            refined_metrics = None
            # node_analysis_result = None
        print('metrics refinement completed!')

        multimodal_prompt = get_multimodal_analysis_prompt(
            log_data=refined_logs ,
            trace_data=refined_traces ,
            metric_data=refined_metrics
        )

        builder = DiGraphBuilder()
        builder.add_node(orchestration_agent)
        builder.add_node(ad_agent).add_node(ft_agent).add_node(rcl_agent)
        builder.add_node(reflection_agent).add_node(summarization_agent)

        builder.add_edge(orchestration_agent, ad_agent)
        builder.add_edge(orchestration_agent, ft_agent).add_edge(ad_agent, ft_agent)
        builder.add_edge(orchestration_agent, rcl_agent).add_edge(ft_agent, rcl_agent)
        builder.add_edge(ad_agent, reflection_agent).add_edge(ft_agent, reflection_agent).add_edge(rcl_agent, reflection_agent)
        builder.add_edge(reflection_agent, orchestration_agent, condition=lambda msg: "APPROVE" not in msg.to_model_text())
        builder.add_edge(reflection_agent, summarization_agent, condition=lambda msg: "APPROVE" in msg.to_model_text())

        builder.set_entry_point(orchestration_agent)
        graph = builder.build()

        team = GraphFlow(
            participants = [orchestration_agent, ad_agent, ft_agent, rcl_agent, reflection_agent, summarization_agent],
            graph = graph,
            termination_condition=termination,
        )
        await team.reset()

        # await Console(team.run_stream(task=f"{multimodal_prompt}"))
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

        # groupchat = SelectorGroupChat(
        #     participants=[orchestration_agent, ad_agent, ft_agent, rcl_agent, reflection_agent],
        #     model_client=model_client,
        #     termination_condition=termination,
        #     selector_func=custom_selector_function
        # )
        # groupchat.reset()
        # await Console(groupchat.run_stream(task=f"{multimodal_prompt}"))

        print("<<" * 100)


if __name__ == "__main__":
    asyncio.run(main())