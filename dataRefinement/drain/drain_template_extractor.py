import os
from tqdm import tqdm
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

def init_drain():
    """
    初始化Drain3模板提取器
    
    返回:
        TemplateMiner: 初始化后的Drain3模板提取器
    """
    config = TemplateMinerConfig()
    config_pth = os.path.join(os.path.dirname(__file__), 'drain3.ini')
    config.load(config_filename=config_pth)
    config.profiling_enabled = True

    return TemplateMiner(config=config)

def extract_templates(log_list: list[str]):
    """
    从日志列表中提取模板并将drain模型保存到指定路径
    
    参数:
        log_list: 包含日志消息的列表
        save_path: 保存drain模型的文件路径
    # """
    # KEEP_TOP_N_TEMPLATE = 1000 #出现次数在前1000的模板

    miner = init_drain()
    for line in tqdm(log_list):
        log_txt = line.rstrip()
        miner.add_log_message(log_txt)

    template_count = len(miner.drain.clusters)
    print(f'The number of templates: {template_count}')

    return miner