import os
from dotenv import load_dotenv

load_dotenv()  # 这一行负责从.env文件加载环境变量

# 环境变量获取
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

BLOCKCHAIN_API_KEYS = {
    'ETHERSCAN': os.getenv("ETHERSCAN_API_KEY"),
    'BSCSCAN': "your_bscscan_api_key_here",
    'POLYGONSCAN': "your_polygonscan_api_key_here",
    'SNOWTRACE': "your_snowtrace_api_key_here",
    'ARBISCAN': "your_arbiscan_api_key_here",
    'OPTIMISM': "your_optimism_etherscan_api_key_here",
    'FTMSCAN': "your_ftmscan_api_key_here",
}