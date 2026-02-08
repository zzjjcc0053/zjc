


import pandas as pd
import numpy as np
import requests
import cookie
import requests
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Any
import warnings
from datetime import datetime
warnings.filterwarnings('ignore')
# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue

from sqlalchemy import create_engine
import pymysql

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:sktt1faker@localhost:3306/创作者中心')

# 插入DataFrame
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time


# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 代理池类
class ProxyPool:
    def __init__(self, max_proxies=10, refresh_interval=50):  # 50秒刷新一次
        self.proxy_pool = Queue()
        self.max_proxies = max_proxies
        self.refresh_interval = refresh_interval
        self.last_refresh = 0
        self.lock = threading.Lock()
        
        # 初始填充代理池
        self._fill_proxy_pool()
        
        # 启动定期刷新线程
        self.refresh_thread = threading.Thread(target=self._auto_refresh, daemon=True)
        self.refresh_thread.start()
    
    
    def _fill_proxy_pool(self):
        """填充代理池（控制频率）"""
        with self.lock:
            current_time = time.time()
            # 检查是否达到刷新间隔
            if current_time - self.last_refresh < 60:
                return
                
            logger.info("刷新代理池...")
            new_proxies = []
            count = 0
            response = requests.get('https://dps.kdlapi.com/api/getdps/?secret_id=okmoku6vzz5m1yje4r66&signature=h5emxqufkga5m8orfz23ukoe7dlsto6c&num='+str(self.max_proxies)+'&format=json&sep=1&dedup=1').json()
            print(response)
            for i in range(len(response.get('data')['proxy_list'])):
                new_proxies.append({
                    "server": response.get('data')['proxy_list'][i],
                    "timestamp": current_time
                })

            print(new_proxies)
            
            # 清空旧代理，添加新代理
            while not self.proxy_pool.empty():
                try:
                    self.proxy_pool.get_nowait()
                except:
                    break
            
            for proxy in new_proxies:
                self.proxy_pool.put(proxy)
            
            self.last_refresh = current_time
            logger.info(f"代理池已刷新，当前大小: {self.proxy_pool.qsize()}")
    
    def get_proxy(self):
        """从池中获取一个代理"""
        # 如果池子空了，尝试填充
        if self.proxy_pool.empty():
            self._fill_proxy_pool()
        
        try:
            while True:
                proxy = self.proxy_pool.get()
                # 检查代理是否过期（超过50秒）
                if time.time() - proxy.get("timestamp", 0) < 50:
                    return {
                        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": 'd3185915467', "pwd": 'ghdxz57s', "proxy":proxy["server"]},
                        "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": 'd3185915467', "pwd": 'ghdxz57s', "proxy":proxy["server"]}
                    }
                    return {"https":"http://"+proxy["server"],'http:':"http://"+proxy["server"]
                            }
                else:
                    logger.debug(f"代理已过期，丢弃: {proxy}")
        except Exception as e:
            logger.error(f"获取代理失败: {e}")
            return None
    
    def _auto_refresh(self):
        """自动刷新代理池"""
        while True:
            time.sleep(self.refresh_interval)
            self._fill_proxy_pool()

# 使用代理池
# proxy_pool = ProxyPool(max_proxies=50, refresh_interval=45)

