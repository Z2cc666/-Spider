#!/home/inspur/anaconda3/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
import argparse

class WeiboDailySpider:
    def __init__(self, user_id, cookie=None, target_date=None):
        self.user_id = user_id
        self.cookie = cookie
        self.target_date = target_date if target_date else datetime.now().date()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cookie': cookie if cookie else ''
        }
        self.base_dir = f'南网50Hz微博/南网50Hz微博_{self.target_date.strftime("%Y%m%d")}'
        os.makedirs(self.base_dir, exist_ok=True)

    def parse_weibo_date(self, date_str):
        """解析微博日期字符串为date对象"""
        try:
            return datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y').date()
        except:
            return None

    def get_daily_weibo(self, max_pages=50):
        """获取指定日期的微博数据"""
        all_data = []
        stop_flag = False
        
        for page in range(1, max_pages + 1):
            if stop_flag:
                break
                
            url = f"https://m.weibo.cn/api/container/getIndex?containerid=107603{self.user_id}&page={page}"
            try:
                response = requests.get(url, headers=self.headers, timeout=15)
                data = response.json()
                
                if data.get('ok') != 1:
                    break

                for card in data['data']['cards']:
                    if 'mblog' not in card:
                        continue
                        
                    mblog = card['mblog']
                    weibo_date = self.parse_weibo_date(mblog['created_at'])
                    
                    # 遇到早于目标日期的微博就停止
                    if not weibo_date or weibo_date < self.target_date:
                        stop_flag = True
                        break
                        
                    # 只处理目标日期的微博
                    if weibo_date == self.target_date:
                        item = self.process_weibo(mblog)
                        all_data.append(item)
                        self.save_weibo_item(item)  # 实时保存每条微博

                print(f"已处理第 {page} 页，找到 {len(all_data)} 条目标微博")
                time.sleep(3)  # 降低请求频率

            except Exception as e:
                print(f"第 {page} 页处理异常:", str(e))
                continue
                
        return pd.DataFrame(all_data)

    def process_weibo(self, mblog):
        """处理单条微博数据"""
        weibo_id = mblog['id']
        text = re.sub(r'<[^>]+>', '', mblog['text']).strip()
        
        # 下载图片
        image_paths = []
        if 'pics' in mblog:
            for i, pic in enumerate(mblog['pics'], 1):
                img_url = pic['large']['url']
                img_path = self.download_image(img_url, weibo_id, i)
                if img_path:
                    image_paths.append(img_path)
        
        return {
            'id': weibo_id,
            'text': text,
            'created_at': mblog['created_at'],
            'images': '|'.join(image_paths),
            'reposts': mblog.get('reposts_count', 0),
            'comments': mblog.get('comments_count', 0),
            'likes': mblog.get('attitudes_count', 0)
        }

    def download_image(self, url, weibo_id, index):
        """下载单张图片"""
        try:
            response = requests.get(url, stream=True, timeout=10)
            if response.status_code == 200:
                ext = os.path.splitext(urlparse(url).path)[1] or '.jpg'
                filename = f"{weibo_id}_{index}{ext}"
                img_dir = os.path.join(self.base_dir, 'images')
                os.makedirs(img_dir, exist_ok=True)
                img_path = os.path.join(img_dir, filename)
                
                with open(img_path, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                return img_path
        except Exception as e:
            print(f"图片下载失败 {url}: {str(e)}")
        return None

    def save_weibo_item(self, item):
        """实时保存单条微博"""
        text_dir = os.path.join(self.base_dir, 'text')
        os.makedirs(text_dir, exist_ok=True)
        
        # 保存文本
        with open(f"{text_dir}/{item['id']}.txt", 'w', encoding='utf-8') as f:
            f.write(item['text'])
            
        # 追加到CSV
        csv_path = os.path.join(self.base_dir, 'weibo_data.csv')
        df = pd.DataFrame([item])
        df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, help='目标日期（格式：YYYYMMDD）')
    args = parser.parse_args()

    # 配置参数
    target_date = datetime.strptime(args.date, "%Y%m%d").date() if args.date else datetime.now().date()
    user_id = '2053782235'  # 南网50Hz的用户ID
    cookie = '_T_WM=32753566029; WEIBOCN_FROM=1110006030; SCF=Am0jHTGWTsBEX2NoPa32YdUWRtZHTNm3-xo_fGdQAzpc8YG6LvWSngDIJe7W6bZR1T5AaDX7Uo5s8vexnr1u59E.; SUB=_2A25FVW-KDeRhGe5O61QU9i3NyjWIHXVmK-1CrDV6PUJbktAbLRbEkW1Ndbp7lFPyoYPCz_qY9Km7CZNG3XdyNtpz; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWgqmRcS77POT1jPOuCOOww5NHD95QReh5cSKq0eK24Ws4DqcjMi--NiK.Xi-2Ri--ciKnRi-zN1h57So-ce02p1Btt; SSOLoginState=1750147036; ALF=1752739036; MLOGIN=1; XSRF-TOKEN=126987; mweibo_short_token=146fc5a91a; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1005052053782235; geetest_token=96a62e5583d9e487d9b83bb2910bb9c2'
    
    spider = WeiboDailySpider(user_id, cookie, target_date)
    spider.get_daily_weibo(max_pages=50)  # 最多爬50页
    
    print(f"\n当日微博数据已保存到：{spider.base_dir}")