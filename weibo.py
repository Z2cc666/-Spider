#!/home/inspur/anaconda3/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse

class WeiboSpiderWithImages:
    def __init__(self, user_id, cookie=None):
        self.user_id = user_id
        self.cookie = cookie
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cookie': cookie if cookie else ''
        }
        self.base_dir = f'南网50Hz微博/南网50Hz微博数据_{datetime.now().strftime("%Y%m%d")}'
        self.text_dir = os.path.join(self.base_dir, '文本内容')
        self.image_dir = os.path.join(self.base_dir, '微博图片')
        os.makedirs(self.text_dir, exist_ok=True)
        os.makedirs(self.image_dir, exist_ok=True)

    def clean_text(self, text):
        """清洗微博文本"""
        text = re.sub(r'<[^>]+>', '', text)  # 去除HTML标签
        text = re.sub(r'\s+', ' ', text)      # 合并空白字符
        return text.strip()

    def download_image(self, url, weibo_id, index):
        """下载单张图片"""
        try:
            response = requests.get(url, stream=True, timeout=10)
            if response.status_code == 200:
                # 从URL提取文件扩展名
                parsed = urlparse(url)
                ext = os.path.splitext(parsed.path)[1] or '.jpg'
                
                # 生成文件名：微博ID_序号.扩展名
                filename = f"{weibo_id}_{index}{ext}"
                filepath = os.path.join(self.image_dir, filename)
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                return filepath
        except Exception as e:
            print(f"图片下载失败 {url}: {str(e)}")
        return None

    def get_weibo_data(self, pages=3100):
        """获取微博数据和图片"""
        all_data = []
        for page in range(1, pages + 1):
            url = f"https://m.weibo.cn/api/container/getIndex?containerid=107603{self.user_id}&page={page}"
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.encoding = 'utf-8'
                data = response.json()
                
                if data.get('ok') == 1:
                    for card in data['data']['cards']:
                        if 'mblog' in card:
                            mblog = card['mblog']
                            weibo_id = mblog['id']
                            text = self.clean_text(mblog['text'])
                            
                            # 保存文本到单独文件
                            text_file = os.path.join(self.text_dir, f"{weibo_id}.txt")
                            with open(text_file, 'w', encoding='utf-8') as f:
                                f.write(text)
                            
                            # 下载图片
                            image_urls = []
                            if 'pics' in mblog:
                                for i, pic in enumerate(mblog['pics'], 1):
                                    largest_url = pic['large']['url']
                                    saved_path = self.download_image(largest_url, weibo_id, i)
                                    if saved_path:
                                        image_urls.append(saved_path)
                            
                            all_data.append({
                                'id': weibo_id,
                                'text': text,
                                'text_path': text_file,
                                'created_at': mblog['created_at'],
                                'reposts': mblog.get('reposts_count', 0),
                                'comments': mblog.get('comments_count', 0),
                                'likes': mblog.get('attitudes_count', 0),
                                'images': ', '.join(image_urls) if image_urls else '无'
                            })
                    
                    print(f"已处理第 {page} 页数据")
                    time.sleep(2)  # 礼貌性延迟
                
            except Exception as e:
                print(f"第 {page} 页处理异常:", str(e))
        
        return pd.DataFrame(all_data)

    def save_metadata(self, df):
        """保存元数据到Excel"""
        excel_path = os.path.join(self.base_dir, '微博元数据.xlsx')
        df.to_excel(excel_path, index=False)
        print(f"元数据已保存到 {excel_path}")

# 使用示例
if __name__ == '__main__':
    # 配置参数
    user_id = '2053782235'  # 南网50Hz的用户ID
    cookie = '_T_WM=32753566029; WEIBOCN_FROM=1110006030; SCF=Am0jHTGWTsBEX2NoPa32YdUWRtZHTNm3-xo_fGdQAzpc8YG6LvWSngDIJe7W6bZR1T5AaDX7Uo5s8vexnr1u59E.; SUB=_2A25FVW-KDeRhGe5O61QU9i3NyjWIHXVmK-1CrDV6PUJbktAbLRbEkW1Ndbp7lFPyoYPCz_qY9Km7CZNG3XdyNtpz; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWgqmRcS77POT1jPOuCOOww5NHD95QReh5cSKq0eK24Ws4DqcjMi--NiK.Xi-2Ri--ciKnRi-zN1h57So-ce02p1Btt; SSOLoginState=1750147036; ALF=1752739036; MLOGIN=1; XSRF-TOKEN=126987; mweibo_short_token=146fc5a91a; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1005052053782235; geetest_token=96a62e5583d9e487d9b83bb2910bb9c2'
    # 创建爬虫实例
    spider = WeiboSpiderWithImages(user_id, cookie)
    
    # 获取数据并保存
    df = spider.get_weibo_data(pages=3100)  # 获取3页数据
    if not df.empty:
        spider.save_metadata(df)
        print("\n数据统计:")
        print(f"总微博数: {len(df)}")
        print(f"带图片微博: {len(df[df['images'] != '无'])}")
        print(f"图片总数: {sum(len(imgs.split(', ')) for imgs in df['images'] if imgs != '无')}")
    else:
        print("未获取到数据")