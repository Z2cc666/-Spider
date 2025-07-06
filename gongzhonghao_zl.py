#!/home/inspur/anaconda3/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import time
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wechat_crawler.log'),
        logging.StreamHandler()
    ]
)

class WeChatCrawler:
    def __init__(self, target_date=None):
        """
        初始化爬虫
        :param target_date: 目标日期，格式为'YYYY-MM-DD'，默认为当天
        """
        self.cookies = {
            # 你的cookies配置
            'pgv_pvid': '1471859404978096',
            'pac_uid': '0_zCFQ3jA7pp8dB',
            '_qimei_uuid42': '193180e24281008d3fa13c77a726b71dc9be17d812',
            '_qimei_fingerprint': '63cdc74b5ee8ca3f649c5ac65b0006a8',
            '_qimei_q36': '',
            '_qimei_h38': '936fd69b3fa13c77a726b71d02000001e19318',
            'ua_id': 'e3Sslr1eD79zqt35AAAAAESTUuR6ClX-Zi4sZdk64kU=',
            '_clck': '7z78i6|1|fwi|0',
            'uuid': '986152af9cc301c7907f27e37f273f6e',
            'wxuin': '49092858894467',
            'rand_info': 'CAESIIyZsbhRDoOps9CU1QlRHdEcE18SuSJYKYkfd0/rGL3w',
            'slave_bizuin': '3240669501',
            'data_bizuin': '3240669501',
            'bizuin': '3240669501',
            'data_ticket': 'Ef2goPeM+B5NaDHksrD2NQXH1vqbUEAuLWKjrCE9HZeyQh61zoRJaqgL5Fa9kJsl',
            'slave_sid': 'OElBcXoyV19GQXYzeU1VcnJ0VElxb3lYOV9ubDFZYTVYV1NPcWlteERoSWJDak51bFVzSVBZOEFzdGp6bDBnSHo5QmtER19Tazd3M2VUVEFRVlk2M2NzdnloY0dUbmo1QUJUTllVZklRc1JhNndGQUMwcnBLZ2E3QXZEazdyMWxEdlBwcE1BV3U4QzhENXJm',
            'slave_user': 'gh_afcb8bc17bdb',
            'xid': '1ccbaa6d1ff7b3c448d1e17bd34c8d9e',
        #     'mm_lang': 'zh_CN',
        #     '_clsk': '1b6nvqr|1749092997186|3|1|mp.weixin.qq.com/weheat-agent/payload/record',
        }
        self.headers = {
            # 你的headers配置
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'priority': 'u=1, i',
            # 'referer': 'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=77&share=1&token=2003789496&lang=zh_CN&timestamp=1749092991060',
            'referer': 'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=77&share=1&token=3514936&lang=zh_CN&timestamp=1750344618094',
            'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            # 'cookie': 'pgv_pvid=1471859404978096; pac_uid=0_zCFQ3jA7pp8dB; _qimei_uuid42=193180e24281008d3fa13c77a726b71dc9be17d812; _qimei_fingerprint=63cdc74b5ee8ca3f649c5ac65b0006a8; _qimei_q36=; _qimei_h38=936fd69b3fa13c77a726b71d02000001e19318; ua_id=e3Sslr1eD79zqt35AAAAAESTUuR6ClX-Zi4sZdk64kU=; _clck=7z78i6|1|fwi|0; uuid=986152af9cc301c7907f27e37f273f6e; wxuin=49092858894467; rand_info=CAESIIyZsbhRDoOps9CU1QlRHdEcE18SuSJYKYkfd0/rGL3w; slave_bizuin=3240669501; data_bizuin=3240669501; bizuin=3240669501; data_ticket=Ef2goPeM+B5NaDHksrD2NQXH1vqbUEAuLWKjrCE9HZeyQh61zoRJaqgL5Fa9kJsl; slave_sid=OElBcXoyV19GQXYzeU1VcnJ0VElxb3lYOV9ubDFZYTVYV1NPcWlteERoSWJDak51bFVzSVBZOEFzdGp6bDBnSHo5QmtER19Tazd3M2VUVEFRVlk2M2NzdnloY0dUbmo1QUJUTllVZklRc1JhNndGQUMwcnBLZ2E3QXZEazdyMWxEdlBwcE1BV3U4QzhENXJm; slave_user=gh_afcb8bc17bdb; xid=1ccbaa6d1ff7b3c448d1e17bd34c8d9e; mm_lang=zh_CN; _clsk=1b6nvqr|1749092997186|3|1|mp.weixin.qq.com/weheat-agent/payload/record',
            'cookie': 'ua_id=L3LS30lCrMcPC19oAAAAAMDT9HyZw6sMVX-ZoaK5Wts=; wxuin=50153937004198; mm_lang=zh_CN; RK=AWH0xyCbRF; ptcz=29962b8615f47fc44645a40f3dee13e0e64cb9feb3ab9da2d365c1dc4402a7b1; _clck=famnza|1|fx7|0; uuid=81c1f657dccc7164c30526fced0167a8; rand_info=CAESIO8zJa4EMw5d/q04/eqGbxrXH4U0M+QEupc6XHvW3IFH; slave_bizuin=3191125112; data_bizuin=3191125112; bizuin=3191125112; data_ticket=wP3N/XgiT0EfHlypmUdFM54xX4t2F4M5VBzK5DuwsuxFj3zeSJsytRZXAXI6Q0Lw; slave_sid=UUFTQnhsdEhHUGkxa3V2NlBfTXZYMlc2bzRxM3EyMnN1OUF1alBSaXF4YlhYNkpZaVFoWWVncTRzZU9RVERSdDZ0ZGt0dGoxYll4aHJobmlmbDJiMW1qM0RSbmxweTZfYWJTa0dzSXliWHlVTDk2N2RkWGg1ak5PMG9ORktQaU1KdGRlSVVhMmU2dldZSXlw; slave_user=gh_3c77b71e7b69; xid=b977718096e5e093f9bb4d1d39552f15; _clsk=1w2g9sk|1751252909593|2|1|mp.weixin.qq.com/weheat-agent/payload/record'

        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.cookies.update(self.cookies)
        
        # 存储已处理文章ID
        self.processed_file = "processed_articles.txt"
        self.processed_articles = self.load_processed_articles()
        
        # 设置目标日期
        self.target_date = target_date or datetime.now().strftime("%Y-%m-%d")
        self.setup_folders()

    def load_processed_articles(self):
        """加载已处理的文章ID"""
        if os.path.exists(self.processed_file):
            with open(self.processed_file, 'r') as f:
                return set(line.strip() for line in f)
        return set()

    def save_processed_article(self, article_id):
        """保存已处理的文章ID"""
        with open(self.processed_file, 'a') as f:
            f.write(f"{article_id}\n")
        self.processed_articles.add(article_id)

    def setup_folders(self):
        """创建必要的文件夹结构"""
        folders = [
            f'微信公众号文章/{self.target_date}/文本',
            f'微信公众号文章/{self.target_date}/图片'
        ]
        for folder in folders:
            os.makedirs(folder, exist_ok=True)

    def sanitize_filename(self, title):
        """清理文件名中的非法字符"""
        return re.sub(r'[\\/*?:"<>|]', "_", title)[:100]

    def is_target_date_article(self, create_time):
        """判断文章是否是指定日期发布的"""
        if isinstance(create_time, int):
            article_date = datetime.fromtimestamp(create_time).strftime("%Y-%m-%d")
        elif isinstance(create_time, str):
            try:
                # 尝试解析不同格式的时间字符串
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        article_date = datetime.strptime(create_time, fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
                else:
                    return False
            except:
                return False
        else:
            return False
            
        return article_date == self.target_date

    def download_images(self, soup, article_title, article_id):
        """下载文章中的所有图片"""
        img_dir = os.path.join('微信公众号文章', self.target_date, '图片', f"{self.sanitize_filename(article_title)}_{article_id}")
        os.makedirs(img_dir, exist_ok=True)
        
        downloaded_images = []
        for idx, img in enumerate(soup.find_all('img'), 1):
            img_url = img.get('data-src') or img.get('src')
            if not img_url:
                continue
                
            try:
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                elif not img_url.startswith(('http://', 'https://')):
                    continue

                response = self.session.get(img_url, stream=True, timeout=15)
                response.raise_for_status()
                
                # 确定文件扩展名
                content_type = response.headers.get('content-type', '')
                if 'jpeg' in content_type or 'jpg' in content_type:
                    ext = '.jpg'
                elif 'png' in content_type:
                    ext = '.png'
                else:
                    ext = os.path.splitext(urlparse(img_url).path)[1] or '.jpg'
                
                img_path = os.path.join(img_dir, f"{idx}{ext}")
                with open(img_path, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                
                downloaded_images.append(img_path)
                time.sleep(0.5)  # 降低请求频率
                
            except Exception as e:
                logging.error(f"图片下载失败: {img_url} - {str(e)}")
        
        return downloaded_images

    def save_article_text(self, soup, article_title, article_id):
        """保存文章文本内容"""
        content_div = soup.find('div', {'id': 'js_content'}) or \
                     soup.find('div', class_='rich_media_content')
        
        if not content_div:
            logging.warning("未找到文章正文内容")
            return None
        
        text = content_div.get_text(separator='\n', strip=True)
        if not text or len(text) < 50:
            logging.warning("获取的文本内容过短，可能不完整")
            return None
        
        text_dir = os.path.join('微信公众号文章', self.target_date, '文本')
        os.makedirs(text_dir, exist_ok=True)
        
        text_path = os.path.join(text_dir, f"{self.sanitize_filename(article_title)}_{article_id}.txt")
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text)
        
        return text_path

    def process_article(self, article_url, article_title, article_id):
        """处理单篇文章"""
        if article_id in self.processed_articles:
            logging.info(f"文章已处理过，跳过: {article_title}")
            return None, []
            
        if not article_url or not article_url.startswith('http'):
            logging.error(f"无效的文章URL: {article_url}")
            return None, []
        
        try:
            response = self.session.get(article_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            images = self.download_images(soup, article_title, article_id)
            text_file = self.save_article_text(soup, article_title, article_id)
            
            # 标记为已处理
            self.save_processed_article(article_id)
            
            return text_file, images
            
        except Exception as e:
            logging.error(f"处理文章失败: {article_url} - {str(e)}")
            return None, []

    def get_articles(self, fakeid, max_count=20):
        """获取公众号文章列表"""
        all_articles = []
        page_size = 5
        max_pages = 20  # 最多获取20页
        
        for page in range(max_pages):
            params = {
                'sub': 'list',
                'begin': page * page_size,
                'count': page_size,
                'fakeid': fakeid,
                'type': '101_1',
                'token': '3514936',
                'lang': 'zh_CN',
                'f': 'json',
 
                'free_publish_type': '1',
                'sub_action': 'list_ex',
                'fingerprint': '45e623cde6e181b213b3f595227b4a71',
                'token': '3514936',
                'lang': 'zh_CN',
                'f': 'json',
                'ajax': '1',
            }
            
            try:
                response = self.session.get(
                    "https://mp.weixin.qq.com/cgi-bin/appmsgpublish",
                    params=params,
                    timeout=10
                )
                time.sleep(1)  # 请求间隔
                
                if response.status_code != 200:
                    logging.error(f"请求失败，状态码: {response.status_code}")
                    break
                    
                result = response.json()
                if result.get("base_resp", {}).get("ret") != 0:
                    logging.error(f"API返回错误: {result['base_resp']['err_msg']}")
                    break

                publish_page = result.get("publish_page")
                if not publish_page:
                    logging.info("没有更多文章了")
                    break

                articleList = json.loads(publish_page)
                articles = articleList.get("publish_list", [])
                if not articles:
                    logging.info("文章列表为空")
                    break
                
                # 过滤出目标日期的文章
                target_articles = []
                for article in articles:
                    try:
                        info = json.loads(article.get("publish_info", "{}"))
                        if info.get("appmsgex"):
                            create_time = info["appmsgex"][0].get("create_time")
                            if self.is_target_date_article(create_time):
                                target_articles.append(article)
                                
                                if len(target_articles) >= max_count:
                                    break
                    except Exception as e:
                        logging.error(f"解析文章信息失败: {e}")
                        continue
                
                all_articles.extend(target_articles)
                logging.info(f"已获取 {len(target_articles)} 篇{self.target_date}的文章")
                
                if len(target_articles) < page_size or len(all_articles) >= max_count:
                    break
                    
            except Exception as e:
                logging.error(f"获取文章列表失败: {e}")
                break
        
        return all_articles[:max_count]

    def run(self, fakeid, max_count=20):
        """运行爬虫"""
        logging.info(f"开始爬取{self.target_date}的微信公众号文章...")
        
        articles = self.get_articles(fakeid, max_count)
        if not articles:
            logging.info(f"没有找到{self.target_date}发布的文章")
            return
        
        articles_data = []
        success_count = 0
        
        for article in articles:
            try:
                info = json.loads(article.get("publish_info", "{}"))
                if info.get("appmsgex"):
                    title = info["appmsgex"][0].get("title", "无标题")
                    link = info["appmsgex"][0].get("link", "")
                    article_id = info["appmsgex"][0].get("aid", "")
                    
                    if link and article_id:
                        logging.info(f"处理文章: {title}")
                        text_path, img_paths = self.process_article(link, title, article_id)
                        
                        create_time = info["appmsgex"][0].get("create_time")
                        if isinstance(create_time, int):
                            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(create_time))
                        
                        articles_data.append({
                            "标题": title,
                            "链接": link,
                            "发布时间": create_time or "",
                            "阅读量": info.get("read_num", ""),
                            "点赞量": info.get("like_num", ""),
                            "文本路径": text_path or "",
                            "图片数量": len(img_paths)
                        })
                        
                        if text_path or img_paths:
                            success_count += 1
                            
                        time.sleep(2)  # 处理间隔
            except Exception as e:
                logging.error(f"处理文章失败: {e}")
                continue
        
        # 保存结果到Excel
        if articles_data:
            excel_path = f"微信公众号文章/{self.target_date}/articles_{self.target_date}.xlsx"
            pd.DataFrame(articles_data).to_excel(excel_path, index=False)
            logging.info(f"成功保存 {success_count} 篇文章数据到 {excel_path}")
        else:
            logging.info("没有成功处理任何文章")

if __name__ == "__main__":
    # 在这里设置要爬取的日期，格式为'YYYY-MM-DD'
    # 例如：target_date = '2023-11-15'
    # 如果为None或空字符串，则默认为当天
    target_date = None  # 修改这里设置目标日期
    # target_date = '2025-07-01'
    
    crawler = WeChatCrawler(target_date=target_date)
    
    # 公众号fakeid (示例: MzA3OTA2Mjc2MA==)
    fakeid = "MzA3OTA2Mjc2MA=="
    
    # 每天最多爬取20篇新文章
    crawler.run(fakeid, max_count=20)