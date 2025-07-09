#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os, time, json, requests, chardet, subprocess, sys
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote
import re
import schedule
import pickle
from pathlib import Path

class NetEaseIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.163.com/',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        }
        self.base_url = "https://www.163.com"
        self.base_dir = "网易"
        self.processed_file = os.path.join(self.base_dir, "processed_articles.json")
        self.processed_urls = self.load_processed_urls()
        self.debug = True
        self.stats = {
            'total_processed': 0,
            'skipped': 0,
            'new': 0,
            'errors': 0
        }
        
        # 频道分类
        self.channels = {
            'news': {'name': '新闻', 'url': 'https://news.163.com/'},
            'sports': {'name': '体育', 'url': 'https://sports.163.com/'},
            'nba': {'name': 'NBA', 'url': 'https://sports.163.com/nba/'},
            'ent': {'name': '娱乐', 'url': 'https://ent.163.com/'},
            'money': {'name': '财经', 'url': 'https://money.163.com/'},
            'stock': {'name': '股票', 'url': 'https://money.163.com/stock/'},
            'auto': {'name': '汽车', 'url': 'https://auto.163.com/'},
            'tech': {'name': '科技', 'url': 'https://tech.163.com/'},
            'mobile': {'name': '手机', 'url': 'https://mobile.163.com/'},
            'lady': {'name': '女人', 'url': 'https://lady.163.com/'},
            'v': {'name': '视频', 'url': 'https://v.163.com/'},
            'house': {'name': '房产', 'url': 'https://house.163.com/'},
            'edu': {'name': '教育', 'url': 'https://edu.163.com/'},
            'war': {'name': '军事', 'url': 'https://war.163.com/'},
            'travel': {'name': '旅游', 'url': 'https://travel.163.com/'},
            'digi': {'name': '数码', 'url': 'https://digi.163.com/'},
        }

    def load_processed_urls(self):
        """加载已处理的URL和文章信息"""
        processed_urls = set()
        
        try:
            os.makedirs(self.base_dir, exist_ok=True)
            
            # 加载旧版本的processed_urls.pkl
            urls_file = Path(self.base_dir) / 'processed_urls.pkl'
            if urls_file.exists():
                try:
                    with open(urls_file, 'rb') as f:
                        processed_urls = pickle.load(f)
                except Exception as e:
                    if self.debug: print(f"加载processed_urls.pkl失败: {str(e)}")

            # 加载新版本的processed_articles.json
            if os.path.exists(self.processed_file):
                try:
                    with open(self.processed_file, 'r', encoding='utf-8') as f:
                        processed_articles = json.load(f)
                        processed_urls.update(article['url'] for article in processed_articles)
                except Exception as e:
                    if self.debug: print(f"加载processed_articles.json失败: {str(e)}")
        except Exception as e:
            if self.debug: print(f"加载已处理URL记录失败: {str(e)}")

        return processed_urls

    def save_processed_urls(self):
        """保存已处理的URL和文章信息"""
        try:
            # 保存旧版本的processed_urls.pkl
            urls_file = Path(self.base_dir) / 'processed_urls.pkl'
            urls_file.parent.mkdir(parents=True, exist_ok=True)
            with open(urls_file, 'wb') as f:
                pickle.dump(self.processed_urls, f)

            # 保存新版本的processed_articles.json
            processed_articles = []
            for url in self.processed_urls:
                article_info = {
                    'url': url,
                    'date': self.get_date_from_url(url),
                    'processed_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                processed_articles.append(article_info)

            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump(processed_articles, f, ensure_ascii=False, indent=2)
        except Exception as e:
            if self.debug: print(f"保存文章记录失败: {str(e)}")

    def safe_request(self, url, stream=False):
        """安全的网络请求"""
        try:
            if self.debug: print(f"发送请求: {url}")
            time.sleep(1)
            
            resp = requests.get(url, headers=self.headers, timeout=30, stream=stream)
            
            if self.debug:
                print(f"响应状态码: {resp.status_code}")
            
            resp.raise_for_status()
            
            if not stream:
                content_type = resp.headers.get('Content-Type', '').lower()
                if 'charset=' in content_type:
                    charset = content_type.split('charset=')[-1].strip()
                    resp.encoding = charset
                else:
                    content_bytes = resp.content
                    detected = chardet.detect(content_bytes)
                    if detected and detected['confidence'] > 0.8:
                        resp.encoding = detected['encoding']
                    else:
                        resp.encoding = 'utf-8'
            
            return resp
            
        except Exception as e:
            if self.debug: print(f"请求失败: {url}, 错误: {str(e)}")
            return None

    def clean_text(self, text):
        """清理文本内容"""
        if not text: return ""
        
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\xff]', '', text)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[\u200b-\u200f\u202a-\u202e\uFEFF]', '', text)
        text = re.sub(r'[\u0000-\u001f\u007f-\u009f]', '', text)
        
        return text.strip()

    def clean_filename(self, filename):
        """清理文件名"""
        return re.sub(r'[<>:"/\\|?*]', '_', filename)[:100].strip()

    def get_date_from_url(self, url):
        """从URL中提取日期"""
        try:
            # 网易的URL日期格式可能是 /yyyy/mm/dd/ 或 /yyyymmdd/
            match = re.search(r'/(\d{4}(?:/\d{2}/\d{2}|\d{4}))/', url)
            if match:
                date_str = match.group(1).replace('/', '')
                return date_str
            return datetime.now().strftime('%Y%m%d')
        except:
            return datetime.now().strftime('%Y%m%d')

    def is_article_processed(self, url, title=None):
        """检查文章是否已处理"""
        try:
            if url in self.processed_urls:
                if self.debug: 
                    print(f"跳过已处理的URL: {url}")
                    if title:
                        print(f"标题: {title}")
                self.stats['skipped'] += 1
                return True

            if title:
                date_str = self.get_date_from_url(url)
                article_dir = os.path.join(self.base_dir, date_str, 
                                         self.clean_filename(title))
                if os.path.exists(article_dir):
                    if self.debug: 
                        print(f"文章目录已存在: {article_dir}")
                        print(f"标题: {title}")
                    self.processed_urls.add(url)
                    self.save_processed_urls()
                    self.stats['skipped'] += 1
                    return True

            self.stats['new'] += 1
            return False
        except Exception as e:
            if self.debug: print(f"检查文章状态失败: {str(e)}")
            self.stats['errors'] += 1
            return False

    def download_media(self, url, save_path):
        """下载媒体文件"""
        try:
            if not url.startswith(('http://', 'https://')):
                return False

            resp = self.safe_request(url, stream=True)
            if not resp:
                return False

            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception as e:
            if self.debug: print(f"下载媒体文件失败: {url}, 错误: {str(e)}")
            return False

    def find_videos(self, soup):
        """查找视频链接"""
        videos = []
        try:
            # 查找video标签
            for video in soup.find_all('video', src=True):
                videos.append(video['src'])
            
            # 查找video.163.com的视频
            for iframe in soup.find_all('iframe', src=True):
                src = iframe['src']
                if 'video.163.com' in src or 'v.163.com' in src:
                    videos.append(src)
            
            # 查找视频播放器
            for div in soup.find_all('div', class_=lambda x: x and ('video' in x.lower() or 'player' in x.lower())):
                data_src = div.get('data-src', '')
                if data_src:
                    videos.append(data_src)
        except Exception as e:
            if self.debug: print(f"查找视频失败: {str(e)}")
        
        return list(set(videos))

    def get_content_selectors(self, url):
        """获取不同类型页面的内容选择器"""
        selectors = {
            'content': ['div.post_text', 'div.article-content', 'div.content', 'div#endText'],
            'title': ['h1.post_title', 'h1.title', 'div.article-title h1'],
            'date': ['div.post_time', 'div.article-time', 'span.time']
        }
        
        # 根据URL确定具体的选择器
        if 'news.163.com' in url:
            selectors['content'] = ['div.post_text']
        elif 'sports.163.com' in url:
            selectors['content'] = ['div.post_text']
        elif 'ent.163.com' in url:
            selectors['content'] = ['div.post_text']
            
        return selectors

    def process_article(self, article):
        """处理单篇文章"""
        try:
            url = article['url']
            title = article['title']
            
            if self.is_article_processed(url, title):
                return False
                
            resp = self.safe_request(url)
            if not resp:
                return False
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 获取选择器
            selectors = self.get_content_selectors(url)
            
            # 提取正文
            content = None
            for selector in selectors['content']:
                content_div = soup.select_one(selector)
                if content_div:
                    content = content_div
                    break
                    
            if not content:
                if self.debug: print(f"未找到文章内容: {url}")
                return False
                
            # 创建文章目录
            date_str = self.get_date_from_url(url)
            safe_title = self.clean_filename(title)
            article_dir = os.path.join(self.base_dir, date_str, safe_title)
            os.makedirs(article_dir, exist_ok=True)
            
            # 保存文章信息和内容到同一个文件
            content_text = self.clean_text(content.get_text())
            article_content = f"""标题：{title}
URL：{url}
日期：{date_str}
抓取时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

正文内容：
{content_text}
"""
            with open(os.path.join(article_dir, f"{safe_title}.txt"), 'w', encoding='utf-8') as f:
                f.write(article_content)
            
            # 下载图片
            for i, img in enumerate(content.find_all('img', src=True)):
                img_url = img['src']
                if not img_url.startswith(('http://', 'https://')):
                    img_url = urljoin(url, img_url)
                
                # 使用标题作为图片文件名前缀
                img_path = os.path.join(article_dir, f"{safe_title}_{i}.png")
                self.download_media(img_url, img_path)
            
            # 下载视频
            videos = self.find_videos(soup)
            if videos:
                for i, video_url in enumerate(videos):
                    video_ext = os.path.splitext(video_url)[1]
                    if not video_ext:
                        video_ext = '.mp4'
                        
                    video_path = os.path.join(article_dir, f"{safe_title}_{i}{video_ext}")
                    self.download_media(video_url, video_path)
            
            # 添加到已处理集合
            self.processed_urls.add(url)
            self.stats['total_processed'] += 1
            
            return True
            
        except Exception as e:
            if self.debug: print(f"处理文章失败: {url}, 错误: {str(e)}")
            self.stats['errors'] += 1
            return False

    def get_articles_from_homepage(self):
        """从首页获取文章列表"""
        articles = []
        
        for channel_key, channel_info in self.channels.items():
            try:
                if self.debug: print(f"\n开始获取频道: {channel_info['name']}")
                resp = self.safe_request(channel_info['url'])
                if not resp: continue
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                for link in soup.find_all('a', href=True):
                    url = link.get('href', '').strip()
                    if not url: continue
                    
                    url = url if url.startswith('http') else urljoin(channel_info['url'], url)
                    
                    if not any(domain in url for domain in ['163.com']) or \
                       not any(ext in url for ext in ['.html', 'article']):
                        continue
                    
                    title = self.clean_text(link.get_text())
                    if not title or len(title) < 4: continue  # 只保留基本的长度检查
                        
                    articles.append({
                        'url': url,
                        'title': title,
                        'channel': channel_info['name']
                    })
                    
            except Exception as e:
                if self.debug: print(f"获取频道文章失败: {channel_info['name']}, 错误: {str(e)}")
                self.stats['errors'] += 1
                
        return articles

    def run_once(self):
        """执行一次爬取任务"""
        try:
            if self.debug: print("\n开始新一轮爬取...")
            
            # 重置统计数据
            self.stats = {
                'total_processed': 0,
                'skipped': 0,
                'new': 0,
                'errors': 0
            }
            
            # 获取并处理文章
            articles = self.get_articles_from_homepage()
            if articles:
                print(f"\n找到 {len(articles)} 篇文章")
                for article in articles:
                    if self.debug: print(f"\n处理: {article['title']}")
                    if self.process_article(article):
                        print(f"成功保存: {article['title']}")
                    time.sleep(1)
            
            # 打印统计信息
            print("\n=== 本次爬取统计 ===")
            print(f"总处理文章数: {self.stats['total_processed']}")
            print(f"新增文章数: {self.stats['new']}")
            print(f"跳过已处理: {self.stats['skipped']}")
            print(f"处理出错数: {self.stats['errors']}")
            print("==================\n")
            
            # 保存处理记录
            self.save_processed_urls()
            
        except Exception as e:
            if self.debug: print(f"爬取任务执行失败: {str(e)}")
            self.stats['errors'] += 1

def run_spider():
    """运行爬虫"""
    spider = NetEaseIncrementalSpider()
    
    # 运行一次
    spider.run_once()
    
    # 设置定时任务
    schedule.every(30).minutes.do(spider.run_once)
    
    # 持续运行
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_spider() 