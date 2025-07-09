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

class CNRIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.cnr.cn/',
            'Accept': '*/*'
        }
        self.base_url = "https://www.cnr.cn"
        self.news_url = "https://news.cnr.cn"
        self.base_dir = "央广网"
        self.processed_urls = self.load_processed_urls()
        self.debug = False
        
        # 频道分类
        self.channels = {
            # 主要频道
            'news': {'name': '新闻', 'url': 'https://news.cnr.cn/'},
            'politics': {'name': '时政', 'url': 'https://news.cnr.cn/native/gd/'},
            'erwen': {'name': '耳闻眼见', 'url': 'https://www.cnr.cn/erwen/'},
            'yuyue': {'name': '云遇中国', 'url': 'https://www.cnr.cn/yuyue/'},
            'songdu': {'name': '诵读大会', 'url': 'https://www.cnr.cn/syk/'},
            'finance': {'name': '财经', 'url': 'https://finance.cnr.cn/'},
            'law': {'name': '法治', 'url': 'https://china.cnr.cn/law/'},
            'tech': {'name': '科技', 'url': 'https://tech.cnr.cn/'},
            'auto': {'name': '汽车', 'url': 'https://auto.cnr.cn/'},
            'health': {'name': '中华名医号', 'url': 'https://health.cnr.cn/'},
            'edu': {'name': '教育', 'url': 'https://edu.cnr.cn/'},
            'travel': {'name': '文旅', 'url': 'https://travel.cnr.cn/'},
            'military': {'name': '军事', 'url': 'https://military.cnr.cn/'},
            'house': {'name': '房产', 'url': 'https://house.cnr.cn/'},
            'food': {'name': '食品', 'url': 'https://food.cnr.cn/'},
            
            # 地方频道
            'bj': {'name': '北京', 'url': 'https://www.cnr.cn/bj/'},
            'tj': {'name': '天津', 'url': 'https://www.cnr.cn/tj/'},
            'hebei': {'name': '河北', 'url': 'https://www.cnr.cn/hebei/'},
            'sx': {'name': '山西', 'url': 'https://www.cnr.cn/sx/'},
            'nmg': {'name': '内蒙古', 'url': 'https://www.cnr.cn/nmg/'},
            'ln': {'name': '辽宁', 'url': 'https://www.cnr.cn/ln/'},
            'jl': {'name': '吉林', 'url': 'https://www.cnr.cn/jl/'},
            'hlj': {'name': '黑龙江', 'url': 'https://hlj.cnr.cn/'},
            'sh': {'name': '上海', 'url': 'https://www.cnr.cn/shanghai/'},
            'js': {'name': '江苏', 'url': 'https://www.cnr.cn/js/'},
            'zj': {'name': '浙江', 'url': 'https://www.cnr.cn/zj/'},
            'ah': {'name': '安徽', 'url': 'https://www.cnr.cn/ah/'},
            'fj': {'name': '福建', 'url': 'https://www.cnr.cn/fj/'},
            'jx': {'name': '江西', 'url': 'https://www.cnr.cn/jx/'},
            'sd': {'name': '山东', 'url': 'https://www.cnr.cn/sd/'},
            'henan': {'name': '河南', 'url': 'https://www.cnr.cn/henan/'},
            'hubei': {'name': '湖北', 'url': 'https://www.cnr.cn/hubei/'},
            'hunan': {'name': '湖南', 'url': 'https://www.cnr.cn/hunan/'},
            'gd': {'name': '广东', 'url': 'https://www.cnr.cn/gd/'},
            'gx': {'name': '广西', 'url': 'https://www.cnr.cn/gx/'},
            'hainan': {'name': '海南', 'url': 'https://www.cnr.cn/hainan/'},
            'cq': {'name': '重庆', 'url': 'https://www.cnr.cn/cq/'},
            'sc': {'name': '四川', 'url': 'https://www.cnr.cn/sc/'},
            'gz': {'name': '贵州', 'url': 'https://www.cnr.cn/gz/'},
            'yn': {'name': '云南', 'url': 'https://www.cnr.cn/yn/'},
            'sx': {'name': '陕西', 'url': 'https://www.cnr.cn/shaanxi/'},
            'gs': {'name': '甘肃', 'url': 'https://www.cnr.cn/gs/'},
            'qh': {'name': '青海', 'url': 'https://www.cnr.cn/qh/'},
            'nx': {'name': '宁夏', 'url': 'https://www.cnr.cn/nx/'},
            'xj': {'name': '新疆', 'url': 'https://www.cnr.cn/xinjiang/'},
            
            # 计划单列市
            'dl': {'name': '大连', 'url': 'https://www.cnr.cn/dl/'},
            'nb': {'name': '宁波', 'url': 'https://www.cnr.cn/nb/'},
            'xm': {'name': '厦门', 'url': 'https://www.cnr.cn/xm/'},
            'sz': {'name': '深圳', 'url': 'https://www.cnr.cn/sz/'},
        }
        
    def load_processed_urls(self):
        """加载已处理的URL"""
        urls_file = Path(self.base_dir) / 'processed_urls.pkl'
        if urls_file.exists():
            try:
                with open(urls_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
        
    def save_processed_urls(self):
        """保存已处理的URL"""
        urls_file = Path(self.base_dir) / 'processed_urls.pkl'
        urls_file.parent.mkdir(parents=True, exist_ok=True)
        with open(urls_file, 'wb') as f:
            pickle.dump(self.processed_urls, f)

    def safe_request(self, url, is_json=False):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=self.headers, timeout=20)
            resp.raise_for_status()
            if not is_json:
                resp.encoding = chardet.detect(resp.content)['encoding'] or 'utf-8'
            return resp.json() if is_json and resp.text.strip() else resp if resp.text.strip() else None
        except Exception as e:
            if self.debug: print(f"请求失败: {url}, {str(e)}")
            return None

    def clean_text(self, text):
        if not text: return ""
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\xff]', '', text)
        return re.sub(r'\s+', ' ', text).strip()

    def clean_filename(self, filename):
        return re.sub(r'[<>:"/\\|?*]', '_', filename)[:100].strip()

    def get_category_from_url(self, url):
        try:
            path = urlparse(url).path
            parts = [p for p in path.split('/') if p and not p.startswith('t2') and not p.endswith('.shtml')]
            return '/'.join(parts[-3:-1]) if len(parts) >= 3 else "未分类"
        except:
            return "未分类"

    def get_articles_from_homepage(self):
        articles = []
        
        # 遍历所有频道
        for channel_key, channel_info in self.channels.items():
            try:
                resp = self.safe_request(channel_info['url'])
                if not resp: continue
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                for link in soup.find_all('a', href=True):
                    url = link.get('href', '').strip()
                    if not url or url in self.processed_urls: continue
                    
                    url = url if url.startswith('http') else urljoin(channel_info['url'], url)
                    if not any(domain in url for domain in ['cnr.cn', 'china.com']) or \
                       not any(ext in url for ext in ['.html', '.shtml', '.htm']): continue
                    
                    title = self.clean_text(link.get_text())
                    if not title or len(title) < 4: continue
                    
                    # 使用频道名称作为分类
                    articles.append({
                        'title': title,
                        'url': url,
                        'category': channel_info['name']
                    })
                    
                print(f"已获取 {channel_info['name']} 频道的文章")
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                if self.debug: print(f"处理频道失败: {channel_info['name']}, {str(e)}")
                
        return articles

    def process_media(self, url, save_path, m3u8_url=None):
        try:
            resp = self.safe_request(url)
            if not resp: return False
            
            if m3u8_url:  # 处理ts文件
                if len(resp.content) < 1000: return False
                with open(save_path, 'wb') as f:
                    f.write(resp.content)
                return True
                
            # 处理普通媒体文件
            with open(save_path, 'wb') as f:
                f.write(resp.content)
            return True
        except Exception as e:
            if self.debug: print(f"下载失败: {url}, {str(e)}")
            return False

    def merge_videos(self, ts_files, output_file):
        try:
            if not ts_files: return False
            
            # 尝试使用ffmpeg
            try:
                # 创建文件列表
                input_file = f"{output_file}.txt"
                with open(input_file, 'w') as f:
                    for ts in ts_files:
                        f.write(f"file '{os.path.abspath(ts)}'\n")
                
                # 使用ffmpeg合并
                cmd = ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', input_file,
                      '-c', 'copy', output_file, '-y']
                result = subprocess.run(cmd, capture_output=True)
                os.remove(input_file)
                
                if result.returncode == 0:
                    for ts in ts_files: 
                        try: os.remove(ts)
                        except: pass
                    return True
            except:
                pass
            
            # 如果ffmpeg失败，使用二进制合并
            with open(output_file, 'wb') as outfile:
                for ts in ts_files:
                    if os.path.exists(ts):
                        with open(ts, 'rb') as infile:
                            outfile.write(infile.read())
                        try: os.remove(ts)
                        except: pass
            
            return os.path.exists(output_file) and os.path.getsize(output_file) > 0
            
        except Exception as e:
            if self.debug: print(f"合并视频失败: {str(e)}")
            return False

    def process_m3u8(self, url, save_path):
        try:
            resp = self.safe_request(url)
            if not resp: return False
            
            # 解析m3u8获取ts文件列表
            ts_urls = [line.strip() for line in resp.text.split('\n') 
                      if line.strip() and not line.startswith('#')]
            
            if not ts_urls: return False
            
            # 下载ts文件
            ts_files = []
            base_url = '/'.join(url.split('/')[:-1]) + '/'
            
            for i, ts_url in enumerate(ts_urls):
                ts_url = ts_url if ts_url.startswith('http') else urljoin(base_url, ts_url)
                ts_file = f"{save_path}.{i}.ts"
                if self.process_media(ts_url, ts_file, url):
                    ts_files.append(ts_file)
            
            # 合并视频
            if len(ts_files) == len(ts_urls):
                output_file = save_path.rsplit('.', 1)[0] + '.mp4'
                return self.merge_videos(ts_files, output_file)
                
            return False
            
        except Exception as e:
            if self.debug: print(f"处理m3u8失败: {url}, {str(e)}")
            return False

    def process_video(self, url, save_path):
        url = re.sub(r'\],poster:.*$', '', unquote(url))
        return self.process_m3u8(url, save_path) if '.m3u8' in url.lower() else self.process_media(url, save_path)

    def find_videos(self, soup):
        video_urls = set()
        
        # 从video标签和source标签获取
        for video in soup.find_all('video'):
            if src := video.get('src'): video_urls.add(src)
            for source in video.find_all('source'):
                if src := source.get('src'): video_urls.add(src)
        
        # 从特定属性获取
        for attr in ['data-video', 'data-src', 'data-url', 'src', 'data-mp4']:
            for elem in soup.find_all(attrs={attr: True}):
                if url := elem[attr]:
                    if any(ext in url.lower() for ext in ['.mp4', '.m3u8', '.flv']):
                        video_urls.add(url)
        
        # 从script标签获取
        pattern = r'https?://[^\s<>"]+?\.(?:mp4|m3u8|flv)[^\s<>"]*'
        for script in soup.find_all('script'):
            if script.string:
                video_urls.update(re.findall(pattern, script.string))
        
        return video_urls

    def process_article(self, article):
        try:
            resp = self.safe_request(article['url'])
            if not resp: return False

            soup = BeautifulSoup(resp.text, 'html.parser')
            date_str = datetime.now().strftime('%Y%m%d')
            article_dir = os.path.join(self.base_dir, date_str, 
                                     article['category'], 
                                     self.clean_filename(article['title']))
            os.makedirs(article_dir, exist_ok=True)

            # 获取正文
            content = None
            for selector in ['div.article-content', 'div.content', 'article.TRS_Editor',
                           'div.TRS_Editor', 'div.articleText', 'div#articleContent']:
                if content := soup.select_one(selector): break

            if not content: return False

            # 清理内容
            for tag in content.find_all(['script', 'style']): tag.decompose()
            text = self.clean_text(content.get_text('\n'))
            
            if len(text) > 50:
                # 保存文章
                with open(os.path.join(article_dir, f"{self.clean_filename(article['title'])}.txt"), 'w') as f:
                    f.write(f"标题：{article['title']}\n日期：{date_str}\n")
                    f.write(f"分类：{article['category']}\n链接：{article['url']}\n")
                    f.write("-" * 50 + "\n\n" + text)

                # 保存图片
                for img in soup.find_all('img'):
                    if img_url := img.get('src') or img.get('data-original') or img.get('data-src'):
                        if any(skip in img_url.lower() for skip in ['ico', 'logo', 'banner']): continue
                        img_url = img_url if img_url.startswith('http') else urljoin(article['url'], img_url)
                        ext = img_url.split('.')[-1].lower() if '.' in img_url else 'jpg'
                        if ext not in ['jpg', 'jpeg', 'png', 'gif']: ext = 'jpg'
                        img_path = os.path.join(article_dir, f"{self.clean_filename(article['title'])}_{len(os.listdir(article_dir))}.{ext}")
                        self.process_media(img_url, img_path)

                # 保存视频
                for video_url in self.find_videos(soup):
                    video_url = video_url if video_url.startswith('http') else urljoin(article['url'], video_url)
                    ext = 'mp4' if '.m3u8' not in video_url.lower() else 'm3u8'
                    video_path = os.path.join(article_dir, f"{self.clean_filename(article['title'])}_{len(os.listdir(article_dir))}.{ext}")
                    self.process_video(video_url, video_path)

                # 标记URL为已处理
                self.processed_urls.add(article['url'])
                return True
                
        except Exception as e:
            if self.debug: print(f"处理文章失败: {article['title']}, {str(e)}")
        return False

    def run_once(self):
        """执行一次爬取"""
        print(f"\n{'='*20} 开始爬取 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {'='*20}")
        
        if articles := self.get_articles_from_homepage():
            print(f"\n找到 {len(articles)} 篇新文章")
            for article in articles:
                print(f"\n处理: {article['title']}")
                if self.process_article(article):
                    print(f"成功保存: {article['title']}")
                time.sleep(1)
            
            # 保存处理过的URL列表
            self.save_processed_urls()
        else:
            print("没有发现新文章")

def run_spider():
    """运行爬虫的包装函数"""
    try:
        spider = CNRIncrementalSpider()
        spider.debug = os.environ.get('DEBUG', '').lower() in ('true', '1', 'yes')
        spider.run_once()
    except Exception as e:
        print(f"爬取过程出错: {str(e)}")
        if spider.debug:
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    # 确保基础目录存在
    os.makedirs("央广网", exist_ok=True)
    
    # 设置定时任务
    schedule.every().day.at("00:00").do(run_spider)  # 每天0点
    schedule.every().day.at("09:43").do(run_spider)  # 每天8点
    schedule.every().day.at("12:00").do(run_spider)  # 每天12点
    schedule.every().day.at("16:00").do(run_spider)  # 每天16点
    schedule.every().day.at("20:00").do(run_spider)  # 每天20点
    
    print("增量爬虫已启动，将在设定时间运行...")
    print("爬取时间: 每天的 00:00, 08:00, 12:00, 16:00, 20:00")
    
    # 先运行一次
    run_spider()
    
    # 持续运行定时任务
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            print("\n用户中断，正在退出...")
            break
        except Exception as e:
            print(f"运行出错: {str(e)}")
            time.sleep(300)  # 出错后等待5分钟再继续 