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

class PeopleIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'http://www.people.com.cn/',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Range': 'bytes=0-',  # 支持断点续传
            'Connection': 'keep-alive'
        }
        self.base_url = "http://www.people.com.cn"
        self.base_dir = "人民网"
        self.processed_urls = self.load_processed_urls()
        self.debug = True  # 默认开启调试模式
        
        # 频道分类
        self.channels = {
            # 党政频道
            'cpc': {'name': '党网时政', 'url': 'http://cpc.people.com.cn/'},
            'renshi': {'name': '人事', 'url': 'http://renshi.people.com.cn/'},
            'fanfu': {'name': '反腐', 'url': 'http://fanfu.people.com.cn/'},
            'theory': {'name': '理论', 'url': 'http://theory.people.com.cn/'},
            'dangshi': {'name': '党史', 'url': 'http://dangshi.people.com.cn/'},
            'dangjian': {'name': '党建', 'url': 'http://dangjian.people.com.cn/'},
            
            # 要闻频道
            'finance': {'name': '经济科技', 'url': 'http://finance.people.com.cn/'},
            'society': {'name': '社会法治', 'url': 'http://society.people.com.cn/'},
            'ent': {'name': '文旅体育', 'url': 'http://ent.people.com.cn/'},
            'health': {'name': '健康生活', 'url': 'http://health.people.com.cn/'},
            'world': {'name': '国际', 'url': 'http://world.people.com.cn/'},
            'military': {'name': '军事', 'url': 'http://military.people.com.cn/'},
            'gba': {'name': '大湾区', 'url': 'http://gba.people.cn/'},
            'tw': {'name': '台湾', 'url': 'http://tw.people.com.cn/'},
            'edu': {'name': '教育', 'url': 'http://edu.people.com.cn/'},
            'kpzg': {'name': '科普', 'url': 'http://kpzg.people.com.cn/'},
            
            # 观点频道
            'opinion': {'name': '人民网评', 'url': 'http://opinion.people.com.cn/'},
            
            # 可视化频道
            'video': {'name': '视频', 'url': 'http://v.people.cn/'},
            'pic': {'name': '图片', 'url': 'http://pic.people.com.cn/'},
            'graphic': {'name': '图解', 'url': 'http://graphicnews.people.com.cn/'},
            
            # 地方频道
            'bj': {'name': '北京', 'url': 'http://bj.people.com.cn/'},
            'tj': {'name': '天津', 'url': 'http://tj.people.com.cn/'},
            'he': {'name': '河北', 'url': 'http://he.people.com.cn/'},
            'sx': {'name': '山西', 'url': 'http://sx.people.com.cn/'},
            'nm': {'name': '内蒙古', 'url': 'http://nm.people.com.cn/'},
            'ln': {'name': '辽宁', 'url': 'http://ln.people.com.cn/'},
            'jl': {'name': '吉林', 'url': 'http://jl.people.com.cn/'},
            'hlj': {'name': '黑龙江', 'url': 'http://hlj.people.com.cn/'},
            'sh': {'name': '上海', 'url': 'http://sh.people.com.cn/'},
            'js': {'name': '江苏', 'url': 'http://js.people.com.cn/'},
            'zj': {'name': '浙江', 'url': 'http://zj.people.com.cn/'},
            'ah': {'name': '安徽', 'url': 'http://ah.people.com.cn/'},
            'fj': {'name': '福建', 'url': 'http://fj.people.com.cn/'},
            'jx': {'name': '江西', 'url': 'http://jx.people.com.cn/'},
            'sd': {'name': '山东', 'url': 'http://sd.people.com.cn/'},
            'henan': {'name': '河南', 'url': 'http://henan.people.com.cn/'},
            'hb': {'name': '湖北', 'url': 'http://hb.people.com.cn/'},
            'hn': {'name': '湖南', 'url': 'http://hn.people.com.cn/'},
            'gd': {'name': '广东', 'url': 'http://gd.people.com.cn/'},
            'gx': {'name': '广西', 'url': 'http://gx.people.com.cn/'},
            'hi': {'name': '海南', 'url': 'http://hi.people.com.cn/'},
            'cq': {'name': '重庆', 'url': 'http://cq.people.com.cn/'},
            'sc': {'name': '四川', 'url': 'http://sc.people.com.cn/'},
            'gz': {'name': '贵州', 'url': 'http://gz.people.com.cn/'},
            'yn': {'name': '云南', 'url': 'http://yn.people.com.cn/'},
            'xz': {'name': '西藏', 'url': 'http://xz.people.com.cn/'},
            'sn': {'name': '陕西', 'url': 'http://sn.people.com.cn/'},
            'gs': {'name': '甘肃', 'url': 'http://gs.people.com.cn/'},
            'qh': {'name': '青海', 'url': 'http://qh.people.com.cn/'},
            'nx': {'name': '宁夏', 'url': 'http://nx.people.com.cn/'},
            'xj': {'name': '新疆', 'url': 'http://xj.people.com.cn/'},
            'sz': {'name': '深圳', 'url': 'http://sz.people.com.cn/'},
            'xiongan': {'name': '雄安', 'url': 'http://www.rmxiongan.com/'},
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

    def safe_request(self, url, is_json=False, stream=False):
        try:
            if self.debug: print(f"发送请求: {url}")
            time.sleep(1)
            
            resp = requests.get(url, headers=self.headers, timeout=30, stream=stream)
            
            if self.debug:
                print(f"响应状态码: {resp.status_code}")
                print(f"响应头: {dict(resp.headers)}")
            
            resp.raise_for_status()
            if not is_json:
                resp.encoding = chardet.detect(resp.content)['encoding'] or 'utf-8'
            return resp.json() if is_json and resp.text.strip() else resp if resp.text.strip() else None
            
        except Exception as e:
            if self.debug: print(f"请求失败: {url}, 错误: {str(e)}")
            return None

    def clean_text(self, text):
        if not text: return ""
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\xff]', '', text)
        return re.sub(r'\s+', ' ', text).strip()

    def clean_filename(self, filename):
        return re.sub(r'[<>:"/\\|?*]', '_', filename)[:100].strip()

    def get_category_from_url(self, url):
        try:
            domain = urlparse(url).netloc
            path = urlparse(url).path
            
            # 处理子域名
            subdomain = domain.split('.')[0]
            if subdomain != 'www' and subdomain in self.channels:
                return self.channels[subdomain]['name']
            
            # 处理路径
            parts = [p for p in path.split('/') if p]
            if parts:
                for channel_key, channel_info in self.channels.items():
                    if channel_key in parts[0].lower():
                        return channel_info['name']
            
            return "未分类"
        except:
            return "未分类"

    def get_articles_from_homepage(self):
        articles = []
        processed_count = 0
        new_count = 0
        
        # 遍历所有频道
        for channel_key, channel_info in self.channels.items():
            try:
                if self.debug: print(f"\n开始获取频道: {channel_info['name']}")
                resp = self.safe_request(channel_info['url'])
                if not resp: continue
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                for link in soup.find_all('a', href=True):
                    url = link.get('href', '').strip()
                    if not url: continue
                    
                    # 规范化URL
                    url = url if url.startswith('http') else urljoin(channel_info['url'], url)
                    
                    # 检查是否已处理过
                    if url in self.processed_urls:
                        if self.debug: print(f"跳过已处理的URL: {url}")
                        processed_count += 1
                        continue
                    
                    if not any(domain in url for domain in ['people.com.cn', 'people.cn']) or \
                       not any(ext in url for ext in ['.html', '.htm', '.shtml']): continue
                    
                    title = self.clean_text(link.get_text())
                    if not title or len(title) < 4: continue
                    
                    # 使用频道名称作为分类
                    articles.append({
                        'title': title,
                        'url': url,
                        'category': channel_info['name']
                    })
                    new_count += 1
                    if self.debug: print(f"找到新文章: {title}")
                
                print(f"已获取 {channel_info['name']} 频道的文章")
                time.sleep(1)  # 避免请求过快
                
            except Exception as e:
                if self.debug: print(f"处理频道失败: {channel_info['name']}, {str(e)}")
        
        print(f"\n文章统计:")
        print(f"新文章: {new_count} 篇")
        print(f"已处理文章: {processed_count} 篇")
        return articles

    def process_media(self, url, save_path, m3u8_url=None):
        """处理媒体文件下载（图片或视频）"""
        try:
            if self.debug: print(f"开始下载媒体: {url}")
            
            # 处理URL编码和特殊字符
            url = unquote(url)
            if '],poster:' in url:
                url = re.sub(r'\],poster:.*$', '', url)
            
            # 使用stream模式下载
            resp = self.safe_request(url, stream=True)
            if not resp: return False
            
            # 获取文件大小
            file_size = int(resp.headers.get('content-length', 0))
            if self.debug: print(f"文件大小: {file_size/1024/1024:.2f}MB")
            
            if file_size < 1000:  # 小于1KB可能是无效文件
                if self.debug: print("文件太小，可能是无效的媒体文件")
                return False
            
            # 创建保存目录
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # 分块下载文件
            with open(save_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # 验证下载的文件
            if os.path.exists(save_path) and os.path.getsize(save_path) == file_size:
                if self.debug: print(f"媒体下载成功: {save_path}")
                return True
            else:
                if self.debug: print("文件大小不匹配，下载可能不完整")
                if os.path.exists(save_path):
                    os.remove(save_path)
                return False
                
        except Exception as e:
            if self.debug: print(f"下载失败: {url}, 错误: {str(e)}")
            if os.path.exists(save_path):
                os.remove(save_path)
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
        """处理视频下载"""
        try:
            if self.debug: print(f"开始处理视频: {url}")
            
            # 处理URL编码和特殊字符
            url = unquote(url)
            if '],poster:' in url:
                url = re.sub(r'\],poster:.*$', '', url)
            
            if '.m3u8' in url.lower():
                return self.process_m3u8(url, save_path)
            else:
                # 对于直接的视频文件，使用process_media下载
                if self.debug: print(f"直接下载视频文件: {url}")
                return self.process_media(url, save_path)
                
        except Exception as e:
            if self.debug: print(f"视频处理失败: {url}, 错误: {str(e)}")
            return False

    def find_videos(self, soup):
        video_urls = set()
        
        # 从video标签和source标签获取
        for video in soup.find_all(['video', 'iframe']):
            if src := video.get('src'): 
                video_urls.add(src)
            for source in video.find_all('source'):
                if src := source.get('src'): 
                    video_urls.add(src)
        
        # 从特定属性获取
        video_attrs = ['data-video', 'data-src', 'data-url', 'src', 'data-mp4', 
                      'data-m3u8', 'data-hls', 'data-video-url', 'video-url']
        for attr in video_attrs:
            for elem in soup.find_all(attrs={attr: True}):
                if url := elem[attr]:
                    if any(ext in url.lower() for ext in ['.mp4', '.m3u8', '.flv', '.ts']):
                        video_urls.add(url)
        
        # 从script标签获取
        video_patterns = [
            r'https?://[^\s<>"]+?\.(?:mp4|m3u8|flv)[^\s<>"]*',  # 直接视频链接
            r'videoUrl\s*[=:]\s*[\'"]([^\'"]+)[\'"]',  # videoUrl变量
            r'video_url\s*[=:]\s*[\'"]([^\'"]+)[\'"]',  # video_url变量
            r'playUrl\s*[=:]\s*[\'"]([^\'"]+)[\'"]',    # playUrl变量
            r'url\s*[=:]\s*[\'"]([^\'"]+\.(?:mp4|m3u8|flv))[\'"]'  # 视频url变量
        ]
        
        for script in soup.find_all('script'):
            if script.string:
                for pattern in video_patterns:
                    matches = re.findall(pattern, script.string, re.I)
                    for match in matches:
                        if isinstance(match, tuple):
                            match = match[0]  # 如果是组匹配，取第一个组
                        if match and any(ext in match.lower() for ext in ['.mp4', '.m3u8', '.flv', '.ts']):
                            video_urls.add(match)
        
        # 检查特定的视频容器div
        video_containers = soup.find_all('div', class_=lambda x: x and any(name in str(x).lower() 
            for name in ['video', 'player', 'media-player', 'video-container']))
        
        for container in video_containers:
            # 检查data属性
            for attr, value in container.attrs.items():
                if attr.startswith('data-') and isinstance(value, str):
                    if any(ext in value.lower() for ext in ['.mp4', '.m3u8', '.flv', '.ts']):
                        video_urls.add(value)
        
        return video_urls

    def process_article(self, article):
        """处理单篇文章"""
        try:
            # 再次检查是否已处理过（双重检查，以防并发）
            if article['url'] in self.processed_urls:
                if self.debug: print(f"跳过已处理的文章: {article['title']}")
                return False

            # 检查文章目录是否已存在
            date_str = datetime.now().strftime('%Y%m%d')
            article_dir = os.path.join(self.base_dir, date_str, 
                                     article['category'],
                                     self.clean_filename(article['title']))
            
            if os.path.exists(article_dir):
                if self.debug: print(f"文章目录已存在，跳过: {article['title']}")
                self.processed_urls.add(article['url'])  # 添加到已处理列表
                return False

            resp = self.safe_request(article['url'])
            if not resp: return False

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 创建文章目录
            os.makedirs(article_dir, exist_ok=True)

            # 获取正文
            content = None
            for selector in ['div.rm_txt_con', 'div#rwb_zw', 'div.show_text',
                           'div.content', 'div.article', 'div#p_content']:
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

                # 保存图片和视频
                media_saved = False
                
                # 保存图片
                for img in soup.find_all('img'):
                    if img_url := img.get('src') or img.get('data-original') or img.get('data-src'):
                        # 跳过小图标、按钮等
                        skip_patterns = ['ico', 'logo', 'banner', 'btn', 'button', 'icon', 'avatar']
                        if any(pattern in img_url.lower() for pattern in skip_patterns): 
                            continue
                            
                        # 检查图片尺寸属性
                        width = img.get('width', '').strip('px') or img.get('data-width', '').strip('px')
                        height = img.get('height', '').strip('px') or img.get('data-height', '').strip('px')
                        if width and height:
                            try:
                                if int(width) < 100 or int(height) < 100:  # 跳过小图片
                                    continue
                            except ValueError:
                                pass
                                
                        img_url = img_url if img_url.startswith('http') else urljoin(article['url'], img_url)
                        ext = img_url.split('.')[-1].lower() if '.' in img_url else 'jpg'
                        if ext not in ['jpg', 'jpeg', 'png', 'gif']: ext = 'jpg'
                        img_path = os.path.join(article_dir, f"{self.clean_filename(article['title'])}_{len([f for f in os.listdir(article_dir) if f.endswith(('.jpg','.jpeg','.png','.gif'))])}.{ext}")
                        if self.process_media(img_url, img_path):
                            media_saved = True

                # 保存视频
                video_dir = os.path.join(article_dir, "视频")
                os.makedirs(video_dir, exist_ok=True)
                video_urls = self.find_videos(soup)
                if video_urls:
                    for video_url in video_urls:
                        video_url = video_url if video_url.startswith('http') else urljoin(article['url'], video_url)
                        ext = 'mp4' if '.m3u8' not in video_url.lower() else 'm3u8'
                        video_path = os.path.join(video_dir, f"{self.clean_filename(article['title'])}_{len(os.listdir(video_dir))}.{ext}")
                        if self.process_video(video_url, video_path):
                            print(f"成功下载视频: {video_url}")
                            media_saved = True
                        else:
                            print(f"视频下载失败: {video_url}")

                # 只有当文章内容保存成功时，才标记为已处理
                if media_saved:
                    self.processed_urls.add(article['url'])
                    self.save_processed_urls()  # 及时保存已处理URL列表
                    return True
                else:
                    # 如果没有保存到任何媒体文件，删除文章目录
                    if os.path.exists(article_dir):
                        import shutil
                        shutil.rmtree(article_dir)
                    return False
                
        except Exception as e:
            if self.debug: print(f"处理文章失败: {article['title']}, {str(e)}")
            # 清理可能部分创建的目录
            if os.path.exists(article_dir):
                import shutil
                shutil.rmtree(article_dir)
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
        spider = PeopleIncrementalSpider()
        spider.debug = os.environ.get('DEBUG', '').lower() in ('true', '1', 'yes')
        spider.run_once()
    except Exception as e:
        print(f"爬取过程出错: {str(e)}")
        if spider.debug:
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    # 确保基础目录存在
    os.makedirs("人民网", exist_ok=True)
    
    # 设置定时任务
    schedule.every().day.at("00:00").do(run_spider)  # 每天0点
    schedule.every().day.at("08:00").do(run_spider)  # 每天8点
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