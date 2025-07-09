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

class SinaIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://news.sina.com.cn/',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        }
        self.base_url = "https://news.sina.com.cn"
        self.base_dir = "新浪网"
        self.processed_file = os.path.join(self.base_dir, "processed_articles.json")  # 移到前面
        self.processed_urls = self.load_processed_urls()
        self.debug = True  # 默认开启调试模式
        self.stats = {
            'total_processed': 0,
            'skipped': 0,
            'new': 0,
            'errors': 0
        }
        
        # 频道分类
        self.channels = {
            # 新闻频道
            'news': {'name': '新闻', 'url': 'https://news.sina.com.cn/'},
            'china': {'name': '国内', 'url': 'https://news.sina.com.cn/china/'},
            'china_shxw': {'name': '国内社会新闻', 'url': 'https://news.sina.com.cn/society/'},
            'china_gjxw': {'name': '国内国际新闻', 'url': 'https://news.sina.com.cn/world/'},
            
            # 军事频道
            'mil': {'name': '军事', 'url': 'https://mil.news.sina.com.cn/'},
            'mil_zonghe': {'name': '军事综合', 'url': 'https://mil.news.sina.com.cn/zonghe/'},
            'mil_dgby': {'name': '军事独家报道', 'url': 'https://mil.news.sina.com.cn/dgby/'},
            'mil_jshm': {'name': '军事解码', 'url': 'https://mil.news.sina.com.cn/jshm/'},
            
            # 财经频道
            'finance': {'name': '财经', 'url': 'https://finance.sina.com.cn/'},
            'finance_stock': {'name': '股市', 'url': 'https://finance.sina.com.cn/stock/'},
            'finance_money': {'name': '理财', 'url': 'https://finance.sina.com.cn/money/'},
            'finance_china': {'name': '国内财经', 'url': 'https://finance.sina.com.cn/china/'},
            
            # 科技频道
            'tech': {'name': '科技', 'url': 'https://tech.sina.com.cn/'},
            'tech_it': {'name': 'IT业', 'url': 'https://tech.sina.com.cn/it/'},
            'tech_mobile': {'name': '手机通信', 'url': 'https://tech.sina.com.cn/mobile/'},
            'tech_discovery': {'name': '科技探索', 'url': 'https://tech.sina.com.cn/discovery/'},
            
            # 体育频道
            'sports': {'name': '体育', 'url': 'https://sports.sina.com.cn/'},
            'sports_nba': {'name': 'NBA', 'url': 'https://sports.sina.com.cn/nba/'},
            'sports_csl': {'name': '中超', 'url': 'https://sports.sina.com.cn/china/'},
            'sports_global': {'name': '国际足球', 'url': 'https://sports.sina.com.cn/global/'},
            
            # 娱乐频道
            'ent': {'name': '娱乐', 'url': 'https://ent.sina.com.cn/'},
            'ent_star': {'name': '明星', 'url': 'https://ent.sina.com.cn/star/'},
            'ent_film': {'name': '电影', 'url': 'https://ent.sina.com.cn/film/'},
            'ent_tv': {'name': '电视', 'url': 'https://ent.sina.com.cn/tv/'},
            
            # 其他频道
            'edu': {'name': '教育', 'url': 'https://edu.sina.com.cn/'},
            'auto': {'name': '汽车', 'url': 'https://auto.sina.com.cn/'},
            'games': {'name': '游戏', 'url': 'https://games.sina.com.cn/'},
            'travel': {'name': '旅游', 'url': 'https://travel.sina.com.cn/'},
        }
        
    def load_processed_urls(self):
        """加载已处理的URL和文章信息"""
        processed_urls = set()
        
        try:
            # 确保目录存在
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
                        # 合并旧的和新的URL集合
                        processed_urls.update(article['url'] for article in processed_articles)
                except Exception as e:
                    if self.debug: print(f"加载processed_articles.json失败: {str(e)}")
        except Exception as e:
            if self.debug: print(f"加载已处理URL记录失败: {str(e)}")

        return processed_urls
        
    def save_processed_urls(self):
        """保存已处理的URL和文章信息"""
        # 保存旧版本的processed_urls.pkl
        urls_file = Path(self.base_dir) / 'processed_urls.pkl'
        urls_file.parent.mkdir(parents=True, exist_ok=True)
        with open(urls_file, 'wb') as f:
            pickle.dump(self.processed_urls, f)

        # 保存新版本的processed_articles.json
        try:
            processed_articles = []
            # 遍历已处理的URL，收集文章信息
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
        try:
            if self.debug: print(f"发送请求: {url}")
            time.sleep(1)
            
            resp = requests.get(url, headers=self.headers, timeout=30, stream=stream)
            
            if self.debug:
                print(f"响应状态码: {resp.status_code}")
                print(f"响应头: {dict(resp.headers)}")
            
            resp.raise_for_status()
            
            # 优化编码处理
            if not stream:
                # 1. 首先尝试从Content-Type中获取编码
                content_type = resp.headers.get('Content-Type', '').lower()
                if 'charset=' in content_type:
                    charset = content_type.split('charset=')[-1].strip()
                    resp.encoding = charset
                else:
                    # 2. 如果没有指定编码，尝试从内容检测
                    content_bytes = resp.content
                    detected = chardet.detect(content_bytes)
                    if detected and detected['confidence'] > 0.8:
                        resp.encoding = detected['encoding']
                    else:
                        # 3. 如果检测不准确，默认使用UTF-8
                        resp.encoding = 'utf-8'
            
            return resp
            
        except Exception as e:
            if self.debug: print(f"请求失败: {url}, 错误: {str(e)}")
            return None

    def clean_text(self, text):
        """清理文本内容"""
        if not text: return ""
        
        # 移除特殊字符
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\xff]', '', text)
        # 移除多余空白字符
        text = re.sub(r'\s+', ' ', text)
        # 移除零宽字符
        text = re.sub(r'[\u200b-\u200f\u202a-\u202e\uFEFF]', '', text)
        # 移除控制字符
        text = re.sub(r'[\u0000-\u001f\u007f-\u009f]', '', text)
        
        return text.strip()

    def clean_filename(self, filename):
        return re.sub(r'[<>:"/\\|?*]', '_', filename)[:100].strip()

    def get_date_from_url(self, url):
        """从URL中提取日期"""
        try:
            match = re.search(r'/(\d{4}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01]))/', url)
            if match:
                return match.group(1)
            return datetime.now().strftime('%Y%m%d')
        except:
            return datetime.now().strftime('%Y%m%d')

    def is_article_processed(self, url, title=None):
        """检查文章是否已处理，支持多重检查"""
        try:
            # 1. 检查URL是否在已处理集合中
            if url in self.processed_urls:
                if self.debug: 
                    print(f"跳过已处理的URL: {url}")
                    if title:
                        print(f"标题: {title}")
                self.stats['skipped'] += 1
                return True

            # 2. 检查文章目录是否存在
            if title:
                date_str = self.get_date_from_url(url)
                article_dir = os.path.join(self.base_dir, date_str, 
                                         self.clean_filename(title))
                if os.path.exists(article_dir):
                    if self.debug: 
                        print(f"文章目录已存在: {article_dir}")
                        print(f"标题: {title}")
                    self.processed_urls.add(url)  # 添加到已处理集合
                    self.save_processed_urls()
                    self.stats['skipped'] += 1
                    return True

            self.stats['new'] += 1
            return False
        except Exception as e:
            if self.debug: print(f"检查文章状态失败: {str(e)}")
            self.stats['errors'] += 1
            return False

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
                
                # 查找文章链接
                for link in soup.find_all('a', href=True):
                    url = link.get('href', '').strip()
                    if not url: continue
                    
                    # 规范化URL
                    url = url if url.startswith('http') else urljoin(channel_info['url'], url)
                    
                    # 检查是否是新浪的文章页面
                    if not any(domain in url for domain in ['sina.com.cn', 'sina.cn']) or \
                       not any(ext in url for ext in ['.html', '.shtml']): continue
                    
                    # 获取标题并清理
                    title = self.clean_text(link.get_text())
                    if not title or len(title) < 4: continue
                    
                    # 检查标题是否包含无效字符
                    if re.search(r'[^\u4e00-\u9fff\w\s\-_.,?!()（）《》【】：，。？！、]+', title):
                        if self.debug: print(f"跳过包含无效字符的标题: {title}")
                        continue
                    
                    # 检查是否已处理
                    if self.is_article_processed(url, title):
                        processed_count += 1
                        continue
                    
                    # 从URL中提取日期
                    date_str = self.get_date_from_url(url)
                    
                    articles.append({
                        'title': title,
                        'url': url,
                        'category': channel_info['name'],
                        'date': date_str
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

    def process_media(self, url, save_path):
        """处理媒体文件下载（图片或视频）"""
        try:
            if self.debug: print(f"开始下载媒体: {url}")
            
            # 处理URL编码和特殊字符
            url = unquote(url)
            
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

    def find_videos(self, soup):
        video_urls = set()
        
        try:
            # 1. 直接从页面提取视频URL
            video_patterns = [
                r'https?://[^"\'\s<>]+?\.(?:mp4|m3u8)[^"\'\s<>]*',
                r'videoPath\s*:\s*[\'"]([^\'"]+)[\'"]',
                r'videoUrl\s*:\s*[\'"]([^\'"]+)[\'"]',
                r'playUrl\s*:\s*[\'"]([^\'"]+)[\'"]',
                r'url\s*:\s*[\'"]([^\'"]+\.(?:mp4|m3u8))[\'"]'
            ]
            
            # 搜索整个页面源码
            page_text = str(soup)
            for pattern in video_patterns:
                matches = re.findall(pattern, page_text, re.I)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    if match and '.mp4' in match.lower():
                        video_urls.add(match)
            
            # 2. 检查特定的视频容器
            video_containers = [
                'div.video-player',
                'div.video-wrapper',
                'div.video-container',
                'div#video_box',
                'div.video_box',
                'div.video-content'
            ]
            
            for container in video_containers:
                if video_div := soup.select_one(container):
                    # 检查data属性
                    for attr, value in video_div.attrs.items():
                        if isinstance(value, str) and ('.mp4' in value.lower() or '.m3u8' in value.lower()):
                            video_urls.add(value)
            
            # 3. 处理相对路径
            final_urls = set()
            for url in video_urls:
                if url.startswith('//'):
                    url = 'https:' + url
                elif not url.startswith('http'):
                    url = urljoin('https://video.sina.com.cn/', url)
                final_urls.add(url)
            
            if self.debug and final_urls:
                print(f"找到视频URL: {final_urls}")
            
            return final_urls
            
        except Exception as e:
            if self.debug: print(f"查找视频失败: {str(e)}")
            return video_urls

    def get_content_selectors(self, url):
        """根据URL返回对应的内容选择器"""
        selectors = {
            'default': ['div.article', 'div#artibody', 'div.article-content',
                       'div.content', 'div.main-content', 'article'],
            'mil.news': ['div.article', 'div#article', 'div.art_content', 'div.content'],
            'tech.sina': ['div.article', 'div.art_content', 'div.tech-content'],
            'sports.sina': ['div.article', 'div#artibody', 'div.sports_content'],
            'finance.sina': ['div.article', 'div#artibody', 'div.finance_content'],
            'ent.sina': ['div.article', 'div.ent-content', 'div#artibody'],
        }
        
        # 根据URL选择对应的选择器
        for key, value in selectors.items():
            if key in url:
                return value
        return selectors['default']

    def process_article(self, article):
        """处理单篇文章"""
        try:
            # 再次检查是否已处理过（双重检查）
            if self.is_article_processed(article['url'], article['title']):
                if self.debug: print(f"跳过已处理的文章: {article['title']}")
                return False

            # 检查文章目录是否已存在
            article_dir = os.path.join(self.base_dir, article['date'], 
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
            # 根据URL获取对应的内容选择器
            content_selectors = self.get_content_selectors(article['url'])
            for selector in content_selectors:
                if content := soup.select_one(selector):
                    break

            if not content: return False

            # 清理内容
            # 扩展需要清理的标签
            cleanup_tags = [
                'script', 'style', 
                'div.article-footer', 'div.statement', 'div.share-btns',
                'div.article-bottom', 'div.related-news', 'div.recommend',
                'div.hot-news', 'div.article-notice', 'div.weibo-card',
                'div.article-tags', 'div.article-editor'
            ]
            for tag in content.find_all(cleanup_tags):
                tag.decompose()
            
            # 清理广告和推荐内容
            ad_patterns = ['广告', '推荐阅读', '相关阅读', '热门推荐', '更多精彩']
            for pattern in ad_patterns:
                for elem in content.find_all(text=lambda text: pattern in str(text)):
                    parent = elem.parent
                    if parent: parent.decompose()
            
            text = self.clean_text(content.get_text('\n'))
            
            # 获取更多元数据
            metadata = {
                'publish_time': '',
                'source': '',
                'author': '',
                'editor': ''
            }
            
            # 时间
            time_selectors = ['div.date-source span.date', 'div.time-source', 'p.origin',
                            'span.time', 'div.article-info span.time']
            for selector in time_selectors:
                if time_elem := soup.select_one(selector):
                    metadata['publish_time'] = self.clean_text(time_elem.get_text())
                    break
            
            # 来源
            source_selectors = ['div.date-source a.source', 'span.source', 'a.source',
                              'div.article-info span.source']
            for selector in source_selectors:
                if source_elem := soup.select_one(selector):
                    metadata['source'] = self.clean_text(source_elem.get_text())
                    break
            
            # 作者
            author_selectors = ['div.author', 'p.author', 'span.author']
            for selector in author_selectors:
                if author_elem := soup.select_one(selector):
                    metadata['author'] = self.clean_text(author_elem.get_text())
                    break
            
            if len(text) > 50:
                # 生成文件名前缀（使用清理过的标题）
                file_prefix = self.clean_filename(article['title'])
                
                # 保存文章
                article_file = os.path.join(article_dir, f"{file_prefix}_正文.txt")
                with open(article_file, 'w', encoding='utf-8') as f:
                    f.write(f"标题：{article['title']}\n")
                    f.write(f"日期：{article['date']}\n")
                    if metadata['publish_time']:
                        f.write(f"发布时间：{metadata['publish_time']}\n")
                    if metadata['source']:
                        f.write(f"来源：{metadata['source']}\n")
                    if metadata['author']:
                        f.write(f"作者：{metadata['author']}\n")
                    f.write(f"分类：{article['category']}\n")
                    f.write(f"链接：{article['url']}\n")
                    f.write("-" * 50 + "\n\n" + text)

                # 保存图片和视频
                media_saved = False
                
                # 保存图片
                img_count = 1
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
                        if img_url.startswith('//'):
                            img_url = 'https:' + img_url
                            
                        ext = img_url.split('.')[-1].lower() if '.' in img_url else 'jpg'
                        if ext not in ['jpg', 'jpeg', 'png', 'gif']: ext = 'jpg'
                        
                        # 使用文章标题作为图片文件名前缀
                        img_path = os.path.join(article_dir, f"{file_prefix}_图片_{img_count}.{ext}")
                        if self.process_media(img_url, img_path):
                            img_count += 1
                            media_saved = True

                # 保存视频
                video_count = 1
                video_urls = self.find_videos(soup)
                if video_urls:
                    for video_url in video_urls:
                        if self.debug: print(f"处理视频: {video_url}")
                        video_url = video_url if video_url.startswith('http') else urljoin(article['url'], video_url)
                        if video_url.startswith('//'):
                            video_url = 'https:' + video_url
                            
                        ext = 'mp4'
                        # 使用文章标题作为视频文件名前缀
                        video_path = os.path.join(article_dir, f"{file_prefix}_视频_{video_count}.{ext}")
                        if self.process_media(video_url, video_path):
                            if self.debug: print(f"视频保存成功: {video_path}")
                            video_count += 1
                            media_saved = True
                        else:
                            if self.debug: print(f"视频保存失败: {video_url}")

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
    """运行爬虫的包装函数"""
    try:
        spider = SinaIncrementalSpider()
        spider.debug = True  # 始终开启调试模式
        spider.run_once()
    except Exception as e:
        print(f"爬取过程出错: {str(e)}")
        if spider.debug:
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    # 确保基础目录存在
    os.makedirs("新浪网", exist_ok=True)
    
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