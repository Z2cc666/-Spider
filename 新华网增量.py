import requests as rq
from bs4 import BeautifulSoup
import re, os, time, json
import datetime
from datetime import timedelta
from urllib.parse import urljoin
import urllib3
import schedule

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class XinhuaSpider:
    def __init__(self):
        self.base_dir = "新华网数据"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.news.cn/'
        }
        self.processed_file = os.path.join(self.base_dir, 'processed_articles.json')
        os.makedirs(self.base_dir, exist_ok=True)
        if not os.path.exists(self.processed_file):
            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump({}, f)
        
        # 分类字典
        self.categories = {
            # 时政外交
            'politics': '时政',
            'leaders': '高层',
            'renshi': '人事',
            'xijinping': '习近平',
            'zhengce': '政务',
            'zonghe': '中央文件',
            
            # 国际
            'world': '国际',
            'asia': '亚太',
            'globallink': '全球连线',
            
            # 财经
            'fortune': '财经',
            'finance': '金融',
            'auto': '汽车',
            'energy': '能源',
            'company': '上市公司',
            'digital': '数字经济',
            
            # 评论
            'comments': '网评',
            'sikao': '思客',
            'zhiku': '智库',
            
            # 港澳台
            'gangao': '港澳',
            'tw': '台湾',
            
            # 教育科技
            'edu': '教育',
            'tech': '科技',
            'science': '科创',
            'quantum': '量子',
            'info': '信息化',
            'academic': '学术',
            
            # 文化
            'culture': '文化',
            'art': '书画',
            'travel': '旅游',
            'expo': '会展',
            'ent': '娱乐',
            'fashion': '时尚',
            'reading': '悦读',
            'industry': '文化产业',
            
            # 健康生活
            'health': '健康',
            'food': '食品',
            'life': '生活',
            'house': '人居',
            'lottery': '彩票',
            
            # 其他专题
            'sports': '体育',
            'mil': '军事',
            'video': '视频',
            'photo': '图片',
            'interview': '访谈',
            'gongyi': '公益',
            
            # 专题报道
            'silkroad': '一带一路',
            'rural': '乡村振兴',
            'city': '中国城市',
            'source': '溯源中国',
        }
        
    def is_article_processed(self, url):
        try:
            with open(self.processed_file, 'r', encoding='utf-8') as f:
                return url in json.load(f)
        except: return False
        
    def mark_article_processed(self, url, title, category, save_path):
        try:
            with open(self.processed_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            data[url] = {
                'title': title,
                'category': category,
                'date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'path': save_path
            }
            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"标记文章失败: {str(e)}")
        
    def clean_old_records(self, days=30):
        try:
            with open(self.processed_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            current = datetime.datetime.now()
            cleaned = {url: info for url, info in data.items() 
                      if current - datetime.datetime.strptime(info['date'], '%Y-%m-%d %H:%M:%S') < timedelta(days=days)}
            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump(cleaned, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"清理记录失败: {str(e)}")

    def safe_request(self, url, retry=1):
        try:
            time.sleep(1)
            response = rq.get(url, headers=self.headers, verify=False, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response
        except Exception as e:
            if retry > 0:
                time.sleep(2)
                return self.safe_request(url, retry-1)
            print(f"请求失败: {url}, 错误: {str(e)}")
            return None

    def get_category_from_url(self, url):
        """根据URL获取分类名称"""
        try:
            # 视频页面特殊处理
            if '/video/' in url.lower():
                return '视频'
                
            # 从URL中提取分类标识
            for category_key, category_name in self.categories.items():
                if f'/{category_key}/' in url.lower():
                    return category_name
            
            # 默认分类
            return '其他'
        except:
            return '其他'

    def get_xinhua_news(self, url):
        try:
            response = self.safe_request(url)
            if not response: return None
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取日期和分类
            date_str = re.search(r'/(\d{4})[-/](\d{2})[-/](\d{2})/', url)
            date_str = f"{date_str.group(1)}-{date_str.group(2)}-{date_str.group(3)}" if date_str else datetime.datetime.now().strftime('%Y-%m-%d')
            category = self.get_category_from_url(url)
            
            # 获取标题
            for selector in ['span.title', 'h1.main-title', 'div.head-title', 'h1#title', 'div.video-title', 'h1', 'title']:
                if title_elem := soup.select_one(selector):
                    if (title := title_elem.get_text(strip=True)) and len(title) > 5:
                        break
            else: return None
            
            # 获取内容
            content = []
            if '/video/' in url.lower():
                # 视频页面
                for selector in ['div.video-desc', 'div.desc', 'div.summary', 'div.video-summary', 'p.video-description']:
                    if desc := soup.select_one(selector):
                        if text := desc.get_text(strip=True):
                            content.append(text)
                            break
                if not content:
                    content.append(title)
            else:
                # 普通页面
                if main := soup.select_one('div#detail, div.main-content, div.article, div.content, article'):
                    content = [p.get_text(strip=True) for p in main.find_all('p')
                             if (text := p.get_text(strip=True)) and not re.match(r'^[\s\d]*$', text)]
                if not content or len('\n'.join(content)) < 100:
                    return None
            
            # 保存内容
            article_dir = os.path.join(self.base_dir, date_str, category, re.sub(r'[<>:"/\\|?*]', '_', title)[:100].strip())
            os.makedirs(article_dir, exist_ok=True)
            with open(os.path.join(article_dir, f"{title[:100]}.txt"), 'w', encoding='utf-8') as f:
                f.write(f"标题：{title}\n链接：{url}\n日期：{date_str}\n分类：{category}\n\n正文：\n{''.join(content)}")
            
            # 下载图片和视频
            if main := soup.select_one('div.main-content, div.article, div#detail, div.video-box'):
                # 图片
                saved = set()
                for i, img in enumerate(main.find_all('img'), 1):
                    if not (src := img.get('src')): continue
                    if src in saved: continue
                    src = urljoin(url, src) if not src.startswith('http') else src
                    if any(skip in src.lower() for skip in ['ad', 'logo', 'icon', 'banner']): continue
                    if not src.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')): continue
                    try:
                        if resp := self.safe_request(src):
                            if len(resp.content) > 50 * 1024:
                                ext = os.path.splitext(src)[1].lower() or '.jpg'
                                with open(os.path.join(article_dir, f"图片_{i}{ext}"), 'wb') as f:
                                    f.write(resp.content)
                                saved.add(src)
                    except Exception as e:
                        print(f"下载图片失败: {src}, {str(e)}")
                
                # 视频
                if video_urls := self.extract_video_url(soup, url):
                    video_dir = os.path.join(article_dir, "视频")
                    os.makedirs(video_dir, exist_ok=True)
                    for i, v_url in enumerate(video_urls, 1):
                        ext = os.path.splitext(v_url)[1].lower()
                        ext = ext if ext in ['.mp4', '.m3u8', '.flv'] else '.mp4'
                        path = os.path.join(video_dir, f"视频_{i}{ext}")
                        if self.download_video(v_url, path):
                            print(f"视频下载成功: {path}")
                        else:
                            print(f"视频下载失败: {v_url}")
                            if os.path.exists(path): os.remove(path)
            
            return title, category, ''.join(content), article_dir
            
        except Exception as e:
            print(f"处理新闻失败: {url}, 错误: {str(e)}")
            return None

    def extract_video_url(self, soup, url):
        video_urls = []
        try:
            # 从页面提取视频信息
            for script in soup.find_all('script'):
                if not script.string: continue
                if 'window.videoInfo' in script.string:
                    try:
                        if match := re.search(r'window\.videoInfo\s*=\s*({[^;]+})', script.string):
                            info = json.loads(match.group(1))
                            if video_url := info.get('url') or info.get('videoUrl') or info.get('videoPath'):
                                video_urls.append(f"https:{video_url}" if not video_url.startswith('http') else video_url)
                            if vid := info.get('vid') or info.get('videoId'):
                                # 尝试API
                                for api in [f'http://player.news.cn/api/v1/getVideoById?vid={vid}',
                                          f'http://videoms.news.cn/api/v1/getVideoById?vid={vid}',
                                          f'http://video.news.cn/api/v1/getVideoById?vid={vid}']:
                                    try:
                                        if resp := self.safe_request(api):
                                            if data := resp.json().get('data'):
                                                for field in ['videoPath', 'videoUrl', 'url', 'hdUrl', 'sdUrl']:
                                                    if v_url := data.get(field):
                                                        if isinstance(v_url, str):
                                                            v_url = f"https:{v_url}" if not v_url.startswith('http') else v_url
                                                            if v_url not in video_urls:
                                                                video_urls.append(v_url)
                                    except: continue
                    except: continue
            
            # 从video标签获取
            for video in soup.find_all(['video', 'source']):
                if src := video.get('src'):
                    src = urljoin(url, src) if not src.startswith('http') else src
                    if src not in video_urls:
                        video_urls.append(src)
            
            return [url for url in set(video_urls) if any(ext in url.lower() for ext in ['.mp4', '.m3u8', '.flv'])]
        except Exception as e:
            print(f"提取视频URL失败: {str(e)}")
            return []

    def download_video(self, url, path):
        try:
            if url.endswith('.m3u8'):
                os.system(f'ffmpeg -i "{url}" -c copy "{path}" -y')
                return os.path.exists(path)
            
            response = rq.get(url, headers=self.headers, stream=True)
            response.raise_for_status()
            
            if int(response.headers.get('content-length', 0)) < 1024 * 1024:
                return False
                
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk: f.write(chunk)
            return True
        except Exception as e:
            print(f"下载视频失败: {url}, 错误: {str(e)}")
            if os.path.exists(path): os.remove(path)
            return False

def main():
    spider = XinhuaSpider()
    spider.clean_old_records()
    
    # 增量爬取的分类
    start_urls = {
        # 时政外交
        'politics': {'name': '时政', 'url': 'http://www.news.cn/politics/'},
        'leaders': {'name': '高层', 'url': 'http://www.news.cn/politics/leaders/'},
        'renshi': {'name': '人事', 'url': 'http://www.news.cn/renshi/'},
        'xijinping': {'name': '习近平', 'url': 'http://www.news.cn/politics/xijinping/'},
        'zhengce': {'name': '政务', 'url': 'http://www.news.cn/politics/zhengce/'},
        'zonghe': {'name': '中央文件', 'url': 'http://www.news.cn/politics/zonghe/'},
        
        # 国际
        'world': {'name': '国际', 'url': 'http://www.news.cn/world/'},
        'asia': {'name': '亚太', 'url': 'http://www.news.cn/asia/'},
        'globallink': {'name': '全球连线', 'url': 'http://www.news.cn/globallink/'},
        
        # 财经
        'fortune': {'name': '财经', 'url': 'http://www.news.cn/fortune/'},
        'finance': {'name': '金融', 'url': 'http://www.news.cn/finance/'},
        'auto': {'name': '汽车', 'url': 'http://www.news.cn/auto/'},
        'energy': {'name': '能源', 'url': 'http://www.news.cn/energy/'},
        'company': {'name': '上市公司', 'url': 'http://www.news.cn/fortune/company/'},
        'digital': {'name': '数字经济', 'url': 'http://www.news.cn/fortune/digital/'},
        
        # 评论
        'comments': {'name': '网评', 'url': 'http://www.news.cn/comments/'},
        'sikao': {'name': '思客', 'url': 'http://www.news.cn/sikao/'},
        'zhiku': {'name': '智库', 'url': 'http://www.news.cn/zhiku/'},
        
        # 港澳台
        'gangao': {'name': '港澳', 'url': 'http://www.news.cn/gangao/'},
        'tw': {'name': '台湾', 'url': 'http://www.news.cn/tw/'},
        
        # 教育科技
        'edu': {'name': '教育', 'url': 'http://www.news.cn/edu/'},
        'tech': {'name': '科技', 'url': 'http://www.news.cn/tech/'},
        'science': {'name': '科创', 'url': 'http://www.news.cn/science/'},
        'quantum': {'name': '量子', 'url': 'http://www.news.cn/tech/quantum/'},
        'info': {'name': '信息化', 'url': 'http://www.news.cn/info/'},
        'academic': {'name': '学术', 'url': 'http://www.news.cn/tech/academic/'},
        
        # 文化
        'culture': {'name': '文化', 'url': 'http://www.news.cn/culture/'},
        'art': {'name': '书画', 'url': 'http://www.news.cn/art/'},
        'travel': {'name': '旅游', 'url': 'http://www.news.cn/travel/'},
        'expo': {'name': '会展', 'url': 'http://www.news.cn/expo/'},
        'ent': {'name': '娱乐', 'url': 'http://www.news.cn/ent/'},
        'fashion': {'name': '时尚', 'url': 'http://www.news.cn/fashion/'},
        'reading': {'name': '悦读', 'url': 'http://www.news.cn/reading/'},
        'industry': {'name': '文化产业', 'url': 'http://www.news.cn/culture/industry/'},
        
        # 健康生活
        'health': {'name': '健康', 'url': 'http://www.news.cn/health/'},
        'food': {'name': '食品', 'url': 'http://www.news.cn/food/'},
        'life': {'name': '生活', 'url': 'http://www.news.cn/life/'},
        'house': {'name': '人居', 'url': 'http://www.news.cn/house/'},
        'lottery': {'name': '彩票', 'url': 'http://www.news.cn/lottery/'},
        
        # 其他专题
        'sports': {'name': '体育', 'url': 'http://www.news.cn/sports/'},
        'mil': {'name': '军事', 'url': 'http://www.news.cn/mil/'},
        'video': {'name': '视频', 'url': 'http://www.news.cn/video/'},
        'photo': {'name': '图片', 'url': 'http://www.news.cn/photo/'},
        'interview': {'name': '访谈', 'url': 'http://www.news.cn/interview/'},
        'gongyi': {'name': '公益', 'url': 'http://www.news.cn/gongyi/'},
        
        # 专题报道
        'silkroad': {'name': '一带一路', 'url': 'http://www.news.cn/silkroad/'},
        'rural': {'name': '乡村振兴', 'url': 'http://www.news.cn/rural/'},
        'city': {'name': '中国城市', 'url': 'http://www.news.cn/city/'},
        'source': {'name': '溯源中国', 'url': 'http://www.news.cn/source/'},
    }
    
    # 获取上次爬取时间
    last_crawl_file = os.path.join(spider.base_dir, 'last_crawl_time.json')
    try:
        with open(last_crawl_file, 'r', encoding='utf-8') as f:
            last_crawl_times = json.load(f)
    except:
        last_crawl_times = {}
    
    current_time = datetime.datetime.now()
    
    # 遍历每个分类
    for category_key, category_info in start_urls.items():
        try:
            print(f"\n开始处理分类: {category_info['name']}")
            
            # 获取该分类上次爬取时间
            last_crawl_time = datetime.datetime.strptime(
                last_crawl_times.get(category_key, '2000-01-01 00:00:00'),
                '%Y-%m-%d %H:%M:%S'
            )
            
            # 请求分类页面
            if not (response := spider.safe_request(category_info['url'])): 
                continue
                
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 获取新闻链接
            links = []
            for a in soup.find_all('a', href=True):
                if url := a.get('href'):
                    if ('xinhuanet.com' in url or 'news.cn' in url) and \
                       url.endswith(('.htm', '.html')) and \
                       not any(skip in url for skip in ['/index.htm', '/index.html', 'special', 'topics', 'subject', 'photo/list', 'video/list']):
                        # 检查日期
                        if date_match := re.search(r'/(\d{4})[-/](\d{2})[-/](\d{2})/', url):
                            url = urljoin(category_info['url'], url) if not url.startswith('http') else url
                            # 提取文章日期
                            article_date = datetime.datetime.strptime(
                                f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)} 00:00:00",
                                '%Y-%m-%d %H:%M:%S'
                            )
                            # 只添加上次爬取之后的文章
                            if article_date > last_crawl_time and url not in links:
                                links.append(url)
            
            print(f"找到 {len(links)} 个新文章")
            
            # 处理每个链接
            for link in links:
                if spider.is_article_processed(link):
                    print(f"跳过已处理: {link}")
                    continue
                    
                if result := spider.get_xinhua_news(link):
                    title, category, _, save_path = result
                    spider.mark_article_processed(link, title, category, save_path)
                    print(f"成功: {title}")
                time.sleep(1)
            
            # 更新该分类的最后爬取时间
            last_crawl_times[category_key] = current_time.strftime('%Y-%m-%d %H:%M:%S')
            
        except Exception as e:
            print(f"处理分类失败: {category_info['name']}, 错误: {str(e)}")
            continue
    
    # 保存最后爬取时间
    with open(last_crawl_file, 'w', encoding='utf-8') as f:
        json.dump(last_crawl_times, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    schedule.every().day.at("00:00").do(main)
    schedule.every().day.at("12:00").do(main)
    main()  # 首次运行
    while True:
        schedule.run_pending()
        time.sleep(60)