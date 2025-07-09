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

class NanFengChuangSpider:
    def __init__(self):
        self.base_dir = "南风窗数据"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.nfcmag.com/'
        }
        self.base_url = "https://www.nfcmag.com"
        self.processed_file = os.path.join(self.base_dir, 'processed_articles.json')
        os.makedirs(self.base_dir, exist_ok=True)
        if not os.path.exists(self.processed_file):
            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump({}, f)
        
        # 分类字典
        self.categories = {
            '45': '区域',
            '16': '窗下人语',
            '17': '纵论',
            '2': '调查与科技',
            '39': '公司与金融',
            '4': '人文',
            '1': '时局',
            '6': '思想与对话',
            '5': '中国与世界',
            '37': '公共政策'
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
            # 从URL中提取分类ID
            category_match = re.search(r'/category/(\d+)\.html', url)
            if category_match:
                category_id = category_match.group(1)
                return self.categories.get(category_id, '其他')
            
            # 从文章URL中提取分类
            article_match = re.search(r'/article/\d+\.html', url)
            if article_match:
                response = self.safe_request(url)
                if response:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # 查找面包屑导航或分类标签
                    for selector in ['.breadcrumb a', '.category a', '.article-category']:
                        if category_elem := soup.select_one(selector):
                            category_text = category_elem.get_text(strip=True)
                            return category_text
            
            return '其他'
        except:
            return '其他'

    def get_article_date(self, soup, url):
        """获取文章发布日期"""
        try:
            # 尝试从URL中提取日期
            url_date = re.search(r'/(\d{4})[-/](\d{2})[-/](\d{2})/', url)
            if url_date:
                return f"{url_date.group(1)}-{url_date.group(2)}-{url_date.group(3)}"
            
            # 从页面中提取日期
            for selector in ['.time', '.date', '.article-date', '.publish-date']:
                if date_elem := soup.select_one(selector):
                    if date_text := date_elem.get_text(strip=True):
                        # 尝试解析日期文本
                        date_match = re.search(r'(\d{4})[-年](\d{1,2})[-月](\d{1,2})', date_text)
                        if date_match:
                            return f"{date_match.group(1)}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}"
            
            # 默认使用当前日期
            return datetime.datetime.now().strftime('%Y-%m-%d')
        except:
            return datetime.datetime.now().strftime('%Y-%m-%d')

    def get_article(self, url):
        """获取文章内容"""
        try:
            response = self.safe_request(url)
            if not response: return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取标题
            title = None
            for selector in ['h1', '.article-title', '.title']:
                if title_elem := soup.select_one(selector):
                    if (title := title_elem.get_text(strip=True)) and len(title) > 5:
                        break
            if not title:
                return None
                
            # 获取日期和分类
            date_str = self.get_article_date(soup, url)
            category = self.get_category_from_url(url)
            
            # 获取作者
            author = ""
            for selector in ['.author', '.writer', '.article-author']:
                if author_elem := soup.select_one(selector):
                    if author_text := author_elem.get_text(strip=True):
                        author = author_text
                        break
            
            # 获取内容
            content = []
            if main := soup.select_one('article, .article-content, .content'):
                for p in main.find_all(['p', 'div']):
                    if text := p.get_text(strip=True):
                        if not re.match(r'^[\s\d]*$', text) and len(text) > 2:
                            content.append(text)
            
            if not content:
                return None
                
            # 创建保存目录
            article_dir = os.path.join(self.base_dir, date_str, category, re.sub(r'[<>:"/\\|?*]', '_', title)[:100].strip())
            os.makedirs(article_dir, exist_ok=True)
            
            # 保存文章信息
            article_info = f"""标题：{title}
作者：{author}
链接：{url}
日期：{date_str}
分类：{category}

正文：
{'\\n'.join(content)}"""
            
            with open(os.path.join(article_dir, f"{title[:100]}.txt"), 'w', encoding='utf-8') as f:
                f.write(article_info)
            
            # 下载图片
            if main:
                saved_images = set()
                for i, img in enumerate(main.find_all('img'), 1):
                    src = img.get('src', '')
                    if not src or src in saved_images: continue
                    
                    # 处理图片URL
                    if not src.startswith('http'):
                        src = urljoin(url, src)
                    
                    # 跳过广告、图标等小图片
                    if any(skip in src.lower() for skip in ['ad', 'logo', 'icon', 'banner']): 
                        continue
                        
                    try:
                        if resp := self.safe_request(src):
                            if len(resp.content) > 10 * 1024:  # 大于10KB的图片
                                ext = os.path.splitext(src)[1].lower() or '.jpg'
                                img_path = os.path.join(article_dir, f"图片_{i}{ext}")
                                with open(img_path, 'wb') as f:
                                    f.write(resp.content)
                                saved_images.add(src)
                                print(f"保存图片: {img_path}")
                    except Exception as e:
                        print(f"下载图片失败: {src}, {str(e)}")
            
            return title, category, '\\n'.join(content), article_dir
            
        except Exception as e:
            print(f"处理文章失败: {url}, 错误: {str(e)}")
            return None

    def get_category_articles(self, category_id):
        """获取分类下的文章链接"""
        try:
            url = f"{self.base_url}/category/{category_id}.html"
            response = self.safe_request(url)
            if not response:
                return []
                
            soup = BeautifulSoup(response.text, 'html.parser')
            article_urls = []
            
            # 查找文章链接
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '/article/' in href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in article_urls:
                        article_urls.append(full_url)
            
            return article_urls
            
        except Exception as e:
            print(f"获取分类文章失败: {category_id}, 错误: {str(e)}")
            return []

def main():
    spider = NanFengChuangSpider()
    spider.clean_old_records()
    
    # 获取上次爬取时间
    last_crawl_file = os.path.join(spider.base_dir, 'last_crawl_time.json')
    try:
        with open(last_crawl_file, 'r', encoding='utf-8') as f:
            last_crawl_times = json.load(f)
    except:
        last_crawl_times = {}
    
    current_time = datetime.datetime.now()
    
    # 遍历每个分类
    for category_id, category_name in spider.categories.items():
        try:
            print(f"\n开始处理分类: {category_name}")
            
            # 获取该分类上次爬取时间
            last_crawl_time = datetime.datetime.strptime(
                last_crawl_times.get(category_id, '2000-01-01 00:00:00'),
                '%Y-%m-%d %H:%M:%S'
            )
            
            # 获取分类文章链接
            article_urls = spider.get_category_articles(category_id)
            print(f"找到 {len(article_urls)} 个文章链接")
            
            # 处理每个文章
            for url in article_urls:
                if spider.is_article_processed(url):
                    print(f"跳过已处理: {url}")
                    continue
                
                if result := spider.get_article(url):
                    title, category, _, save_path = result
                    spider.mark_article_processed(url, title, category, save_path)
                    print(f"成功: {title}")
                time.sleep(1)
            
            # 更新该分类的最后爬取时间
            last_crawl_times[category_id] = current_time.strftime('%Y-%m-%d %H:%M:%S')
            
        except Exception as e:
            print(f"处理分类失败: {category_name}, 错误: {str(e)}")
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