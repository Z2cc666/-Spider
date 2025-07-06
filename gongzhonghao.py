#!/home/inspur/anaconda3/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import time
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import logging


import requests
import json
import pandas as pd
from time import sleep
import time  # 添加这一行
import re
import os
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def setup_folders():
    """创建必要的文件夹结构"""
    folders = ['微信公众号文章/文本', '微信公众号文章/图片', '微信公众号文章/视频']
    for folder in folders:
        os.makedirs(folder, exist_ok=True)

def sanitize_filename(title):
    """清理文件名中的非法字符"""
    return re.sub(r'[\\/*?:"<>|]', "_", title)[:100]

def download_images(soup, article_title, session):
    """下载文章中的所有图片"""
    img_dir = os.path.join('微信公众号文章', '图片', sanitize_filename(article_title))
    os.makedirs(img_dir, exist_ok=True)
    
    downloaded_images = []
    for idx, img in enumerate(soup.find_all('img'), 1):
        img_url = img.get('data-src') or img.get('src')
        if not img_url:
            continue
            
        try:
            # 处理微信图片URL
            if img_url.startswith('//'):
                img_url = 'https:' + img_url
            elif not img_url.startswith(('http://', 'https://')):
                logging.warning(f"跳过无效图片URL: {img_url}")
                continue

            # 下载图片
            response = session.get(img_url, stream=True, timeout=15)
            response.raise_for_status()
            
            # 确定文件扩展名
            content_type = response.headers.get('content-type', '')
            if 'jpeg' in content_type or 'jpg' in content_type:
                ext = '.jpg'
            elif 'png' in content_type:
                ext = '.png'
            else:
                ext = os.path.splitext(urlparse(img_url).path)[1] or '.jpg'
            
            # 保存图片
            img_path = os.path.join(img_dir, f"{idx}{ext}")
            with open(img_path, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            
            downloaded_images.append(img_path)
            # logging.info(f"已保存图片: {img_path}")
            time.sleep(1)  # 避免请求过于频繁
            
        except Exception as e:
            logging.error(f"图片下载失败: {img_url} - {str(e)}")
    
    return downloaded_images

def save_article_text(soup, article_title):
    """保存文章文本内容"""
    # 查找微信文章正文
    content_div = soup.find('div', {'id': 'js_content'}) or \
                 soup.find('div', class_='rich_media_content')
    
    if not content_div:
        logging.warning("未找到文章正文内容")
        return None
    
    # 清理文本
    text = content_div.get_text(separator='\n', strip=True)
    if not text or len(text) < 50:  # 简单验证文本长度
        logging.warning("获取的文本内容过短，可能不完整")
        return None
    
    # 保存文本文件
    text_dir = os.path.join('微信公众号文章', '文本')
    os.makedirs(text_dir, exist_ok=True)
    
    text_path = os.path.join(text_dir, f"{sanitize_filename(article_title)}.txt")
    with open(text_path, 'w', encoding='utf-8') as f:
        f.write(text)
    
    # logging.info(f"已保存文本: {text_path}")
    return text_path

def process_article(article_url, article_title, session):
    """处理单篇文章"""
    if not article_url or not article_url.startswith('http'):
        logging.error(f"无效的文章URL: {article_url}")
        return None, [], []  # 返回值添加视频列表
    
    try:
        response = session.get(article_url, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 下载所有图片
        images = download_images(soup, article_title, session)
        
        # 下载所有视频
        videos = []
        video_urls = extract_video_url(soup)
        for idx, video_url in enumerate(video_urls, 1):
            video_path = download_video(video_url, article_title, idx)
            if video_path:
                videos.append(video_path)
        
        # 保存文本内容
        text_file = save_article_text(soup, article_title)
        
        return text_file, images, videos
        
    except Exception as e:
        logging.error(f"处理文章失败: {article_url} - {str(e)}")
        return None, [], []

def extract_video_url(soup):
    """提取文章中的视频URL"""
    videos = []
    
    # 查找视频iframe
    for iframe in soup.find_all('iframe', class_='video_iframe'):
        video_url = iframe.get('data-src')
        if video_url:
            if video_url.startswith('//'):
                video_url = 'https:' + video_url
            videos.append(video_url)
    
    # 查找video标签
    for video in soup.find_all('video'):
        video_url = video.get('src')
        if video_url:
            if video_url.startswith('//'):
                video_url = 'https:' + video_url
            videos.append(video_url)
            
    return videos

def download_video(video_url, article_title, index):
    """下载单个视频"""
    try:
        # 处理文件名
        safe_title = re.sub(r'[\\/*?:"<>|]', "_", article_title)[:50]
        filename = f"{safe_title}_视频{index}.mp4"
        filepath = os.path.join('微信公众号文章/视频', filename)
        
        # 检查文件是否已存在
        if os.path.exists(filepath):
            print(f"视频已存在: {filename}")
            return filepath
            
        print(f"正在下载视频: {filename}")
        response = requests.get(video_url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        
        print(f"✅ 视频下载成功: {filename}")
        return filepath
        
    except Exception as e:
        print(f"❌ 视频下载失败: {str(e)}")
        return None

# 使用示例
if __name__ == "__main__":
    setup_folders()
    
    # 测试下载5页
    page_size = 5  # 每页5篇文章
    max_pages = 5  # 先测试5页





cookies = {
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

headers = {
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

params = {
    'sub': 'list',
    'search_field': 'null',
    'begin': '0',
    'count': '1000',# 获取文章数量
    'query': '',
    'fakeid': 'MzA3OTA2Mjc2MA==', 
    'type': '101_1',
    'free_publish_type': '1',
    'sub_action': 'list_ex',
    'fingerprint': '45e623cde6e181b213b3f595227b4a71',
    'token': '3514936',
    'lang': 'zh_CN',
    'f': 'json',
    'ajax': '1',
}
 


all_articles = []
page_size = 5  # 微信每次最多返回20条
max_pages = 5   # 最多翻50页（防止无限循环）

for page in range(max_pages):
 

    params = {
        'sub': 'list',
        'search_field': 'null',
        "begin": page * page_size,  # 翻页关键参数
        "count": page_size,
        'query': '',
        'fakeid': 'MzA3OTA2Mjc2MA==', 
        'type': '101_1',
        'free_publish_type': '1',
        'sub_action': 'list_ex',
        'fingerprint': '45e623cde6e181b213b3f595227b4a71',
        'token': '3514936',
        'lang': 'zh_CN',
        'f': 'json',
        'ajax': '1',
    }
 

    def get_articles_with_retry(page, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    "https://mp.weixin.qq.com/cgi-bin/appmsgpublish",
                    params=params,
                    cookies=cookies,
                    headers=headers,
                    timeout=30
                )
                return response
            except requests.exceptions.ReadTimeout:
                if attempt == max_retries - 1:
                    print(f"第 {page} 页重试{max_retries}次后仍然失败")
                    return None
                print(f"第 {page} 页超时，等待后重试...")
                time.sleep(5 * (attempt + 1))

    response = get_articles_with_retry(page)
    # 新增频率控制
    time.sleep(3)  # 从1秒改为3秒
    if response.status_code != 200:
        print(f"第 {page+1} 页请求失败")
        break

    try:
        result = response.json()
        if result.get("base_resp", {}).get("ret") != 0:
            print(f"第 {page+1} 页错误: {result['base_resp']['err_msg']}")
            break

        publish_page = result.get("publish_page")
        if not publish_page:
            print(f"第 {page+1} 页无数据")
            break

        articleList = json.loads(publish_page)
        articles = articleList.get("publish_list", [])
        if not articles:
            print(f"第 {page+1} 页文章列表为空")
            break

        all_articles.extend(articles)
        print(f"已获取第 {page+1} 页，当前总数: {len(all_articles)}")

    except Exception as e:
        print(f"第 {page+1} 页解析异常: {e}")
        break

articles_data = []
for article in all_articles:
    try:
        info = json.loads(article.get("publish_info", "{}"))
        if info.get("appmsgex"):
            # 获取基本信息
            title = info["appmsgex"][0].get("title", "")
            link = info["appmsgex"][0].get("link", "")
            create_time = info["appmsgex"][0].get("create_time")
            
            if not create_time:
                create_time = info.get("create_time")
            
            if isinstance(create_time, int):
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(create_time))

            # 处理文章内容
            if link:
                session = requests.Session()
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Referer': 'https://mp.weixin.qq.com/'
                })
                
                text_path, img_paths, video_paths = process_article(link, title, session)
                
                articles_data.append({
                    "标题": title,
                    "链接": link,
                    "发布时间": create_time or "",
                    "阅读量": info.get("read_num", ""),
                    "点赞量": info.get("like_num", ""),
                    "文本文件": text_path,
                    "图片数量": len(img_paths),
                    "视频数量": len(video_paths),
                    "视频文件": ", ".join(video_paths) if video_paths else "无"
                })
                
                print(f"处理完成: {title} (图片:{len(img_paths)}张, 视频:{len(video_paths)}个)")
                time.sleep(3)  # 添加延迟
                
    except Exception as e:
        print(f"文章解析失败: {e}")

# 保存到Excel
if articles_data:
    df = pd.DataFrame(articles_data)
    df.to_excel("微信公众号文章/微信公众号文章.xlsx", index=False)
    print(f"成功保存，包含发布时间列！")
else:
    print("无有效数据")

 


 