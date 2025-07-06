#!/home/inspur/anaconda3/bin/python
# -*- coding: UTF-8 -*-
import os
import time
import re
import pandas as pd
import requests
from requests.exceptions import RequestException, ConnectionError, Timeout, TooManyRedirects

from lxml import etree

url_root = "https://epaper.wsqejt.com"
path_root = '信息时报'


def doArticle(url, filePath):
    try:
        resp = requests.get(url, timeout=10)  # 添加10秒超时
        resp.raise_for_status()  # 检查HTTP错误
        
        html = etree.HTML(resp.text)
        title = 'content_' + str.strip(html.xpath('//*[@id="list"]/div[3]/div[2]/text()')[0])
        # 清理标题中的非法字符
        title = re.sub(r'[:*?"<>|]', '-', title)
        title = re.sub(r'\s+', '', title)
        
        # 确保目录存在
        os.makedirs(filePath, exist_ok=True)
        
        # 下载图片
        imgUrls = html.xpath('//*[@id="list"]//table//img/@src')
        for imgUrl in imgUrls:
            try:
                respImg = requests.get(imgUrl, timeout=10)
                respImg.raise_for_status()
                imgName = str.split(imgUrl,"/")[-1]
                with open(f'{filePath}/{imgName}', mode='wb') as f:
                    f.write(respImg.content)
            except Exception as e:
                print(f"下载图片失败: {imgUrl}, 错误: {e}")
                continue  # 继续处理下一张图片
        
        # 保存文本内容
        contents = html.xpath('//*[@id="list"]//table//td//text()')
        with open(f'{filePath}/{title}.txt', mode='w', encoding='utf-8') as f:
            f.write(''.join(contents))
            
    except ConnectionError:
        print(f"网络连接失败，请检查网络: {url}")
    except Timeout:
        print(f"请求超时: {url}")
    except TooManyRedirects:
        print(f"重定向次数过多: {url}")
    except RequestException as e:
        print(f"请求发生异常: {url}, 错误: {e}")
    except Exception as e:
        print(f"处理文章时发生未知错误: {url}, 错误: {e}")


def crawler(url):
    try:
        response = requests.get(url=url, timeout=10)
        response.raise_for_status()
        
        html = etree.HTML(response.text)
        news_item = html.xpath('//div[@class="main-list"]')[0]
        summary_text = news_item.xpath('//div[@class="summary clearfix"]//text()')
        cleaned_text = ' '.join([text.strip() for text in summary_text if text.strip()])

        next_page = html.xpath('//div[@class="tonextblock"]/a/@href')[0]

        if '.html' in next_page:
            next_page = f'{url_root + next_page}'
            print(f'下一页:{next_page}')
            if next_page != url:
                crawler(next_page)
                title = news_item.xpath('.//h2/a/text()')
                link = news_item.xpath('.//h2/a/@href')
                imgs = html.xpath('/html/body//div[@class="main-paper"]//img/@src')
                if title and link:
                    print(f"标题: {title[0]}")
                    print(f"链接: {url_root + link[0]}")
                    print("-" * 40)
                    parts = link[0].split("/")
                    year_month = parts[3]
                    day = parts[4]
                    title = re.sub(r'[:*?"<>|]', '-', title[0])
                    title = re.sub(r'\s+', '', title)
                    path_year_month_day = f'{path_root}/{year_month}/{day}/{title + next_page.split("/")[-1].split(".")[0]}'
                    os.makedirs(path_year_month_day, exist_ok=True)
                    doArticle(f'{url_root + link[0]}', path_year_month_day)
                    time.sleep(2)
                    print(f"年月日: {year_month}-{day}")
                    with open(f"{path_year_month_day}/{title}.txt", "w", encoding="utf-8") as f:
                        f.write(cleaned_text)
                    for img in imgs:
                        try:
                            respImg = requests.get(img, timeout=10)
                            respImg.raise_for_status()
                            with open(f"{path_year_month_day}/{str.split(img,'/')[-1]}", "wb") as f:
                                f.write(respImg.content)
                        except Exception as e:
                            print(f"下载图片失败: {img}, 错误: {e}")
                            
    except ConnectionError:
        print(f"网络连接失败，请检查网络: {url}")
    except Timeout:
        print(f"请求超时: {url}")
    except TooManyRedirects:
        print(f"重定向次数过多: {url}")
    except RequestException as e:
        print(f"请求发生异常: {url}, 错误: {e}")
    except Exception as e:
        print(f"爬取页面时发生未知错误: {url}, 错误: {e}")


if __name__ == '__main__':
    try:
        for date in pd.date_range(start='2024-01-01', end='2025-06-30', freq='D'):
            url = f'https://epaper.wsqejt.com/html/node/{date.strftime("%Y-%m")}/{date.strftime("%d")}/A1.html'
            print(f"正在处理: {url}")
            crawler(url)
            time.sleep(1)
    except Exception as e:
        print(f"主程序运行出错: {e}")