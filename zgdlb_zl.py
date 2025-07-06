#!/home/inspur/anaconda3/bin/python
# -*- coding: UTF-8 -*-
import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
from requests.exceptions import RequestException, JSONDecodeError

# 配置请求头
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://cpnn.com.cn',
    'Referer': 'https://cpnn.com.cn/epaper/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

def safe_request(url, method='GET', data=None, max_retry=3):
    """安全的网络请求函数"""
    for attempt in range(max_retry):
        try:
            if method.upper() == 'GET':
                resp = requests.get(url, headers=headers, timeout=15)
            else:
                resp = requests.post(url, headers=headers, data=data, timeout=15)
            
            resp.raise_for_status()
            return resp
        except RequestException as e:
            print(f"请求失败(尝试 {attempt + 1}/{max_retry}): {url}, 错误: {str(e)}")
            if attempt == max_retry - 1:
                return None
            time.sleep(2)
    return None

def save_text_content(content, filepath):
    """保存文本内容到文件"""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"文本内容已保存到: {filepath}")
        return True
    except Exception as e:
        print(f"保存文本内容失败: {filepath}, 错误: {str(e)}")
        return False

def download_attachment(url, filepath):
    """下载并保存附件"""
    try:
        if not url or not isinstance(url, str):
            print(f"无效的附件URL: {url}")
            return False
            
        resp = safe_request(url)
        if not resp or resp.status_code != 200:
            return False
            
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        print(f"附件已保存到: {filepath}")
        return True
        
    except Exception as e:
        print(f"下载附件失败: {url}, 错误: {str(e)}")
        return False

def process_detail_data(guid, filepath):
    """处理详细数据"""
    try:
        url = 'https://cpnn.com.cn/reader/layout/detailData.do'
        payload = f'guid={guid}'
        
        resp = safe_request(url, method='POST', data=payload)
        if not resp:
            return False
            
        try:
            data = resp.json()
        except JSONDecodeError:
            print(f"无效的JSON响应: {resp.text[:200]}...")
            return False
            
        # 提取文章信息
        doc_title = data.get('docTitle', '无标题').strip()
        fb = data.get('fb', '').strip()
        doc_author = data.get('docAuthor', '').strip()
        doc_pub_time = data.get('docPubTime', '').strip()
        content = data.get('content', '').strip()
        txs = data.get('txs', '').strip()
        
        # 准备保存内容
        text_content = f"""标题: {doc_title}
版面: {fb}
发布时间: {doc_pub_time}
作者: {doc_author}
字数: {txs}

{content}
"""
        
        # 保存文本内容
        txt_filename = f"{doc_title}.txt"
        txt_path = os.path.join(filepath, txt_filename)
        if not save_text_content(text_content, txt_path):
            return False
        
        # 下载附件
        jp_path = data.get('jpPath')
        if jp_path:
            jp_filename = os.path.basename(jp_path)
            jp_path_full = os.path.join(filepath, jp_filename)
            download_attachment(f'https://cpnn.com.cn/epaper/{jp_path}', jp_path_full)
            
        pf_path = data.get('pfPath')
        if pf_path:
            pf_filename = os.path.basename(pf_path)
            pf_path_full = os.path.join(filepath, pf_filename)
            download_attachment(f'https://cpnn.com.cn/epaper/{pf_path}', pf_path_full)
            
        return True
        
    except Exception as e:
        print(f"处理详细数据时出错: {str(e)}")
        return False

def process_bm_detail_pub(bc, docpubtime, filepath):
    """处理版面详细发布信息"""
    try:
        url = f'https://cpnn.com.cn/reader/layout/getBmDetailPub.do?bc={bc}&docpubtime={docpubtime}'
        resp = safe_request(url)
        if not resp:
            return False
            
        try:
            detail_pubs = resp.json()
        except JSONDecodeError:
            print(f"无效的JSON响应: {resp.text[:200]}...")
            return False
            
        if not isinstance(detail_pubs, list):
            print(f"获取到的版面信息为空: {resp.text[:200]}...")
            return False
            
        for detail_pub in detail_pubs:
            try:
                doc_title = detail_pub.get('DOCTITLE', '无标题').strip()
                zb_guid = detail_pub.get('ZB_GUID')
                if not zb_guid:
                    continue
                    
                article_dir = os.path.join(filepath, f"{doc_title}-{zb_guid}")
                os.makedirs(article_dir, exist_ok=True)
                
                # 下载附件
                jppath = detail_pub.get('JPPATH')
                if jppath:
                    jp_filename = os.path.basename(jppath)
                    jp_path_full = os.path.join(article_dir, jp_filename)
                    download_attachment(f'https://cpnn.com.cn/epaper/{jppath}', jp_path_full)
                    
                pdpath = detail_pub.get('PDPATH')
                if pdpath:
                    pd_filename = os.path.basename(pdpath)
                    pd_path_full = os.path.join(article_dir, pd_filename)
                    download_attachment(f'https://cpnn.com.cn/epaper/{pdpath}', pd_path_full)
                
                # 处理详细数据
                if not process_detail_data(zb_guid, article_dir):
                    print(f"处理文章详情失败: {doc_title}")
                
                time.sleep(1)
                
            except Exception as e:
                print(f"处理版面详细条目时出错: {str(e)}")
                continue
                
        return True
        
    except Exception as e:
        print(f"处理版面详细发布信息时出错: {str(e)}")
        return False

def process_bm_menu_pub(doc_pub_time, filepath):
    """处理版面菜单发布信息"""
    try:
        url = "https://cpnn.com.cn/reader/layout/findBmMenuPub.do"
        payload = f"docPubTime={doc_pub_time}"
        
        resp = safe_request(url, method='POST', data=payload)
        if not resp:
            return False
            
        try:
            menu_pubs = resp.json()
        except JSONDecodeError:
            print(f"无效的JSON响应: {resp.text[:200]}...")
            return False
            
        if not isinstance(menu_pubs, list):
            print(f"获取到的版面信息为空: {resp.text[:200]}...")
            return False
            
        for obj in menu_pubs:
            try:
                bc = obj.get("BC", "").strip()
                bm = obj.get("BM", "").strip()
                if not bc or not bm:
                    continue
                    
                section_name = f"{bc}：{bm}"
                ircatelog = obj.get('IRCATELOG')
                if not ircatelog:
                    continue
                    
                section_dir = os.path.join(filepath, section_name)
                os.makedirs(section_dir, exist_ok=True)
                
                # 下载各种附件
                bm_jpg_url = obj.get('BM_JPG_URL', {})
                if isinstance(bm_jpg_url, dict):
                    for key in ['JPPATH_BZT', 'JPPATH_TB', 'JPPATH_JT', 'JPPATH_ZT']:
                        path = bm_jpg_url.get(key)
                        if path:
                            filename = os.path.basename(path)
                            full_path = os.path.join(section_dir, filename)
                            download_attachment(f'https://cpnn.com.cn/epaper/{path}', full_path)
                
                jppath = obj.get('JPPATH')
                if jppath:
                    jp_filename = os.path.basename(jppath)
                    jp_path_full = os.path.join(section_dir, jp_filename)
                    download_attachment(f'https://cpnn.com.cn/epaper/{jppath}', jp_path_full)
                    
                pdpath = obj.get('PDPATH')
                if pdpath:
                    pd_filename = os.path.basename(pdpath)
                    pd_path_full = os.path.join(section_dir, pd_filename)
                    download_attachment(f'https://cpnn.com.cn/epaper/{pdpath}', pd_path_full)
                
                # 处理详细发布信息
                if not process_bm_detail_pub(ircatelog, doc_pub_time, section_dir):
                    print(f"处理版面详情失败: {section_name}")
                
                time.sleep(1)
                
            except Exception as e:
                print(f"处理版面菜单条目时出错: {str(e)}")
                continue
                
        return True
        
    except Exception as e:
        print(f"处理版面菜单发布信息时出错: {str(e)}")
        return False

def main_ql():
    """主函数"""
    try:
        # 配置日期范围
        start_date = '2024-08-28'
        end_date = '2025-06-30'
        
        # 创建主目录
        base_dir = "中国电力报"
        os.makedirs(base_dir, exist_ok=True)
        print(f"数据将保存到: {os.path.abspath(base_dir)}")
        
        # 生成日期序列
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        
        for date in date_range:
            date_str = date.strftime('%Y%m%d')
            print(f"\n开始处理 {date_str} 的数据...")
            
            # 创建日期目录
            date_dir = os.path.join(base_dir, date_str)
            
            # 处理当天的版面菜单
            success = process_bm_menu_pub(date_str, date_dir)
            if success:
                print(f"成功处理 {date_str} 的数据")
            else:
                print(f"处理 {date_str} 数据失败")
                
            time.sleep(2)
            
    except Exception as e:
        print(f"主程序运行出错: {str(e)}")
    finally:
        print("\n爬取任务完成！")

def main_zl(date_str=None):

    """主函数"""
    try:
        # 如果未提供日期参数，则使用当天日期
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
        
        # 创建主目录
        base_dir = "中国电力报"
        os.makedirs(base_dir, exist_ok=True)
        print(f"数据将保存到: {os.path.abspath(base_dir)}")
        
        # 创建日期目录
        date_dir = os.path.join(base_dir, date_str)
        
        # 处理当天的版面菜单
        success = process_bm_menu_pub(date_str, date_dir)
        if success:
            print(f"成功处理 {date_str} 的数据")
        else:
            print(f"处理 {date_str} 数据失败")
            
    except Exception as e:
        print(f"主程序运行出错: {str(e)}")
    finally:
        print("\n爬取任务完成！")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='中国电力报')
    parser.add_argument('--date', type=str, help='指定日期(格式: YYYYMMDD)，默认为当天')
    args = parser.parse_args()
    #增量爬取设置
    main_zl(args.date)
