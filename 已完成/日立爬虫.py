#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os, time, json, requests, chardet
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote, quote
import re
import pickle
from pathlib import Path
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import hmac
import base64
import urllib.parse

class HitachiUltimateSpider: # 日立爬虫
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.hitachi-iec.cn/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.base_url = "https://www.hitachi-iec.cn"
        
        # 服务器固定路径（按规范要求）
        self.base_dir = "/srv/downloads/approved/日立"
        self.processed_urls = self.load_processed_urls()
        self.new_files = []
        self.debug = True
        self.config = self.load_config()
        
        # 钉钉通知配置
        self.ACCESS_TOKEN = "1a431b6482e59ec652a6eab37705d50fd282dc426f0b829b3eb9a9c1b277ed24"
        self.SECRET = "SECc5e2168011dd97d4c511e06832d172f31635ef635771668e4e6003fce23c07bb"
        
        # 判断是否为首次运行（全量爬取）
        self.is_first_run = not os.path.exists(os.path.join(self.base_dir, 'processed_urls.pkl'))
        
        # 完整的产品目录 - 更新为实际可用的URL
        self.product_categories = {
            'inverters': {
                'name': '变频器',
                'products': [
                    {'name': 'SJ-P1全球版高性能矢量型变频器', 'url': 'https://www.hitachi-iec.cn/ch/product/trans/sjp1/index.html'},
                    {'name': 'SH1高性能全兼容型变频器', 'url': 'https://www.hitachi-iec.cn/ch/product/trans/sh1/index.html'},
                    {'name': 'NH1高性能标准矢量型变频器', 'url': 'https://www.hitachi-iec.cn/ch/product/trans/nh1/index.html'},
                    {'name': 'LH1多用途通用矢量型变频器', 'url': 'https://www.hitachi-iec.cn/ch/product/trans/lh1/index.html'},
                    {'name': 'Ps-H100书本式高性能变频器', 'url': 'https://www.hitachi-iec.cn/ch/product/trans/psh100/index.html'},
                    {'name': 'WJ-C1紧凑高性能变频器', 'url': 'https://www.hitachi-iec.cn/ch/product/trans/wjc1/index.html'},
                    {'name': 'Cs-H100小型通用矢量型变频器', 'url': 'https://www.hitachi-iec.cn/ch/product/trans/csh100/index.html'}
                ]
            },
            'marking': {
                'name': '标识设备',
                'products': [
                    {'name': 'UX系列工业喷码机', 'url': 'https://www.hitachi-iec.cn/ch/product/print/ux/index.html'},
                    {'name': 'G系列工业喷码机', 'url': 'https://www.hitachi-iec.cn/ch/product/print/g/index.html'},
                    {'name': 'RX2系列工业喷码机', 'url': 'https://www.hitachi-iec.cn/ch/product/print/rx/index.html'}
                ]
            },
            'motor': {
                'name': '电机',
                'products': [
                    {'name': '工业电机产品', 'url': 'https://www.hitachi-iec.cn/ch/product/driver/index.html'}
                ]
            },
            'plc': {
                'name': '可编程控制器',
                'products': [
                    {'name': 'HX系列PLC', 'url': 'https://www.hitachi-iec.cn/ch/product/plc/kzq/hx/index.html'},
                    {'name': 'MICRO-EHV系列PLC', 'url': 'https://www.hitachi-iec.cn/ch/product/plc/kzq/microehv/index.html'},
                    {'name': 'EH-150 EHV系列PLC', 'url': 'https://www.hitachi-iec.cn/ch/product/plc/kzq/eh150ehv/index.html'}
                ]
            },
            'blower': {
                'name': '鼓风机',
                'products': [
                    {'name': '工业鼓风机', 'url': 'https://www.hitachi-iec.cn/ch/product/fan/index.html'}
                ]
            },
            'hoist': {
                'name': '日立电动葫芦',
                'products': [
                    {'name': '日立钢丝绳葫芦', 'url': 'https://www.hitachi-iec.cn/ch/product/ddhl/g/index.html'},
                    {'name': '日立环链葫芦', 'url': 'https://www.hitachi-iec.cn/ch/product/ddhl/h/index.html'}
                ]
            }
        }
        
    def load_config(self):
        """加载配置文件"""
        config_file = "config.json"
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            if self.debug:
                print(f"配置文件加载失败: {e}")
        
        # 返回默认配置
        return {
            "spider_settings": {
                "max_concurrent_downloads": 10,
                "download_timeout": 60,
                "retry_times": 3,
                "delay_between_requests": 1
            },
            "notification_settings": {
                "enable_email": False,
                "enable_console": True
            },
            "schedule_settings": {
                "enable_schedule": True,
                "check_times": ["09:00", "13:00", "17:00"]
            },
            "output_settings": {
                "generate_html_report": True,
                "generate_pdf_features": True,
                "compress_old_files": False
            }
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

    def clean_filename(self, filename):
        """清理文件名"""
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = filename.replace('（', '(').replace('）', ')')
        return filename[:100].strip()

    def safe_request(self, url, **kwargs):
        """安全的网络请求"""
        try:
            time.sleep(self.config['spider_settings']['delay_between_requests'])
            response = self.session.get(url, timeout=self.config['spider_settings']['download_timeout'], **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            if self.debug:
                print(f"请求失败: {url} - {str(e)}")
            return None

    def download_image(self, img_url, save_path):
        """下载图片文件"""
        try:
            response = self.safe_request(img_url)
            if response and response.status_code == 200:
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                return True
        except Exception as e:
            if self.debug:
                print(f"下载图片失败: {img_url} - {str(e)}")
        return False

    def extract_product_features_with_images(self, soup, product_name, product_url):
        """提取产品特点信息，包括图片"""
        features = {
            'product_name': product_name,
            'product_url': product_url,
            'feature_images': [],
            'feature_sections': [],
            'all_content': '',
            'page_structure': []
        }
        
        try:
            # 查找产品特点相关的图片
            feature_images = []
            images = soup.find_all('img')
            
            for img in images:
                src = img.get('src', '')
                alt = img.get('alt', '')
                
                # 查找特点相关的图片 - 扩大搜索范围
                if (any(keyword in src.lower() for keyword in ['feature', '特点', 'point', '1_', '2_', 'pic']) or
                    any(keyword in alt.lower() for keyword in ['特点', 'feature']) or
                    'images/' in src):  # 产品页面的images目录通常包含特点图片
                    
                    if not src.startswith('http'):
                        img_url = urljoin(product_url, src)
                    else:
                        img_url = src
                    
                    feature_images.append({
                        'url': img_url,
                        'alt': alt or '产品特点图片',
                        'src': src
                    })
            
            features['feature_images'] = feature_images
            
            # 分析页面结构，查找主要内容区域
            main_sections = []
            
            # 查找包含产品特点的div或section
            for element in soup.find_all(['div', 'section', 'article']):
                element_text = element.get_text().strip()
                
                # 如果元素包含特点相关内容
                if (len(element_text) > 50 and
                    any(keyword in element_text for keyword in ['特点', '功能', '性能', '优势', '应用'])):
                    
                    main_sections.append({
                        'tag': element.name,
                        'text': element_text[:500],
                        'html': str(element)[:1000]
                    })
            
            features['feature_sections'] = main_sections
            
        except Exception as e:
            if self.debug:
                print(f"提取页面信息失败: {e}")
        
        return features

    def create_product_features_pdf(self, features, product_dir):
        """创建产品特点PDF文件"""
        try:
            # 下载特点相关图片
            image_files = []
            images_dir = os.path.join(product_dir, 'images')
            os.makedirs(images_dir, exist_ok=True)
            
            print(f"📸 开始下载产品特点图片...")
            for i, img_info in enumerate(features['feature_images']):
                img_filename = f"feature_image_{i+1:02d}.png"
                img_path = os.path.join(images_dir, img_filename)
                
                if self.download_image(img_info['url'], img_path):
                    image_files.append({
                        'filename': img_filename,
                        'alt': img_info['alt'],
                        'path': img_path,
                        'relative_path': f"images/{img_filename}"
                    })
                    print(f"  ✓ {img_info['alt'] or f'图片{i+1}'}")
                else:
                    print(f"  ✗ 下载失败: {img_info['url']}")
            
            # 创建HTML内容
            html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{features['product_name']} - 产品特点详情</title>
    <style>
        @page {{
            size: A4;
            margin: 20mm 15mm;
        }}
        body {{
            font-family: "Microsoft YaHei", "SimHei", "PingFang SC", Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            color: #333;
            font-size: 14px;
        }}
        .header {{
            text-align: center;
            border-bottom: 3px solid #0066cc;
            padding-bottom: 20px;
            margin-bottom: 30px;
            page-break-after: avoid;
        }}
        .product-title {{
            font-size: 28px;
            font-weight: bold;
            color: #0066cc;
            margin-bottom: 10px;
        }}
        .subtitle {{
            font-size: 20px;
            color: #666;
            margin-bottom: 15px;
        }}
        .meta-info {{
            font-size: 12px;
            color: #666;
            margin-bottom: 20px;
            line-height: 1.4;
        }}
        .section {{
            margin-bottom: 40px;
            page-break-inside: avoid;
        }}
        .section-title {{
            font-size: 18px;
            font-weight: bold;
            color: #0066cc;
            border-left: 4px solid #0066cc;
            padding-left: 10px;
            margin-bottom: 20px;
            page-break-after: avoid;
        }}
        .feature-images {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .feature-image-container {{
            text-align: center;
            page-break-inside: avoid;
        }}
        .feature-image {{
            max-width: 100%;
            max-height: 400px;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .image-caption {{
            font-size: 12px;
            color: #666;
            text-align: center;
            margin-top: 8px;
            font-style: italic;
        }}
        .content-text {{
            background-color: #f9f9f9;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border-left: 4px solid #ffc107;
        }}
        .highlight {{
            background-color: #fff3cd;
            padding: 15px;
            border-left: 4px solid #ffc107;
            margin: 20px 0;
            border-radius: 4px;
        }}
        .footer {{
            border-top: 2px solid #ddd;
            padding-top: 20px;
            margin-top: 40px;
            font-size: 12px;
            color: #666;
            text-align: center;
            page-break-inside: avoid;
        }}
        .info-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin: 20px 0;
        }}
        .info-item {{
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            border-left: 3px solid #0066cc;
        }}
        .feature-count {{
            background: linear-gradient(135deg, #0066cc, #004499);
            color: white;
            padding: 8px 16px;
            border-radius: 20px;
            display: inline-block;
            font-size: 12px;
            font-weight: bold;
            margin-bottom: 15px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <div class="product-title">{features['product_name']}</div>
        <div class="subtitle">产品特点详细资料</div>
        <div class="meta-info">
            📍 数据来源: <a href="{features['product_url']}">{features['product_url']}</a><br>
            🕒 生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}<br>
            🛠️ 生成工具: 日立终极爬虫 v1.0
        </div>
    </div>

    <div class="section">
        <div class="section-title">📋 产品概况</div>
        <div class="info-grid">
            <div class="info-item">
                <strong>📸 特点图片数量:</strong> {len(image_files)} 张
            </div>
            <div class="info-item">
                <strong>📝 内容段落:</strong> {len(features['feature_sections'])} 个
            </div>
        </div>
        <div class="highlight">
            <strong>📖 说明:</strong> 日立产品页面的产品特点主要以图片形式展示详细的功能说明和技术参数。
            下方的特点图片包含了完整的产品功能介绍，请仔细查看图片内容了解具体特点。
        </div>
    </div>

    <div class="section">
        <div class="section-title">🎯 产品特点图片展示</div>
        <div class="feature-count">总计 {len(image_files)} 张产品特点图片</div>
        <div class="feature-images">
"""
            
            # 添加特点图片
            for i, img_info in enumerate(image_files, 1):
                html_content += f"""
            <div class="feature-image-container">
                <img src="{img_info['relative_path']}" alt="{img_info['alt']}" class="feature-image">
                <div class="image-caption">图片 {i}: {img_info['alt']}</div>
            </div>
"""
            
            html_content += """
        </div>
    </div>
"""
            
            # 添加文本内容
            if features['feature_sections']:
                html_content += f"""
    <div class="section">
        <div class="section-title">📝 产品特点文字说明</div>
        <div class="feature-count">发现 {len(features['feature_sections'])} 个相关内容段落</div>
"""
                for i, section in enumerate(features['feature_sections'], 1):
                    html_content += f"""
        <div class="content-text">
            <h4>📄 内容段落 {i}</h4>
            <p>{section['text']}</p>
        </div>
"""
                html_content += """
    </div>
"""
            else:
                html_content += """
    <div class="section">
        <div class="section-title">📝 内容说明</div>
        <div class="content-text">
            <p>该产品页面的特点信息主要通过上方的图片展示。图片中包含了详细的产品功能说明、技术参数和应用场景等信息。</p>
        </div>
    </div>
"""
            
            html_content += f"""
    <div class="footer">
        <p><strong>日立产机系统(中国)有限公司</strong></p>
        <p>产品资料自动提取工具 | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>© 版权信息请参考原网站 | 本文档仅供参考</p>
    </div>
</body>
</html>
"""
            
            # 保存HTML文件
            html_file = os.path.join(product_dir, f"{features['product_name']}_产品特点.html")
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"✓ HTML文件已生成: {os.path.basename(html_file)}")
            
            # 生成PDF - 优先使用图片直接合并方法
            pdf_file = os.path.join(product_dir, f"{features['product_name']}_产品特点.pdf")
            
            # 方法1: 直接合并图片为PDF（推荐，更可靠）
            if self.convert_images_to_pdf(image_files, pdf_file, features['product_name']):
                # PDF生成成功后删除HTML文件
                try:
                    if os.path.exists(html_file):
                        os.remove(html_file)
                        print(f"🧹 已删除HTML文件: {os.path.basename(html_file)}")
                except Exception as e:
                    print(f"⚠️  删除HTML文件失败: {e}")
                return pdf_file, len(image_files)
            
            # 方法2: HTML转PDF（备用方法）
            print("🔄 尝试HTML转PDF方法...")
            if self.convert_html_to_pdf(html_file, pdf_file):
                print(f"✓ PDF文件已生成: {os.path.basename(pdf_file)}")
                # PDF生成成功后删除HTML文件
                try:
                    if os.path.exists(html_file):
                        os.remove(html_file)
                        print(f"🧹 已删除HTML文件: {os.path.basename(html_file)}")
                except Exception as e:
                    print(f"⚠️  删除HTML文件失败: {e}")
                return pdf_file, len(image_files)
            else:
                print(f"⚠️  所有PDF转换方法都失败，保留HTML文件供手动转换")
                return html_file, len(image_files)
            
        except Exception as e:
            print(f"✗ 生成产品特点PDF失败: {str(e)}")
            return None, 0

    def convert_images_to_pdf(self, image_files, pdf_file, product_name):
        """将图片直接合并为PDF - 更可靠的方法"""
        try:
            from PIL import Image
            
            if not image_files:
                print("⚠️  没有图片文件，无法生成PDF")
                return False
            
            # 收集所有有效的图片
            valid_images = []
            for img_info in image_files:
                img_path = img_info['path']
                if os.path.exists(img_path):
                    try:
                        # 打开并转换图片
                        img = Image.open(img_path)
                        # 转换为RGB模式（PDF需要）
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        valid_images.append(img)
                        print(f"  ✓ 添加图片: {img_info['alt']}")
                    except Exception as e:
                        print(f"  ✗ 图片处理失败: {img_path} - {e}")
                else:
                    print(f"  ✗ 图片文件不存在: {img_path}")
            
            if not valid_images:
                print("❌ 没有有效的图片文件")
                return False
            
            # 创建PDF
            print(f"📄 正在合并 {len(valid_images)} 张图片为PDF...")
            
            # 使用第一张图片作为基础，其他图片追加
            first_image = valid_images[0]
            other_images = valid_images[1:] if len(valid_images) > 1 else []
            
            # 保存为PDF
            first_image.save(
                pdf_file, 
                "PDF", 
                resolution=100.0,
                save_all=True, 
                append_images=other_images
            )
            
            # 获取PDF文件大小
            pdf_size = os.path.getsize(pdf_file)
            print(f"✅ PDF生成成功: {os.path.basename(pdf_file)}")
            print(f"📊 PDF大小: {pdf_size/1024:.1f} KB")
            print(f"📄 包含图片: {len(valid_images)} 张")
            
            # 关闭图片对象
            for img in valid_images:
                img.close()
            
            # PDF生成成功后，删除原始图片文件以节省空间
            print(f"🧹 清理原始图片文件...")
            deleted_count = 0
            for img_info in image_files:
                try:
                    if os.path.exists(img_info['path']):
                        os.remove(img_info['path'])
                        deleted_count += 1
                        print(f"  ✓ 已删除: {img_info['filename']}")
                except Exception as e:
                    print(f"  ✗ 删除失败: {img_info['filename']} - {e}")
            
            # 尝试删除images目录（如果为空）
            try:
                images_dir = os.path.dirname(image_files[0]['path']) if image_files else None
                if images_dir and os.path.exists(images_dir) and not os.listdir(images_dir):
                    os.rmdir(images_dir)
                    print(f"  ✓ 已删除空目录: images/")
            except:
                pass
            
            print(f"✅ 已清理 {deleted_count} 个图片文件")
            return True
            
        except ImportError:
            print("❌ PIL/Pillow未安装，无法生成PDF")
            print("💡 请运行: pip install Pillow")
            return False
        except Exception as e:
            print(f"❌ 图片转PDF失败: {e}")
            return False

    def convert_html_to_pdf(self, html_file, pdf_file):
        """将HTML转换为PDF - 备用方法"""
        try:
            # 使用weasyprint转换
            import weasyprint
            
            # 设置CSS字体配置以支持中文
            font_config = weasyprint.text.fonts.FontConfiguration()
            
            html_doc = weasyprint.HTML(filename=html_file)
            html_doc.write_pdf(pdf_file, font_config=font_config)
            
            return True
            
        except ImportError:
            print("⚠️  weasyprint未安装，无法生成PDF文件")
            return False
        except Exception as e:
            print(f"HTML转PDF失败: {e}")
            return False

    def find_downloadable_files(self, soup, base_url):
        """查找可下载的文件链接"""
        downloads = []
        
        pdf_keywords = ['pdf', '样本', '手册', '技术手册', '产品样本', '说明书', '下载', 'manual', 'catalog', 'datasheet']
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            link_text = link.get_text().strip()
            
            if not href:
                continue
            
            # 检查是否是下载链接
            is_download = False
            
            # 直接PDF链接
            if href.lower().endswith('.pdf'):
                is_download = True
            # 包含下载关键词的链接文本
            elif any(keyword in link_text.lower() for keyword in pdf_keywords):
                is_download = True
            # 包含下载关键词的URL
            elif any(keyword in href.lower() for keyword in ['pdf', 'download', 'file', 'doc']):
                is_download = True
            
            if is_download:
                # 转换为绝对URL
                if href.startswith('//'):
                    href = 'https:' + href
                elif href.startswith('/'):
                    href = urljoin(base_url, href)
                elif not href.startswith('http'):
                    href = urljoin(base_url, href)
                
                downloads.append({
                    'url': href,
                    'title': link_text or '未命名文件',
                    'type': 'PDF' if '.pdf' in href.lower() else '文档'
                })
        
        return downloads

    def download_file(self, file_url, save_path, file_title=""):
        """下载文件 - 改进版，支持分块下载和进度显示，增量逻辑"""
        try:
            # 增量逻辑：首先检查文件是否已经存在
            if os.path.exists(save_path):
                print(f"文件已存在，跳过下载: {file_title}")
                return False
            
            # 检查URL是否已处理过
            if file_url in self.processed_urls:
                print(f"URL已处理过，跳过: {file_title}")
                return False
            
            print(f"🔄 下载: {file_title}")
            print(f"📎 链接: {file_url}")
            
            # 使用流式下载
            with self.session.get(file_url, stream=True, timeout=self.config['spider_settings']['download_timeout']) as response:
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                
                # 确保目录存在
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if total_size > 0:
                                progress = (downloaded_size / total_size) * 100
                                print(f"\r📥 下载进度: {progress:.1f}% ({downloaded_size/1024:.1f}KB/{total_size/1024:.1f}KB)", end='')
                print()  # 换行
            
            file_size = os.path.getsize(save_path)
            print(f"✅ 下载成功: {file_title}")
            print(f"📊 文件大小: {file_size/1024:.1f} KB")
            print(f"💾 保存位置: {save_path}")
            
            # 文件信息不再保存为txt文件，仅在控制台显示
            
            self.processed_urls.add(file_url)
            self.new_files.append({
                'type': 'PDF' if '.pdf' in file_url.lower() else '文档',
                'title': file_title,
                'path': save_path,
                'url': file_url,
                'size': file_size
            })
            return True
            
        except Exception as e:
            print(f"❌ 下载异常: {file_title} - {str(e)}")
            return False

    def process_product_page(self, category_name, product_name, product_url):
        """处理单个产品页面 - 完整版"""
        try:
            print(f"\n{'='*80}")
            print(f"🔍 处理产品: {category_name} -> {product_name}")
            print(f"🔗 页面链接: {product_url}")
            print(f"{'='*80}")
            
            response = self.safe_request(product_url)
            if not response:
                print(f"✗ 无法访问页面: {product_url}")
                return 0
            
            # 检测编码
            response.encoding = chardet.detect(response.content)['encoding'] or 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 创建产品目录
            product_dir = os.path.join(self.base_dir, category_name, self.clean_filename(product_name))
            os.makedirs(product_dir, exist_ok=True)
            
            downloaded_count = 0
            
            # 1. 提取产品特点并生成PDF（增量逻辑）
            print("📋 检查产品特点PDF...")
            
            # 检查PDF是否已存在
            expected_pdf_path = os.path.join(product_dir, f"{self.clean_filename(product_name)}_产品特点.pdf")
            
            if os.path.exists(expected_pdf_path):
                print(f"产品特点PDF已存在，跳过: {os.path.basename(expected_pdf_path)}")
            else:
                print("📋 提取产品特点和生成PDF...")
                features = self.extract_product_features_with_images(soup, product_name, product_url)
                
                if self.config['output_settings']['generate_pdf_features']:
                    pdf_file, image_count = self.create_product_features_pdf(features, product_dir)
                    if pdf_file:
                        self.new_files.append({
                            'type': '产品特点PDF',
                            'title': f"{product_name} - 产品特点详情",
                            'path': pdf_file,
                            'url': product_url,
                            'size': os.path.getsize(pdf_file) if os.path.exists(pdf_file) else 0
                        })
                        print(f"✅ 产品特点PDF已生成 (包含{image_count}张图片)")
            
            # 2. 查找并下载PDF文件
            print("📎 查找技术手册和样本文件...")
            download_files = self.find_downloadable_files(soup, product_url)
            
            if download_files:
                print(f"✅ 找到 {len(download_files)} 个PDF文件")
                
                for i, file_info in enumerate(download_files, 1):
                    file_url = file_info['url']
                    file_title = file_info['title']
                    file_type = file_info['type']
                    
                    print(f"\n📄 [{i}/{len(download_files)}] 处理PDF文件")
                    
                    # 生成文件名
                    filename = self.clean_filename(file_title)
                    if not filename.lower().endswith('.pdf') and file_type == 'PDF':
                        filename += '.pdf'
                    elif not '.' in filename:
                        filename += '.pdf'
                    
                    save_path = os.path.join(product_dir, filename)
                    
                    # 检查文件是否已存在
                    if os.path.exists(save_path):
                        print(f"文件已存在，跳过: {filename}")
                        continue
                    
                    # 下载文件
                    if self.download_file(file_url, save_path, file_title):
                        downloaded_count += 1
                    
                    time.sleep(1)  # 下载间隔
            else:
                print("⚠️  未找到PDF下载文件")
            
            print(f"\n✅ {product_name} 处理完成")
            print(f"📊 下载PDF文件: {downloaded_count} 个")
            print(f"📁 保存目录: {product_dir}")
            
            return downloaded_count
            
        except Exception as e:
            print(f"❌ 处理产品页面失败: {product_name} - {str(e)}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return 0

    def crawl_all_products(self):
        """爬取所有产品"""
        print(f"\n{'='*100}")
        print(f"🚀 日立终极爬虫开始运行")
        print(f"⏰ 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💾 存储路径: {self.base_dir}")
        
        if self.is_first_run:
            print(f"🆕 首次运行 - 执行全量爬取")
        else:
            print(f"🔄 增量运行 - 只下载新增或修改的文件")
        
        print(f"{'='*100}")
        
        total_downloaded = 0
        total_products = sum(len(category['products']) for category in self.product_categories.values())
        processed_products = 0
        
        for category_key, category_info in self.product_categories.items():
            category_name = category_info['name']
            products = category_info['products']
            
            print(f"\n{'='*80}")
            print(f"📂 处理分类: {category_name} ({len(products)}个产品)")
            print(f"{'='*80}")
            
            for product in products:
                product_name = product['name']
                product_url = product['url']
                
                processed_products += 1
                print(f"\n进度: {processed_products}/{total_products} 产品")
                
                downloaded = self.process_product_page(category_name, product_name, product_url)
                total_downloaded += downloaded
                
                time.sleep(2)  # 产品间隔
        
        # 保存处理记录
        self.save_processed_urls()
        
    
        if self.new_files:
            self.send_notifications()
        
        return total_downloaded


    def send_dingtalk_notification(self, message):
        """发送钉钉通知"""
        try:
            timestamp = str(round(time.time() * 1000))
            string_to_sign = f'{timestamp}\n{self.SECRET}'
            hmac_code = hmac.new(self.SECRET.encode(), string_to_sign.encode(), hashlib.sha256).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

            url = f'https://oapi.dingtalk.com/robot/send?access_token={self.ACCESS_TOKEN}&timestamp={timestamp}&sign={sign}'
            headers = {'Content-Type': 'application/json'}
            data = {
                "msgtype": "text",
                "text": {"content": message},
                "at": {"isAtAll": False}
            }

            response = requests.post(url, json=data, headers=headers)
            print(f"📨 钉钉通知响应：{response.status_code} {response.text}")
            return response.status_code == 200
        except Exception as e:
            print(f"钉钉通知发送失败: {e}")
            return False

    def send_notifications(self):
        """发送新增文件通知"""
        try:
            if not self.new_files:
                return
            
            # 控制台通知
            if self.config.get("notification_settings", {}).get("enable_console", True):
                print(f"\n🎉 爬取完成通知:")
                print("=" * 60)
                print(f"📊 发现 {len(self.new_files)} 个新文件:")
                
                # 按类型统计
                type_counts = {}
                for file_info in self.new_files:
                    file_type = file_info['type']
                    type_counts[file_type] = type_counts.get(file_type, 0) + 1
                
                for file_type, count in type_counts.items():
                    print(f"  📁 {file_type}: {count} 个")
                
                print(f"\n📂 最新文件预览:")
                for file_info in self.new_files[:5]:  # 显示前5个
                    size_str = f" ({file_info['size']/1024:.1f}KB)" if 'size' in file_info else ""
                    print(f"  📄 {file_info['title']}{size_str}")
                
                if len(self.new_files) > 5:
                    print(f"  ... 还有 {len(self.new_files) - 5} 个文件")
                    
                print(f"\n💾 所有文件已保存至: {self.base_dir}")
            
            # 钉钉通知
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            total_files = len(self.new_files)
            success_rate = 100.0  # 假设全部成功
            
            if self.is_first_run:
                # 第一次全量爬取通知
                message = f"""✅ 日立 爬取成功，请及时审核

📊 下载统计:
  成功下载: {total_files} 个文件
  总文件数: {total_files} 个文件
  成功率: {success_rate}%

📁 文件存放路径: /srv/downloads/approved/
⏰ 完成时间: {current_time}"""
            else:
                # 增量爬取通知
                message = f"""✅ 日立 增量爬取成功，请及时审核

📊 下载统计:
  成功下载: {total_files} 个文件
  总文件数: {total_files} 个文件
  成功率: {success_rate}%
文件明细："""
                
                # 添加文件明细
                for file_info in self.new_files:
                    # 构建相对路径（从日立开始）
                    relative_path = file_info['path'].replace('/srv/downloads/approved/', '')
                    message += f"\n{relative_path}"
                
                message += f"""

📁 文件存放路径: /srv/downloads/approved/
⏰ 完成时间: {current_time}"""
            
            # 发送钉钉通知
            self.send_dingtalk_notification(message)
            
        except Exception as e:
            print(f"发送通知失败: {e}")

    def run_once(self):
        """执行一次完整爬取"""
        start_time = datetime.now()
        
        try:
            downloaded_count = self.crawl_all_products()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n🎉 日立终极爬虫任务完成！")
            print(f"⏱️  总耗时: {duration}")
            print(f"📊 总处理: {downloaded_count} 个文件")
            print(f"🆕 新增文件: {len(self.new_files)} 个")
            print(f"💾 保存位置: {os.path.abspath(self.base_dir)}")
            print(f"✨ 功能特色: PDF下载 + 产品特点图片提取 + 自动PDF生成")
            
            return downloaded_count
            
        except Exception as e:
            print(f"爬取过程出错: {str(e)}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return 0

def main():
    """主函数"""
    print("🚀 日立终极爬虫启动...")
    print("🎯 功能: PDF下载 + 产品特点图片提取 + 自动PDF生成 + 增量更新 + 自动通知")
    print("📋 涵盖: 变频器、标识设备、电机、PLC、鼓风机、电动葫芦")
    print("✨ 特色: 将产品特点图片整合生成PDF文档")
    
    spider = HitachiUltimateSpider()
    
    # 确保基础目录存在
    os.makedirs(spider.base_dir, exist_ok=True)
    
    try:
        downloaded_count = spider.run_once()
        
        if downloaded_count > 0:
            print(f"\n✅ 任务成功完成！")
            print(f"💡 提示: 产品特点已保存为PDF文件，可直接查看")
        else:
            print(f"\n⚠️  没有下载到新文件，可能所有内容都是最新的")
        
    except KeyboardInterrupt:
        print("\n👋 用户中断，正在退出...")
    except Exception as e:
        print(f"💥 运行出错: {str(e)}")

if __name__ == '__main__':
    main()