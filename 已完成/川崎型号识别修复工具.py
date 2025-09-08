#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json

class KawasakiModelFixer:
    def __init__(self):
        self.base_url = "https://kawasakirobotics.cn"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 服务器路径
        if os.path.exists("/srv/downloads/approved/"):
            self.base_dir = "/srv/downloads/approved/川崎"
        else:
            self.base_dir = os.path.join(os.getcwd(), "downloads", "川崎")
    
    def log(self, message):
        """日志输出"""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    def get_product_page_info(self, product_url):
        """获取产品页面信息，尝试提取完整型号"""
        try:
            self.log(f"🔍 正在分析产品页面: {product_url}")
            
            response = self.session.get(product_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 从分类页面直接提取所有产品型号
            content = soup.get_text()
            
            # 查找所有型号
            models = []
            
            # 方法1: 查找所有型号模式
            model_patterns = [
                r'([A-Z]{1,3}\d{3}[NLHX]?)',  # RS003N, RS003L等
                r'([A-Z]{1,3}\d{3})\s*([NLHX])',  # RS003 N 这种格式
            ]
            
            for pattern in model_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        # 处理分离的格式
                        model = f"{match[0]}{match[1]}"
                    else:
                        model = match
                    
                    if model not in models:
                        models.append(model.upper())
            
            # 方法2: 从页面标题提取
            title = soup.find('title')
            if title:
                title_text = title.get_text().strip()
                self.log(f"📄 页面标题: {title_text}")
                
                title_model = self.extract_model_from_text(title_text)
                if title_model and title_model not in models:
                    models.append(title_model)
            
            # 方法3: 从URL路径提取基础型号
            url_model = self.extract_base_model_from_url(product_url)
            if url_model and url_model not in models:
                models.append(url_model)
            
            # 去重并排序
            models = list(set(models))
            models.sort()
            
            if models:
                self.log(f"🏷️ 从页面提取到型号: {', '.join(models)}")
                return models
            else:
                self.log("⚠️ 未从页面提取到型号信息")
                return None
            
        except Exception as e:
            self.log(f"❌ 获取产品页面信息失败: {str(e)}")
            return None
    
    def extract_model_from_text(self, text):
        """从文本中提取型号"""
        patterns = [
            r'([A-Z]{1,3}\d{3}[NLHX]?)',  # RS003N, RS003L等，包括YF002N
            r'([A-Z]{1,3}\d{3})\s*([NLHX])',  # RS003 N 这种格式
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    # 处理分离的格式
                    return f"{match.group(1)}{match.group(2)}"
                else:
                    return match.group(1)
        
        return None
    
    def extract_base_model_from_url(self, url):
        """从URL中提取基础型号（不含后缀）"""
        try:
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')
            
            for part in path_parts:
                # 匹配基础型号模式，包括Y前缀
                match = re.search(r'([A-Z]{1,3}\d{3})', part, re.IGNORECASE)
                if match:
                    return match.group(1).upper()
            
            return None
            
        except Exception as e:
            self.log(f"❌ 从URL提取基础型号失败: {str(e)}")
            return None
    
    def get_base_model_from_full_model(self, full_model):
        """从完整型号中提取基础型号（不含后缀）"""
        # 匹配模式：字母+数字+可选后缀
        match = re.match(r'^([A-Z]{1,3}\d{3})[NLHX]?$', full_model, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        return None
    
    def find_matching_models(self, folder_name, model_mapping):
        """查找与文件夹名称匹配的型号"""
        matches = []
        
        # 方法1: 直接匹配完整型号
        if folder_name in model_mapping:
            matches.extend(model_mapping[folder_name])
        
        # 方法2: 提取基础型号后匹配
        base_model = self.get_base_model_from_full_model(folder_name)
        if base_model and base_model in model_mapping:
            matches.extend(model_mapping[base_model])
        
        # 方法3: 处理缺少前缀的情况（如F002 -> YF002）
        if folder_name.startswith('F') and len(folder_name) >= 4:
            # 尝试添加Y前缀
            y_prefixed = 'Y' + folder_name
            if y_prefixed in model_mapping:
                matches.extend(model_mapping[y_prefixed])
            
            # 也检查基础型号
            y_base = self.get_base_model_from_full_model(y_prefixed)
            if y_base and y_base in model_mapping:
                matches.extend(model_mapping[y_base])
        
        # 方法4: 处理缺少前缀的情况（如F002N -> YF002N）
        if folder_name.startswith('F') and len(folder_name) >= 5:
            # 尝试添加Y前缀
            y_prefixed = 'Y' + folder_name
            if y_prefixed in model_mapping:
                matches.extend(model_mapping[y_prefixed])
        
        return list(set(matches))  # 去重
    
    def select_best_match(self, folder_name, matching_models):
        """从多个匹配型号中选择最佳匹配"""
        if not matching_models:
            return None
        
        if len(matching_models) == 1:
            return matching_models[0]
        
        # 优先选择与文件夹名称最相似的
        best_match = None
        best_score = 0
        
        for model in matching_models:
            score = 0
            
            # 完全匹配得分最高
            if model == folder_name:
                score = 100
            # 包含关系得分次之
            elif folder_name in model or model in folder_name:
                score = 80
            # 长度相似得分再次
            elif abs(len(model) - len(folder_name)) <= 1:
                score = 60
            # 前缀匹配得分
            elif model.startswith(folder_name[:3]) or folder_name.startswith(model[:3]):
                score = 40
            
            if score > best_score:
                best_score = score
                best_match = model
        
        # 如果没有找到好的匹配，返回第一个
        if best_match is None:
            best_match = matching_models[0]
        
        return best_match
    
    def infer_model_suffix(self, base_model, content):
        """推断型号后缀"""
        try:
            # 在内容中查找包含基础型号的完整型号
            pattern = f"{base_model}[NLHX]"
            matches = re.findall(pattern, content, re.IGNORECASE)
            
            if matches:
                return matches[0].upper()
            
            # 如果没有找到，尝试从产品特性推断
            if '负载' in content or 'payload' in content.lower():
                # 根据负载信息推断后缀
                if '80kg' in content or '80 kg' in content:
                    return f"{base_model}N"  # 标准负载
                elif '150kg' in content or '150 kg' in content:
                    return f"{base_model}L"  # 长臂
                elif '300kg' in content or '300 kg' in content:
                    return f"{base_model}H"  # 高负载
            
            # 默认返回基础型号
            return base_model
            
        except Exception as e:
            self.log(f"❌ 推断型号后缀失败: {str(e)}")
            return base_model
    
    def find_product_urls(self):
        """查找所有产品页面的URL"""
        try:
            self.log("🔍 正在查找产品页面URL...")
            
            # 机器人产品页面
            robot_urls = [
                'https://kawasakirobotics.cn/robots-category/small-medium-payloads/',
                'https://kawasakirobotics.cn/robots-category/large-payloads/',
                'https://kawasakirobotics.cn/robots-category/extra-large-payloads/',
                'https://kawasakirobotics.cn/robots-category/dual-arm-scara/',
                'https://kawasakirobotics.cn/robots-category/palletizing/',
                'https://kawasakirobotics.cn/robots-category/pick-and-place/',
                'https://kawasakirobotics.cn/robots-category/medical/',
                'https://kawasakirobotics.cn/robots-category/arc-welding/',
                'https://kawasakirobotics.cn/robots-category/painting/',
                'https://kawasakirobotics.cn/robots-category/wafer/'
            ]
            
            product_urls = []
            
            for category_url in robot_urls:
                try:
                    self.log(f"🔍 正在分析分类页面: {category_url}")
                    
                    response = self.session.get(category_url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # 直接从分类页面提取产品信息，而不是寻找product链接
                    # 川崎官网的产品信息是直接在分类页面上的
                    product_urls.append(category_url)
                    
                    time.sleep(1)  # 避免请求过快
                    
                except Exception as e:
                    self.log(f"❌ 分析分类页面失败 {category_url}: {str(e)}")
                    continue
            
            self.log(f"✅ 找到 {len(product_urls)} 个分类页面")
            return product_urls
            
        except Exception as e:
            self.log(f"❌ 查找产品页面失败: {str(e)}")
            return []
    
    def create_model_mapping(self):
        """创建型号映射表"""
        try:
            self.log("🔍 正在创建型号映射表...")
            
            product_urls = self.find_product_urls()
            model_mapping = {}
            
            for i, url in enumerate(product_urls):
                self.log(f"📊 进度: {i+1}/{len(product_urls)}")
                
                models = self.get_product_page_info(url)
                if models:
                    # 处理返回的型号列表
                    for model in models:
                        # 提取基础型号（不含后缀）
                        base_model = re.match(r'([A-Z]{1,3}\d{3})', model)
                        if base_model:
                            base = base_model.group(1)
                            if base not in model_mapping:
                                model_mapping[base] = []
                            if model not in model_mapping[base]:
                                model_mapping[base].append(model)
                
                time.sleep(0.5)  # 避免请求过快
            
            # 保存映射表
            mapping_file = os.path.join(self.base_dir, 'model_mapping.json')
            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump(model_mapping, f, ensure_ascii=False, indent=2)
            
            self.log(f"✅ 型号映射表已保存到: {mapping_file}")
            return model_mapping
            
        except Exception as e:
            self.log(f"❌ 创建型号映射表失败: {str(e)}")
            return {}
    
    def fix_existing_folders(self):
        """修复现有文件夹的命名"""
        try:
            self.log("🔧 正在修复现有文件夹命名...")
            
            # 加载型号映射表
            mapping_file = os.path.join(self.base_dir, 'model_mapping.json')
            if not os.path.exists(mapping_file):
                self.log("❌ 型号映射表不存在，请先运行 create_model_mapping()")
                return
            
            with open(mapping_file, 'r', encoding='utf-8') as f:
                model_mapping = json.load(f)
            
            # 定义所有机器人类别目录
            robot_categories = [
                'SCARA机器人',
                '中小型通用机器人~80kg负载',
                '医药机器人', 
                '协作机器人',
                '喷涂机器人',
                '大型通用机器人~300kg负载',
                '晶圆搬运机器人',
                '涂胶机器人',
                '焊接_切割机器人',
                '码垛机器人',
                '超大型通用机器人~1,500kg负载',
                '高速分拣机器人'
            ]
            
            total_fixed = 0
            
            for category in robot_categories:
                robot_dir = os.path.join(self.base_dir, '机器人', category)
                if not os.path.exists(robot_dir):
                    self.log(f"⚠️ 目录不存在，跳过: {robot_dir}")
                    continue
                
                self.log(f"🔧 正在修复类别: {category}")
                category_fixed = 0
                
                for item in os.listdir(robot_dir):
                    item_path = os.path.join(robot_dir, item)
                    
                    if os.path.isdir(item_path):
                        # 使用新的匹配方法查找对应的型号
                        matching_models = self.find_matching_models(item, model_mapping)
                        
                        if matching_models:
                            if len(matching_models) == 1:
                                # 只有一个匹配型号，直接重命名
                                new_name = matching_models[0]
                                new_path = os.path.join(robot_dir, new_name)
                                
                                if not os.path.exists(new_path):
                                    try:
                                        os.rename(item_path, new_path)
                                        self.log(f"  ✅ 重命名: {item} -> {new_name}")
                                        category_fixed += 1
                                    except Exception as e:
                                        self.log(f"  ❌ 重命名失败 {item}: {str(e)}")
                                else:
                                    self.log(f"  ⚠️ 目标文件夹已存在: {new_name}")
                            
                            elif len(matching_models) > 1:
                                # 多个匹配型号，需要用户选择
                                self.log(f"  🔍 {item} 有多个型号选择: {matching_models}")
                                # 优先选择与文件夹名称最匹配的
                                best_match = self.select_best_match(item, matching_models)
                                if best_match:
                                    new_path = os.path.join(robot_dir, best_match)
                                    if not os.path.exists(new_path):
                                        try:
                                            os.rename(item_path, new_path)
                                            self.log(f"  ✅ 重命名: {item} -> {best_match}")
                                            category_fixed += 1
                                        except Exception as e:
                                            self.log(f"  ❌ 重命名失败 {item}: {str(e)}")
                                    else:
                                        self.log(f"  ⚠️ 目标文件夹已存在: {best_match}")
                        else:
                            self.log(f"  ⚠️ 未找到匹配型号: {item}")
                
                if category_fixed > 0:
                    self.log(f"  📊 类别 {category} 修复了 {category_fixed} 个文件夹")
                else:
                    self.log(f"  📊 类别 {category} 无需修复")
                
                total_fixed += category_fixed
            
            self.log(f"✅ 修复完成，共修复 {total_fixed} 个文件夹")
            
        except Exception as e:
            self.log(f"❌ 修复文件夹失败: {str(e)}")
    
    def show_folder_status(self):
        """显示文件夹状态"""
        try:
            # 定义所有机器人类别目录
            robot_categories = [
                'SCARA机器人',
                '中小型通用机器人~80kg负载',
                '医药机器人', 
                '协作机器人',
                '喷涂机器人',
                '大型通用机器人~300kg负载',
                '晶圆搬运机器人',
                '涂胶机器人',
                '焊接_切割机器人',
                '码垛机器人',
                '超大型通用机器人~1,500kg负载',
                '高速分拣机器人'
            ]
            
            total_folders = 0
            total_need_fix = 0
            
            for category in robot_categories:
                robot_dir = os.path.join(self.base_dir, '机器人', category)
                if not os.path.exists(robot_dir):
                    self.log(f"⚠️ 目录不存在: {robot_dir}")
                    continue
                
                self.log(f"📊 类别: {category}")
                self.log("-" * 40)
                
                folders = os.listdir(robot_dir)
                folders.sort()
                category_folders = 0
                category_need_fix = 0
                
                for folder in folders:
                    if os.path.isdir(os.path.join(robot_dir, folder)):
                        category_folders += 1
                        # 检查是否包含后缀和前缀
                        if re.match(r'^[A-Z]{1,3}\d{3}[NLHX]$', folder):
                            status = "✅ 正确"
                        elif folder.startswith('F') and len(folder) >= 4:
                            # 检查是否是缺少Y前缀的型号
                            status = "❌ 缺少Y前缀"
                            category_need_fix += 1
                        elif re.match(r'^[A-Z]{1,3}\d{3}$', folder):
                            status = "❌ 缺少后缀"
                            category_need_fix += 1
                        else:
                            status = "❌ 格式错误"
                            category_need_fix += 1
                        
                        self.log(f"  {folder:<15} {status}")
                
                self.log(f"  总计: {category_folders} 个文件夹, 需要修复: {category_need_fix} 个")
                self.log("")
                
                total_folders += category_folders
                total_need_fix += category_need_fix
            
            self.log("=" * 50)
            self.log(f"📊 总体统计: 共 {total_folders} 个文件夹, 需要修复 {total_need_fix} 个")
            self.log("=" * 50)
            
        except Exception as e:
            self.log(f"❌ 显示文件夹状态失败: {str(e)}")

def main():
    """主函数"""
    fixer = KawasakiModelFixer()
    
    print("🔧 川崎机器人型号识别修复工具")
    print("=" * 50)
    print("1. 显示当前文件夹状态")
    print("2. 创建型号映射表")
    print("3. 修复现有文件夹")
    print("4. 退出")
    print("=" * 50)
    
    while True:
        try:
            choice = input("请选择操作 (1-4): ").strip()
            
            if choice == '1':
                fixer.show_folder_status()
            elif choice == '2':
                fixer.create_model_mapping()
            elif choice == '3':
                fixer.fix_existing_folders()
            elif choice == '4':
                print("👋 再见！")
                break
            else:
                print("❌ 无效选择，请输入 1-4")
            
            print()
            
        except KeyboardInterrupt:
            print("\n👋 用户中断，再见！")
            break
        except Exception as e:
            print(f"❌ 操作失败: {str(e)}")

if __name__ == "__main__":
    main()
