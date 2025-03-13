import json
import os
import glob
from typing import Dict, Any, List, Optional

def convert_pixiv_novel_to_target_format(pixiv_novel_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    将Pixiv小说JSON数据转换为目标格式
    
    Args:
        pixiv_novel_data: Pixiv小说的原始JSON数据
        
    Returns:
        转换后的目标格式JSON数据
    """
    # 确保数据存在
    if not pixiv_novel_data or 'novel' not in pixiv_novel_data:
        raise ValueError('Invalid Pixiv novel data structure')
    
    # 获取第一个小说对象的ID和数据
    novel_id = list(pixiv_novel_data['novel'].keys())[0]
    novel_data = pixiv_novel_data['novel'][novel_id]
    
    # 获取标签数组
    tags_array = []
    if 'tags' in novel_data and 'tags' in novel_data['tags']:
        tags_array = [tag.get('tag', '') for tag in novel_data['tags']['tags']]
    
    # 构建结果对象
    result = {
        "metadata": {
            "Rating": [],
            "Archive Warning": [],
            "Category": [],
            "Fandom": [],
            "Relationship": [],
            "Characters": [],
            "Additional Tags": [],
            "Language": novel_data.get('language', ''),
            "Published": novel_data.get('createDate', ''),
            "Completed": novel_data.get('updateDate', novel_data.get('createDate', '')),
            "Words": format_number(novel_data.get('wordCount', 0)),
            "Chapters": "1/1",  # 假设是单章节作品，可根据实际情况调整
            "Kudos": format_number(novel_data.get('likeCount', 0)),
            "Bookmarks": format_number(novel_data.get('bookmarkCount', 0)),
            "Hits": format_number(novel_data.get('viewCount', 0)),
            "Kudos Users": [],
            "Work ID": novel_id,
            "Work Type": "Single-Chapter"  # 默认为单章节，可根据实际情况调整
        },
        "content": {
            "title": novel_data.get('title', ''),
            "author": novel_data.get('userName', ''),
            "summary": novel_data.get('description', ''),
            "chapters": [
                {
                    "title": "Chapter 1",
                    "number": "1",
                    "content": [novel_data.get('content', '')] if novel_data.get('content') else []
                }
            ],
            "text": []
        }
    }
    
    # 处理标签分类
    for tag in tags_array:
        # 根据标签类型进行分类
        if 'R-18' in tag or 'R18' in tag:
            result['metadata']['Rating'].append(tag)
        elif is_relationship_tag(tag):
            result['metadata']['Relationship'].append(tag)
        elif is_fandom_tag(tag):
            result['metadata']['Fandom'].append(tag)
        elif is_character_tag(tag):
            result['metadata']['Characters'].append(tag)
        else:
            result['metadata']['Additional Tags'].append(tag)
    
    # 如果没有R-18标签但xRestrict为1，则添加R-18标签
    if not result['metadata']['Rating'] and novel_data.get('xRestrict') == 1:
        result['metadata']['Rating'].append("R-18")
    
    return result

def format_number(num: int) -> str:
    """格式化数字为带千分位的字符串"""
    return f"{num:,}"

def is_relationship_tag(tag: str) -> bool:
    """判断是否为关系标签"""
    # 这里使用简单示例判断，可根据实际需求完善
    relationship_indicators = ['x', '/', 'カプ', 'CP']
    return any(indicator in tag for indicator in relationship_indicators)

def is_fandom_tag(tag: str) -> bool:
    """判断是否为作品标签"""
    # 判断标签是否代表一个作品/系列
    fandom_keywords = ['シリーズ', '作品', 'アニメ', 'ゲーム', '漫画']
    return any(keyword in tag for keyword in fandom_keywords)

def is_character_tag(tag: str) -> bool:
    """判断是否为角色标签"""
    # 判断标签是否代表一个角色
    # 可以根据已知的角色名列表或特定规则判断
    return False  # 默认返回False，需要根据实际情况完善

def process_files(input_dir: str, output_dir: str, file_pattern: str = "novel_*.json"):
    """
    处理输入目录中所有匹配的文件并转换到输出目录
    
    Args:
        input_dir: 输入文件目录
        output_dir: 输出文件目录
        file_pattern: 文件匹配模式
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取匹配的文件列表
    file_pattern_path = os.path.join(input_dir, file_pattern)
    matching_files = glob.glob(file_pattern_path)
    
    if not matching_files:
        print(f"没有找到匹配的文件：{file_pattern_path}")
        return
    
    # 处理每个文件
    success_count = 0
    error_count = 0
    
    for input_file in matching_files:
        try:
            # 提取文件名（不含路径和扩展名）
            file_name = os.path.basename(input_file)
            base_name = os.path.splitext(file_name)[0]
            output_file = os.path.join(output_dir, f"converted_{base_name}.json")
            
            # 读取输入文件
            with open(input_file, 'r', encoding='utf-8') as f:
                pixiv_data = json.load(f)
            
            # 转换数据
            converted_data = convert_pixiv_novel_to_target_format(pixiv_data)
            
            # 写入输出文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(converted_data, f, ensure_ascii=False, indent=2)
            
            print(f"成功: {input_file} → {output_file}")
            success_count += 1
            
        except Exception as e:
            print(f"处理 {input_file} 时出错: {str(e)}")
            error_count += 1
    
    # 输出统计信息
    print(f"\n处理完成! 成功: {success_count}, 失败: {error_count}")

def main():
    """主函数：设置输入输出目录并启动处理"""
    import argparse
    
    parser = argparse.ArgumentParser(description='批量转换Pixiv小说JSON文件')
    parser.add_argument('--input', '-i', default='.', help='输入文件目录 (默认: 当前目录)')
    parser.add_argument('--output', '-o', default='./converted', help='输出文件目录 (默认: ./converted)')
    parser.add_argument('--pattern', '-p', default='novel_*.json', help='文件匹配模式 (默认: novel_*.json)')
    
    args = parser.parse_args()
    
    print(f"开始处理文件...")
    print(f"输入目录: {args.input}")
    print(f"输出目录: {args.output}")
    print(f"文件模式: {args.pattern}")
    
    process_files(args.input, args.output, args.pattern)

if __name__ == "__main__":
    main()