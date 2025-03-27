// 引入Node.js文件系统模块
const fs = require('fs');

/**
 * 读取JSON文件并统计重复字符串
 * @param {string} filename - JSON文件路径
 * @returns {Object} 统计结果
 */
function countDuplicateStrings(filename) {
  try {
    // 同步读取文件
    const fileContent = fs.readFileSync(filename, 'utf8');
    
    // 解析JSON字符串为JavaScript数组
    const stringArray = JSON.parse(fileContent);
    
    // 验证输入是字符串数组
    if (!Array.isArray(stringArray)) {
      throw new Error('文件内容不是数组');
    }
    
    if (!stringArray.every(item => typeof item === 'string')) {
      throw new Error('数组中有非字符串元素');
    }
    
    // 使用Map计数每个字符串出现的次数
    const countMap = new Map();
    
    for (const str of stringArray) {
      countMap.set(str, (countMap.get(str) || 0) + 1);
    }
    
    // 创建结果对象：只包含出现多次的字符串
    const duplicates = {};
    
    for (const [str, count] of countMap.entries()) {
      if (count > 1) {
        duplicates[str] = count;
      }
    }
    
    return {
      totalStrings: stringArray.length,
      uniqueStrings: countMap.size,
      duplicateStrings: duplicates
    };
    
  } catch (error) {
    console.error('处理文件时出错:', error.message);
    throw error;
  }
}

// 使用示例
const filename = 'novel_image_ids.json';
try {
  const result = countDuplicateStrings(filename);
  console.log('统计结果:', result);
} catch (error) {
  console.error('错误:', error.message);
}

// 导出函数，以便可以在其他模块中使用
module.exports = countDuplicateStrings;