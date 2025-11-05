##step 1 get data
import requests
import pandas as pd
from notion_client import Client
import logging
import re
import json
import os
import time

# Load NOTION_API_KEY from project document (config.json)
CONFIG_FILE = "config.json"
HEADERS = {}

def load_notion_api_key():
    """
    Load NOTION_API_KEY from config.json. If not found, guide user to create it.
    """
    if not os.path.exists(CONFIG_FILE):
        logging.error(f"找不到 {CONFIG_FILE}，请在项目根目录创建该文件并写入 NOTION_API_KEY。示例: { '{"NOTION_API_KEY": "your_secret_key"}' }")
        raise FileNotFoundError(f"缺少 {CONFIG_FILE}")
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        try:
            cfg = json.load(f)
        except json.JSONDecodeError:
            logging.error(f"{CONFIG_FILE} 内容不是有效的 JSON，请修复后重试。")
            raise
    api_key = cfg.get("NOTION_API_KEY")
    if not api_key:
        raise ValueError("config.json 中缺少 NOTION_API_KEY")
    return api_key

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Used to recursively get all blocks and their child blocks
def get_all_blocks(block_id, max_retries=3):
    url = f"https://api.notion.com/v1/blocks/{block_id}/children?page_size=100"
    blocks = []
    has_more = True
    start_cursor = None

    while has_more:
        attempt = 0
        data = None
        while attempt < max_retries:
            try:
                params = {"start_cursor": start_cursor} if start_cursor else None
                response = requests.get(url, headers=HEADERS, params=params)
                if response.ok:
                    logging.info(f"访问成功：{url}")
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt < max_retries:
                    logging.warning(f"访问失败（第{attempt}次），准备重试... 错误：{e}")
                    time.sleep(min(2 * attempt, 5))
                else:
                    logging.error(f"访问失败，已达到最大重试次数 {max_retries}。错误：{e}")
                    return blocks
            except Exception as e:
                logging.error(f"获取 blocks 发生未预期错误：{e}")
                return blocks

        results = data.get('results', []) if data else []
        for block in results:
            blocks.append(block)
            # If the block has children, recursively get them
            if block.get('has_children', False):
                child_blocks = get_all_blocks(block['id'], max_retries=max_retries)
                blocks.extend(child_blocks)

        has_more = data.get('has_more', False) if data else False
        start_cursor = data.get('next_cursor') if data else None

    logging.info(f"Fetched {len(blocks)} blocks from Notion.")
    return blocks

def get_notion_page_content(page_id, max_retries=3):
    logging.info(f"Getting content for Notion page: {page_id}")
    blocks = get_all_blocks(page_id, max_retries=max_retries)
    if not blocks:
        logging.warning("No blocks found for the given page.")
    return blocks


## Step 2: Convert blocks to DataFrame
def blocks_to_dataframe(blocks):
    data = []
    for block in blocks:
        block_type = block['type']
        content = ''
        
        # Handle block types with rich_text
        if 'rich_text' in block.get(block_type, {}):
            for item in block[block_type]['rich_text']:
                if item['type'] == 'text':
                    content += item['text']['content']
                elif item['type'] == 'equation':
                    content += f"$$ {item['equation']['expression']} $$"
        # Handle other types of blocks, such as code blocks
        elif block_type == 'code':
            content += block['code']['text'][0]['text']['content']
        # Handle quote blocks
        elif block_type == 'quote':
            for item in block['quote']['rich_text']:
                if item['type'] == 'text':
                    content += item['text']['content']
                elif item['type'] == 'equation':
                    content += f"$$ {item['equation']['expression']} $$"
        # Handle equation blocks (block type 'equation')
        elif block_type == 'equation':
            # Equation blocks have a single expression
            content += f"$$ {block['equation']['expression']} $$"
        # Other possible block types can be added here

        data.append({'id': block['id'], 'type': block_type, 'content': content})
    
    logging.info(f"Converted {len(data)} blocks to DataFrame.")
    return pd.DataFrame(data)

def to_dataframe_safe(blocks):
    try:
        df_local = blocks_to_dataframe(blocks)
    except Exception as e:
        logging.error(f"Error converting blocks to DataFrame: {e}")
        df_local = pd.DataFrame([])
    return df_local

## Step 3: Process content, extract formulas and format
def format_content_for_notion(block):
    # Improved: Use regex to find all $$equation$$ and convert to equation blocks
    if isinstance(block, str):
        # Find all $$...$$ and split text accordingly
        # Support multi-line equations by using DOTALL
        pattern = re.compile(r'\$\$(.+?)\$\$', re.DOTALL)
        parts = []
        last_end = 0
        for m in pattern.finditer(block):
            # Add text before equation
            if m.start() > last_end:
                text_part = block[last_end:m.start()]
                if text_part:
                    parts.append({
                        "type": "text",
                        "text": {"content": text_part}
                    })
            # Add equation
            eq = m.group(1).strip()
            if eq:
                parts.append({
                    "type": "equation",
                    "equation": {"expression": eq}
                })
            last_end = m.end()
        # Add any remaining text
        if last_end < len(block):
            text_part = block[last_end:]
            if text_part:
                parts.append({
                    "type": "text",
                    "text": {"content": text_part}
                })
        # After extracting $$...$$ equations, handle inline $...$ equations within text parts
        final_parts = []
        inline_pattern = re.compile(r'\$(.+?)\$')
        for part in parts:
            if part.get('type') == 'text':
                text = part['text']['content']
                last = 0
                for m in inline_pattern.finditer(text):
                    if m.start() > last:
                        txt = text[last:m.start()]
                        if txt:
                            final_parts.append({'type': 'text', 'text': {'content': txt}})
                    expr = m.group(1).strip()
                    if expr:
                        final_parts.append({'type': 'equation', 'equation': {'expression': expr}})
                    last = m.end()
                # remaining text
                if last < len(text):
                    rem = text[last:]
                    if rem:
                        final_parts.append({'type': 'text', 'text': {'content': rem}})
            else:
                # equation blocks pass through
                final_parts.append(part)
        return final_parts
    else:
        # If the block is a dictionary, return directly
        return block

def combine_text_and_equations(df):
    combined_blocks = []

    for _, row in df.iterrows():
        content = row['content']
        notion_block_content = format_content_for_notion(content)

        # Handle divider type (no content needed)
        if row['type'] == "divider":
            combined_blocks.append({
                'type': 'divider',
                'divider': {}
            })
        
        # Handle heading types (heading_1, heading_2, heading_3)
        elif row['type'] == "heading_3" or row['type'] == "heading_2" or row['type'] == "heading_1":
            combined_blocks.append({
                'type': row['type'],
                row['type']: {
                    'rich_text': notion_block_content
                }
            })
        
        # Handle quote type
        elif row['type'] == "quote":
            combined_blocks.append({
                'type': 'quote',
                'quote': {
                    'rich_text': notion_block_content
                }
            })
        
        # Handle general paragraph type, ensure it's not empty and has correct structure
        elif row['type'] == "paragraph":
            if notion_block_content:  # Check that rich_text is not empty
                combined_blocks.append({
                    'type': 'paragraph',
                    'paragraph': {
                        'rich_text': notion_block_content
                    }
                })
        # Handle other block types (e.g., code)
        elif row['type'] == "code":
            combined_blocks.append({
                'type': 'code',
                'code': {
                    'text': notion_block_content,
                    'language': 'python'  # Set language according to actual situation
                }
            })
        # Other block types can be added here
        elif row['type'] == "bulleted_list_item":
            combined_blocks.append({
                'type': 'bulleted_list_item',
                'bulleted_list_item': {
                    'rich_text': notion_block_content
                }
            })

    return combined_blocks

def combine_safe(df_local):
    try:
        combined = combine_text_and_equations(df_local)
        logging.info(f"Combined data contains {len(combined)} blocks.")
    except Exception as e:
        logging.error(f"Error combining text and equations: {e}")
        combined = []
    return combined

##step 4 upload to notion
def upload_to_notion(page_id, combined_blocks):
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    children_blocks = combined_blocks
    payload = {
        "children": children_blocks
    }
    try:
        response = requests.patch(url, json=payload, headers=HEADERS)
        response.raise_for_status()
        logging.info(f"Successfully uploaded {len(children_blocks)} blocks to Notion page {page_id}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to upload blocks to Notion: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during upload: {e}")
        return None
    
# Batch upload in chunks to avoid rate limits
def upload_blocks_in_batches(page_id, combined_blocks, batch_size=10):
    total = len(combined_blocks)
    for i in range(0, total, batch_size):
        batch = combined_blocks[i:i+batch_size]
        logging.info(f"Uploading blocks {i+1} to {i+len(batch)} of {total}")
        upload_to_notion(page_id, batch)





def main():
    # Load API key from config and set headers
    api_key = load_notion_api_key()
    global HEADERS
    HEADERS = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }

    # Prompt PAGE_ID at runtime
    page_id = input("请输入 Notion 页面 ID（PAGE_ID）: ").strip()

    # Optional: set retry count
    retry_input = input("请输入最大重试次数(默认3): ").strip()
    try:
        max_retries = int(retry_input) if retry_input else 3
    except ValueError:
        logging.warning("输入的重试次数不是有效数字，默认使用 3 次重试。")
        max_retries = 3

    # Try to fetch content to confirm access success
    page_content = get_notion_page_content(page_id, max_retries=max_retries)

    # If only access confirmation is required, we already logged success when fetching.
    # Continue original flow: convert and upload
    df_local = to_dataframe_safe(page_content)
    combined_data = combine_safe(df_local)

    # Prompt user to manually clear page content in Notion
    input("请先在 Notion 页面中手动清空内容，然后按回车继续上传... ")

    # Proceed to upload processed blocks in batches
    if combined_data:
        upload_blocks_in_batches(page_id, combined_data, batch_size=10)
    else:
        logging.warning("No data to upload to Notion.")

if __name__ == "__main__":
    main()
