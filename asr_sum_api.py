import os
import random
import uvicorn
import sys
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import List
from utils import get_current_datatime, format2wav, load_audio_byte, hash_str, handle2wav, call_split_api, save_audio, call_with_retry, load_config, remove_file
import logging
import requests
import json
import email
from email import policy
import soundfile
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(module)s: %(message)s')
logger = logging.getLogger(__name__)

config = load_config('./config/config.yaml')
api_urls = config["apis"] 
split_url = config["split_url"]
TMP_DIR = "tmp"
CVT_DIR = "file_data"
STORY_DIR = "story_data"
# SEGMENT_DATA = "segment_data_audio"
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(CVT_DIR, exist_ok=True)
os.makedirs(STORY_DIR, exist_ok=True)
# os.makedirs(SEGMENT_DATA, exist_ok=True)

app = FastAPI()

@app.get("/")
def main():
    return {"message": "Welcome!"}

@app.post("/process_data/")
async def process_data(story: str = Form(...), data: List[UploadFile] = File(...)):
    min_samples = 0.5 * 16000
    story_data = {"data":[]}
    time_story = get_current_datatime(True)
    story = json.loads(story)
    story_name = story["story_name"]
    story_id = time_story + '_' + story_name
    story_data["story_id"] = str(hash_str(story_id))
    story_data["story_name"] = story_name
    story_data["created_date"] = time_story  
    
    result = {
        "success": False,
        "message": "split failed",
        "data": {}
    }
    
    list_file_data = {}
    for st in story["data"]:
        if st["type"] == 'chat':
            data_id = time_story 
            data_id = hash_str(data_id)
            chat_data = {
                "data_id": str(data_id), 
                "date": st["date"],
                "type": "chat", 
                "processed_text": st["text"]
            }
            story_data["data"].append(chat_data)
        else:
            list_file_data[str(st['file_index'])] = st
    
    logger.info(f'list file info {list_file_data}')
    for idx, file in enumerate(data):
        filename, file_extension = os.path.splitext(file.filename)
        file_extension = file_extension.replace('.', '')
        logger.info(f"Filename, extension: {filename}, {file_extension}")
                
        if file_extension == '':
            result["message"] = "No file uploaded"  
        ## process txt
        elif file_extension == 'txt':
            # Process uploaded txt file
            text = await file.read()
            text = text.decode('utf-8')
            # Example: Add the text to story data
            data_id = time_story + '_' + filename
            data_id = hash_str(data_id)
            text_data = {
                "data_id": str(data_id), 
                "type": "chat", 
                "processed_text": text,
                "date": list_file_data[str(idx)]["date"]
            }
            story_data["data"].append(text_data)
        ## process eml 
        elif file_extension == 'eml':
            # Process uploaded eml file
            eml_content = await file.read()
            eml_content = eml_content.decode('utf-8')
            
            data_id = time_story + '_' + filename
            data_id = hash_str(data_id)
            eml_file_path = os.path.join(CVT_DIR, f"{data_id}.eml") 
            with open(eml_file_path, 'w') as eml_file:
                eml_file.write(eml_content)
                    
            text_data = {
                "data_id": str(data_id), 
                "type": "email", 
                "processed_text": eml_content,
                "date": list_file_data[str(idx)]["date"]
            }
            story_data["data"].append(text_data)
        ## process audio
        elif file_extension in "wav mp3 mp4 m4a webm m4v":
            audio_bytes = await file.read()
            data_id = time_story + '_' + filename
            data_id = hash_str(data_id)
            tmp_filepath, audio_filepath = handle2wav(TMP_DIR, CVT_DIR, file.filename, data_id, audio_bytes)
            logger.info('Saving 16000_sr audio file')
            split_data, message = call_split_api(split_url, audio_filepath)
            result["message"] = message
            
            if split_data is None:
                break
            
            waveform, sr = soundfile.read(audio_filepath)
            send_records = list()
            with ThreadPoolExecutor(max_workers=5) as executor:  # Set the desired number of workers
                threads = []
                for order, seg in enumerate(split_data['segments']):
                    start = int(float(seg['start']) * sr)
                    end = int(float(seg['end']) * sr)
                    # if end - start < min_samples:
                    #     continue

                    # create segment_id
                    segment_id = f"{data_id}_{order}"
                    segment_id = hash_str(segment_id)
                    segment_audio = waveform[start:end]
                    segment_path = f"{TMP_DIR}/{segment_id}.wav"
                    
                    send_records.append((segment_path, seg['start'], seg['end']))
                    t = executor.submit(save_audio, segment_path, segment_audio, sr)
                    threads.append(t)
                
                for t in threads:
                    t.result()
            
            text_data = {
                "data_id": str(data_id), 
                "type": "audio", 
                "processed_text": [],
                "segments": [],
                "date": list_file_data[str(idx)]["date"]
            }
            
            for segment_path, start, end in send_records:
                asr_url = random.choice(api_urls)
                is_success, response_text = call_with_retry(asr_url, segment_path, start, end)
                # logger.info(f"Calling {response_text}")
                text_data["segments"].extend(response_text)
                final_text = list()
                for infor_dict in response_text:
                    text = infor_dict.get('text', '')
                    text = text.lower().capitalize()
                    final_text.append(text)
                text_data["processed_text"].extend(final_text)
            
            text = '. '.join(text_data["processed_text"])
            text_data["processed_text"] = text
            story_data["data"].append(text_data)
            remove_file(tmp_filepath)
        else:
            result["success"] = False
            result["message"] = f"file format: {file_sextension} is not support"
            break
        
        result["success"] = True
        result["message"] = f"Received data for story '{story_name}' with {len(story_data['data'])} data items. "
        result["data"] = story_data
    
    if result["success"]:
        story_id = story_data["story_id"]
        json_file_path = os.path.join(STORY_DIR, f"{story_id}.json")
        with open(json_file_path, 'w') as json_file:
            json.dump(story_data, json_file, indent=2)
    
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="172.26.33.174", port=8005)
