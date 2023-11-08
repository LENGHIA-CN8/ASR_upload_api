import os
from datetime import datetime
import numpy as np
# import fleep
import subprocess
import hashlib
import json
from pydub import AudioSegment
import requests
import logging
import soundfile as sf
import yaml
import time
 
logger = logging.getLogger(__name__)

def remove_file(tmp_filepath):
    try:
        os.remove(tmp_filepath)  # Delete the file
        logger.info(f"File '{tmp_filepath}' removed successfully.")
    except FileNotFoundError:
        logger.info(f"File '{tmp_filepath}' not found.")
        
def load_config(yaml_url):
    with open(yaml_url) as f:
        config = yaml.safe_load(f)
    return config 
    
def call_with_retry(api_url, segment_path, start, end, max_retries=3):
    retries = 0
    is_success = False
    response_text = None

    while retries < max_retries and not is_success:
        is_success, response_text = call_asr_api(api_url, segment_path, start, end)
        
        if not is_success:
            retries += 1
            if retries < max_retries:
                # Wait for some time before the next retry (for example, 1 seconds)
                time.sleep(1)
    logger.info(is_success)
    return is_success, response_text

def call_asr_api(
        api_url: str,
        tmp_segment_audio_path: str,
        start, end
    ) -> str:
    
    is_sucess = False
    final_text = ''
    try:
        audio_binary = load_audio_byte(tmp_segment_audio_path)
        os.system("rm {}".format(tmp_segment_audio_path))
        
        # upload
        headers = {
            "Content-Type":"application/binary",
        }
        
        response = requests.post(api_url, data= audio_binary, headers= headers)
        output = response.json()

        if output['status'] == 'success':
            is_sucess = True
            transcript = output.get('result', dict()).get('text', list())
            final_text = list()
            for infor_dict in transcript:
                # text = infor_dict.get('text', '')
                # text = text.lower().capitalize()
                infor_dict["start"] = start
                infor_dict["end"] = end
                final_text.append(infor_dict)
            # final_text = '. '.join(final_text)
    except Exception as e:
        if "404" in str(e):
            final_text = "error 404"
    return is_sucess, final_text

def save_audio(filepath: str, audio_sample: np.array, sr=16000):
    sf.write(filepath, audio_sample, sr)

def call_split_api(split_url, audio_filepath):
    file = {'file' : load_audio_byte(audio_filepath)}
    response = requests.post(split_url, files=file)

    if response.status_code == 200:
        logger.info("File splitted successfully!")
        content = json.loads(response.content)
        logger.info(content)
        if content['success']:
            split_data = content['data']
            return split_data, content['message']
        else:
            result["message"] = content['message']
            return None, content['message']
    else:
        logger.info("Failed to upload audio file to split. Status code:", response.status_code)
        
    return None

def get_current_datatime(is_filename: bool = True) -> str:
    if is_filename:
        datatime_str = str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S.%f')[:-6])
    else:
        datatime_str = str(datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')[:-3])
    return datatime_str

def format2wav(filepath: str, out_filepath: str = None)-> str:
    if out_filepath is None:
        out_filepath = filepath + ".wav"
    cmd = f"ffmpeg -y -hide_banner -loglevel error -i {filepath}  -acodec pcm_s16le -ac 1 -ar 16000 {out_filepath}"
    try:
        subprocess.call(cmd.split())
    except Exception as e:  # noqa: E722
        logger.error(f"run ffmpeg exception: {str(e)}")
        out_filepath = None
    return out_filepath

def handle2wav(TMP_DIR, CVT_DIR, filename_raw, filename, audio_bytes: bytes) -> str:
    tmp_filepath = os.path.join(TMP_DIR, filename_raw)
    with open(tmp_filepath, "wb") as f_tmp:
            f_tmp.write(audio_bytes)
    audio_filepath = os.path.join(CVT_DIR, f"{filename}" + ".wav")
    sound = AudioSegment.from_file(tmp_filepath)
    sound.export(audio_filepath, format='wav', bitrate='256k', parameters=["-ac", "1", "-ar", "16000"])
    return tmp_filepath, audio_filepath
    
# def get_extension_from_byte(audio_bytes: bytes)-> str:
#     info = fleep.get(audio_bytes[:128])
#     ext = info.extension
#     if len(ext):
#         return ext[0]
#     else:
#         return None

def load_audio_byte(path: str) -> bytes:
    with open(path, "rb") as fp:
        data = fp.read()
    return data

def hash_str(string: str) -> int:
    # hashlib.sha256(string.encode('utf-8')) computes the SHA-256 hash of the input string.
    # string.encode('utf-8') encodes the string into a sequence of bytes using UTF-8 encoding.

    # .hexdigest() returns the digest of the SHA-256 hash as a hexadecimal string.

    # int(..., 16) converts the hexadecimal string into an integer. The base is 16 (hexadecimal).

    # % 10**9 performs modulo operation to ensure the result is within the range of 0 to 10^9.
    # This constrains the result to a maximum of 1,000,000,000 (10 to the power of 9).
    return int(hashlib.sha256(string.encode('utf-8')).hexdigest(), 16) % 10**9

