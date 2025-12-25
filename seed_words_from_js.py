from google.cloud import bigquery
import hashlib
import re, json
from pathlib import Path

PROJECT_ID = "acidrain-event"
DATASET = "acidrain.word_catalog"

client = bigquery.Client(project=PROJECT_ID)

def make_id(lang, word):
  return hashlib.sha1(f"{lang}:{word}".encode()).hexdigest()

def load_words_from_js(js_path: str, var_name: str) -> list[str]:
  """
    words.js / words-ko.js 안의 const WORD_LIST(_KO) = [ ... ]; 배열을 파싱해서 list[str]로 반환
  """
  with open(js_path, "r", encoding="utf-8") as f:
      text = f.read()

  # 1) block comment 제거
  text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

  # 2) line comment 제거
  text = re.sub(r"//.*$", "", text, flags=re.MULTILINE)

  # 3) 문자열 리터럴만 추출 ('...' 또는 "...")
  words = re.findall(r"""['"]([^'"]+)['"]""", text)

  # 4) 공백 제거 + 중복 제거 (순서 유지)
  seen = set()
  result = []
  for w in words:
      w = w.strip()
      if w and w not in seen:
          seen.add(w)
          result.append(w)

  return result

def insert(words, lang):
  rows = [{
    "id": make_id(lang, word),
    "lang": lang,
    "word": word,
    "length": len(word)
  } for word in words]

  errors = client.insert_rows_json(
        f"{PROJECT_ID}.{DATASET}",
        rows
    )
  if errors:
      raise RuntimeError(errors)

  print(f"Inserted {len(rows)} ({lang})")

if __name__ == "__main__":
  en_words = load_words_from_js("./app/static/words.js", "WORD_LIST")
  ko_words = load_words_from_js("app/static/words-ko.js", "WORD_LIST_KO")

  insert(en_words, "en")
  insert(ko_words, "ko")
