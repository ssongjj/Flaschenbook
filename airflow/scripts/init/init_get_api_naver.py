
import os
from dotenv import load_dotenv
import requests
import time
from utils.file_operations import upload_files_to_s3
from utils.api_operations import get_isbn_list
from utils.api_operations import save_csv_file
from utils.api_operations import save_json_file
from utils.api_operations import get_headers

load_dotenv()
BUCKET_NAME = os.environ.get("BUCKET_NAME")
site = 'naver'
source_dir = 'data/'
TODAY = '2023-08-16'


def fetch_naver_api_data(isbn_list):
    books = {'items': []}
    valid_isbn_list = []
    valid_isbn_cnt = 0
    key_num = 1
    file_num = 1
    url = "https://openapi.naver.com/v1/search/book.json"
    headers = get_headers(site, key_num)
    csv_file_path = ""
    json_file_path = ""

    for i, isbn in enumerate(isbn_list):
        params = {
            "query": isbn,
            "start": '1'
        }

        time.sleep(0.1)

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error while fetching data: {e}")
            if e.response.status_code == 429:
                print('Too Many Requests for url')
                isbn_list.append(isbn)

                # key 변경
                key_num += 1
                headers = get_headers(site, key_num)
                print(f'API Key {key_num}으로 변경')

                continue

        book_info = response.json()
        if book_info.get("total") == 0:
            print(f'naver {i} 번째 {isbn} book info 없음!')
            continue

        books['items'].append(book_info)
        valid_isbn_list.append(isbn)
        valid_isbn_cnt += 1

        print(f'naver {i} 번째 {isbn} book info 수집, 현재 {valid_isbn_cnt}개')

        # 5000개 간격으로 나눠서 파일 저장
        if valid_isbn_cnt % 5000 == 0:
            csv_file_path = f"{source_dir}isbn/raw+isbn+{TODAY}+init+{file_num}.csv"
            json_file_path = f"{source_dir}{site}/raw+book_info+{site}+{TODAY}+init+books_{file_num}.json"
            save_csv_file(csv_file_path, valid_isbn_list)
            save_json_file(json_file_path, books)
            file_num += 1
            valid_isbn_list = []
            books = {'items': []}

    # 나머지 저장
    csv_file_path = f"{source_dir}isbn/raw+isbn+{TODAY}+init+{file_num}.csv"
    json_file_path = f"{source_dir}{site}/raw+book_info+{site}+{TODAY}+init+books_{file_num}.json"
    save_csv_file(csv_file_path, valid_isbn_list)
    save_json_file(json_file_path, books)


def main():
    isbn_object_key = f'raw/isbn/{TODAY}/raw.csv'
    isbn_list = get_isbn_list(BUCKET_NAME, isbn_object_key)
    fetch_naver_api_data(isbn_list)
    upload_files_to_s3(BUCKET_NAME, f'{source_dir}isbn/')
    upload_files_to_s3(BUCKET_NAME, f'{source_dir}{site}/')


if __name__ == "__main__":
    main()
