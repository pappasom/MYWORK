##################################################
### Script for download MIMIC Data in parallel ###
################################################## 

import os
import requests
import getpass
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


# DEFAULT CONFIGS
LOGIN_URL = "https://physionet.org/login/"
BASE_URL = "https://physionet.org/files/mimiciii/1.4/"
DOWNLOAD_DIR = "mimiciii_data"
MAX_WORKERS = 6  # Number of parallel downloads. More then 6 will be randomly closing connections


def get_all_file_urls(session, url):
    """Recursively scans a URL to find all downloadable file links."""
    links = []
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('?') or href == '../':
                continue
            full_url = urljoin(url, href)
            if href.endswith('/'):
                tqdm.write(f"[INFO] Scanning directory: {full_url}")
                links.extend(get_all_file_urls(session, full_url))
            else:
                links.append(full_url)
    except Exception as e:
        tqdm.write(f"[ERROR] Could not access {url} due to '{e}'")
    return links


def download_file_worker(session, url, download_dir, lock, positions):
    """Function to download a single file with progress bar."""
    my_pos = -1
    try:
        with lock:
            my_pos = positions.pop(0)

        relative_path = url.replace(BASE_URL, '')
        local_path = os.path.join(download_dir, relative_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        head_resp = session.head(url, timeout=10)
        head_resp.raise_for_status()
        server_mtime_str = head_resp.headers.get('Last-Modified')
        server_size = int(head_resp.headers.get('Content-Length', 0))

        headers = {}
        mode = 'wb'
        initial_progress = 0

        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            if local_size == server_size:
                return (url, "Skipped (already done)")
            elif local_size < server_size:
                headers = {'Range': f'bytes={local_size}-'}
                mode = 'ab'
                initial_progress = local_size

        with session.get(url, stream=True, headers=headers) as r:
            r.raise_for_status()
            with tqdm(
                total=server_size,
                initial=initial_progress,
                desc=os.path.basename(local_path).ljust(25),
                unit='B', unit_scale=True, unit_divisor=1024,
                position=my_pos,
                leave=False
            ) as pbar:
                with open(local_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        pbar.update(len(chunk))
        return (url, "Success")
    except Exception as e:
        return (url, f"Failed: {e}")
    finally:
        if my_pos != -1:
            with lock:
                positions.append(my_pos)
                positions.sort()

def main():
    """Main function to log in and then orchestrate the download."""
    with requests.Session() as session:
        print(f"[START] Authenticating on {LOGIN_URL}.")
        username = input("Enter your PhysioNet username: ")
        password = getpass.getpass("Enter your PhysioNet password: ")
        
        try:
            login_page_resp = session.get(LOGIN_URL)
            login_page_resp.raise_for_status()
            soup = BeautifulSoup(login_page_resp.text, 'html.parser')
            csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
            login_data = {'csrfmiddlewaretoken': csrf_token, 'username': username, 'password': password, 'next': BASE_URL}
            login_resp = session.post(LOGIN_URL, data=login_data, headers={'Referer': LOGIN_URL})
            login_resp.raise_for_status()
        except Exception as e:
            print(f"[ERROR] An error occurred during login: '{e}'")
            return
        
        print("")
        print("[START] Scanning for all files to be downloaded...")
        all_urls = get_all_file_urls(session, BASE_URL)
        if not all_urls:
            print("[WARN] Files not found. Please check your default URls")
            return
        print("")
        print(f"[INFO] Found {len(all_urls)} files. Downloading...")
        
        position_lock = threading.Lock()
        available_positions = list(range(1, MAX_WORKERS + 1))
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            main_pbar = tqdm(total=len(all_urls), desc="Overall Progress", position=0, unit="file")
            future_to_url = {executor.submit(download_file_worker,
                                             session,
                                             url,
                                             DOWNLOAD_DIR,
                                             position_lock,
                                             available_positions): url for url in all_urls}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    _, message = future.result()
                    if "Success" not in message:
                        tqdm.write(f"{os.path.basename(url)}: {message}")
                except Exception as exc:
                    tqdm.write(f"{os.path.basename(url)} generated an exception: {exc}")
                main_pbar.update(1)
            main_pbar.close()
    
    print("")
    print(f"[DONE] All files downloaded and saved into '{DOWNLOAD_DIR}' directory.")

if __name__ == "__main__":
    main()
