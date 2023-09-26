import csv
import datetime
import json
import time
import multiprocessing
import threading
import tkinter as tk
from multiprocessing import freeze_support, Lock
from multiprocessing.pool import ThreadPool
from tkinter import filedialog as fd
from tkinter import ttk

import requests
import sv_ttk
from bs4 import BeautifulSoup


def scrape_links(i, new_businesses, last_id, total_count, keywords, lock):
    global info_text
    url = f"https://nextdoor.com/pages/{i}/"
    headers = {
        'authority': 'nextdoor.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'no-cache',
        'cookie': 'WE=d501b9e3-ca84-47d1-b832-0e498d61955b230922; csrftoken=VkRzxQJr8XZVVadoDWx2raJMyIqswFSSCmL5Jcdisxn3L7fCEq0c7wMs6PyQlCpH; ndbr_at=PuYU0gtF4PWL9DzzqxNIn4tXBLTcwf6qJ4ZFIx5Blvo; ndbr_idt=eyJhbGciOiJSUzI1NiIsImtpZCI6ImFjM2VmYmJlLTRjNjQtNGNiZi05NjVkLWJiZTljNjIyZjE3YSIsInR5cCI6IkpXVCJ9.eyJhdF9oYXNoIjoibDdXaFBpcytKWVZIelNxL0dlTG56QSIsImF1ZCI6WyJuZXh0ZG9vci1kamFuZ28iLCJuZXh0ZG9vciJdLCJleHAiOjE2OTU0OTEwNTQsImlhdCI6MTY5NTQwNDY1NCwiaXNzIjoiaHR0cHM6Ly9hdXRoLm5leHRkb29yLmNvbSIsIm5kX3ByYyI6W3siY291bnRyeSI6IlVTIiwicGlkIjoiNzY3MDIwNzgiLCJ1cmwiOiJodHRwczovL3VzZXIubmV4dGRvb3IuY29tL3YxL3Byb2ZpbGUvNzY3MDIwNzgifSx7ImNvdW50cnkiOiJVUyIsInBpZCI6Ijc4MzQ3MDkwIiwidXJsIjoiaHR0cHM6Ly91c2VyLm5leHRkb29yLmNvbS92MS9wcm9maWxlLzc4MzQ3MDkwIn0seyJjb3VudHJ5IjoiVVMiLCJwaWQiOiI0ZTM1YWI1Zi1mOTJmLTQxYzgtYTNlZi1jNzI4MzFjMWY4ZjIiLCJ1cmwiOiJodHRwczovL2Fkcy5uZXh0ZG9vci5jb20vc2VsZi1zZXJ2ZS9wcm9maWxlL3YxLzRlMzVhYjVmLWY5MmYtNDFjOC1hM2VmLWM3MjgzMWMxZjhmMiJ9XSwic3ViIjoiY2M4ZWZhYTItYTVmYS00MjQ5LTlhMGEtZjcxNTk4NWQ4YzgxIn0.JFEZyN31h3_-hYvikbxf1IjGjATwYFFYurDQVAp4IdzWBRnfGK9saVcNVOzaxgDWlfW891vbcXjHPOEhroKm_K9VGD9zme7wuyErUznmurrNJIiZ53kaSTl_lgG31-5wYZckhW-Eel7Y9bu5zRGW-7i0Dm2DoBbt1MKLMXBJJTm0zdRon_8G-K-yDqp4g9nBVWOWsmaal3AgZ8piqNMDRRKlA7FTGucJK39pDccgC2qMjxxkGXl9t1xz7wbvI1G8a9ieP8an0_PZqKU0K2bCOuvkPvwdafMnlGuFWHHAwFs8C7mv96tVTQuzfZ8LWNzJ-TwUJkS1wNye0M52QVKKWQ; WE3P=d501b9e3-ca84-47d1-b832-0e498d61955b230922; spark-nav-onboarding-completion=Sat%20Sep%2023%202023%2010:45:56%20GMT+0100%20(British%20Summer%20Time); role-invitation-decline-notification=false; role-resignation-notification=false; seen-tour-in-past-day=true; _gcl_au=1.1.51882913.1695462369; session_id=f73eb25a-c440-4f45-b637-9f13de19582b; _gid=GA1.2.1201469620.1695462381; FPID=FPID2.2.QdRiXlblMByDp0HQdyNy5BYaFDjOHSsk3U6wX9CC5rQ%3D.1695462378; __gads=ID=de1180d68a3b4cc7:T=1695404705:RT=1695404705:S=ALNI_MZ0V5DPTpSWdT6QxNk8qnfV7iR3qA; __gpi=UID=00000cad21ef4c4d:T=1695404705:RT=1695404705:S=ALNI_MYlnd-7qGciPxBBYMJmJsvJTR0LaA; _pin_unauth=dWlkPU1qTmlaRFJpTWpBdE9EVTJOUzAwTjJGa0xXRTVaRFl0WWpRd05XSXlZVGczTXpKaQ; _uetsid=0ddef51059f611ee98c1a1ad6b36b5f8; _uetvid=0ddf0fa059f611ee95ecdbb85e214957; _fbp=fb.1.1695462436685.1544600420; __hstc=1713326.e8d5e2386483ade1ef7a4931169513d6.1695462441423.1695462441423.1695462441423.1; hubspotutk=e8d5e2386483ade1ef7a4931169513d6; __hssrc=1; _ga=GA1.1.1355627909.1695462378; FPLC=ZaG6eo7ynRfwtM8nuugxK85SkU%2BeC7lgs0tFxtMk0aV18%2BYGXKbrWR8TrGi4zoSX%2FRgXgVmJAwFRVnmsxi6kJ%2FPTsw5X%2FXAsj%2BkCUcvF3slyX%2FeJwcFOJ5L842kAow%3D%3D; WERC=5b84595e-d140-4714-91fd-52be7b68e97b2309231695482117; OptanonConsent=isGpcEnabled=1&datestamp=Sun+Sep+24+2023+08%3A16%3A37+GMT%2B0100+(British+Summer+Time)&version=202303.2.0&browserGpcFlag=1&isIABGlobal=false&hosts=&consentId=d2bafa91-ed9e-4827-b105-f75027792e2b&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A0%2CC0004%3A0%2CC0002%3A0%2CC0007%3A0&AwaitingReconsent=false; _ga_L2ES4MTTT0=GS1.1.1695534808.5.1.1695539799.57.0.0; flaskTrackReferrer=862F33A7-112C-438B-A239-9412BD129579; _dd_s=logs=1&id=2a7ceb98-e57b-42be-b278-2fa7afca4066&created=1695534805889&expire=1695541392202&rum=0',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if(response.history):
            response=requests.get(response.url,headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        with lock:
            try:
                link = soup.select('link[rel="canonical"]')[0].get('href')
            
                if link == None:
                    return
                else:
                    business = {}
                    details = json.loads(soup.select('script[type="application/ld+json"]')[0].text)
                   
                    if details is None:
                        return
                    else:
                    
                        business["Name"] = details["name"]
                        print(details)
                        business["Categories"] = str(details['business_topics'])

                        if "telephone" in details:
                            business["Phone Number"] = str(details["telephone"])

                        global keywords_filepath
                        if keywords_filepath != '' and len(keywords) > 0:
                            contains_keyword = False

                            for keyword in keywords:
                                if keyword.lower() in business["Categories"].lower():
                                    contains_keyword = True

                            if contains_keyword:
                                business["Link"] = link
                                business["ID"] = i
                                new_businesses.append(business)

                                total_count.value += 1
                                info_text.config(text=f"Found: {int(total_count.value)}")

                        else:
                            business["Link"] = link
                            business["ID"] = i
                            new_businesses.append(business)

                            total_count.value += 1
                            info_text.config(text=f"Found: {int(total_count.value)}")

                        if i > int(last_id):
                            last_id = i

                        return False
        
            except Exception as e:
                # print(f"An error occurred: {e}")
                if i > int(last_id) + 1000:
                    return True
    except requests.exceptions.RequestException as e:
         print(f"An error occurred: {e}")

def main():
    global startbot
    global info_text

    startbot.config(state="disabled", text="Started...")

    with multiprocessing.Manager() as manager:
        keywords = []
        if keywords_filepath != '':
            with open(keywords_filepath, 'r') as f:
                for line in f:
                    cleaned_line = line.strip()

                    if cleaned_line:
                        keywords.append(cleaned_line)

        new_businesses = manager.list()
        last_id = manager.Value('i', 0)
        total_count = manager.Value('i', 0)
        lock = manager.Lock()

        try:
            with open("last_id.txt", 'r') as f:  
                last_id = int(f.read())
                f.close()
        except FileNotFoundError:
            info_text.config(text="Last ID txt file cannot be found!")
            startbot.config(state="enabled")
            return

        info_text.config(text=f"Found: 0")

        loop_start = int(last_id) + 1
        loop_end = loop_start + 2000
        while True:
            with ThreadPool(100) as p:
                
                if any(p.starmap(scrape_links, [(i, new_businesses, last_id, total_count, keywords, lock) for i in
                                                range(loop_start, loop_end)])):
                    break
                else:
                    loop_start = loop_start + 2000
                    loop_end = loop_start + 2000

        current_datetime = datetime.datetime.now()
        csv_file = f"Nextdoor Scrape - {current_datetime.strftime('%d-%m-%Y %H_%M_%S')}.csv"

        with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
            fieldnames = ["Name", "Categories", "Phone Number", "Link", "ID"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            sorted_data = sorted(new_businesses, key=lambda x: x["ID"])
            for business in sorted_data:
                writer.writerow(business)
        if len(sorted_data) > 0:
            print(sorted_data[len(sorted_data)-1])
            last_id = int(sorted_data[len(sorted_data)-1]["ID"]) + 1
        with open("last_id.txt", 'w') as f:
            f.write(str(last_id))
            f.close()

    startbot.config(state="enabled")
    info_text.config(text="Completed!")


if __name__ == '__main__':
    freeze_support()

    app = tk.Tk()
    app.title(f'Nextdoor Scraper')
    app.geometry('400x400')
    app.minsize(400, 400)
    app.maxsize(400, 400)

    ttk.Frame(app, height=30).pack()
    title = tk.Label(app, text='Nextdoor Scraper', font=("Calibri", 24, "bold"))
    title.pack(pady=20)


    def select_file():
        global keywords_filepath

        file_path = fd.askopenfilename()
        file_path_short = file_path[file_path.rindex('/') + 1:]

        keywords_element.config(text=file_path_short)
        keywords_filepath = file_path


    keywords_filepath = ''

    keywords_info = ttk.Labelframe(app, text='Keywords (Optional)')
    keywords_info.pack(padx=60, pady=20)
    keywords_element = ttk.Button(keywords_info, text='Select file', width=40,
                                  command=lambda: select_file())
    keywords_element.pack(padx=10, pady=10, fill=tk.X)

    startbot = ttk.Button(app, text='Start Bot', style='Accent.TButton', width=15,
                          command=lambda: threading.Thread(target=main).start())
    startbot.pack(pady=10)

    info_text = ttk.Label(app, text='', justify=tk.CENTER)
    info_text.pack(pady=5)

    sv_ttk.set_theme('dark')
    app.mainloop()
