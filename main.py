# main.py
from typing import Optional
from fastapi import Query
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pymongo import MongoClient
from datetime import datetime
import time
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from celery import Celery
from bson import ObjectId
import traceback
# main.py (FastAPI backend)
from fastapi.middleware.cors import CORSMiddleware

# ========== Celery CONFIG ==========
celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

# ========== FastAPI APP ==========
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ========== MongoDB Connection ==========
client = MongoClient("mongodb://localhost:27017/")
db = client["walmart"]
task_col = db["tasks"]
product_col = db["product"]
client_col = db["clients"]

# ========== Request Models ==========
class TaskRequest(BaseModel):
    client_name: str
    category: str
    url: str

class ClientRequest(BaseModel):
    client_name: str
    client_email: str

# ------------ API ENDPOINTS ------------

@app.post("/register-client/")
def register_client(client_data: ClientRequest):
    existing = client_col.find_one({"client_email": client_data.client_email})
    if existing:
        return JSONResponse(status_code=400, content={"message": "Client already registered."})
    client_doc = {
        "client_name": client_data.client_name,
        "client_email": client_data.client_email,
        "registered_at": datetime.now()
    }
    result = client_col.insert_one(client_doc)
    return {
        "message": "✅ Client registered successfully",
        "client_id": str(result.inserted_id)
    }

@app.post("/submit-task/")
def submit_task(task: TaskRequest):
    task_data = {
        "client_name": task.client_name,
        "category": task.category,
        "url": task.url,
        "status": "pending",
        "created_at": datetime.now()
    }
    result = task_col.insert_one(task_data)
    task_id = str(result.inserted_id)
    # Start scraping in the background with Celery
    scrape_and_store.delay(task.url, task.category, task.client_name, task_id)
    return {"message": "✅ Task created and scraping started in background", "task_id": task_id}

@app.get("/tasks/")
def list_tasks(
    client_name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    category: Optional[str] = Query(None)
):
    query = {}
    if client_name:
        query["client_name"] = client_name
    if status:
        query["status"] = status
    if category:
        query["category"] = category

    tasks = [
        {
            "task_id": str(t["_id"]),
            "client_name": t.get("client_name"),
            "category": t.get("category"),
            "url": t.get("url"),
            "status": t.get("status"),
            "created_at": t.get("created_at"),
            "error": t.get("error", None)
        }
        for t in task_col.find(query).sort("created_at", -1)
    ]
    return tasks

@app.get("/products/")
def list_products(
    client_name: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    min_price: Optional[float] = Query(None),
    max_price: Optional[float] = Query(None)
):
    query = {}
    if client_name:
        query["client_name"] = client_name
    if category:
        query["category"] = category
    if min_price is not None or max_price is not None:
        price_query = {}
        if min_price is not None:
            price_query["$gte"] = min_price
        if max_price is not None:
            price_query["$lte"] = max_price
        query["price"] = price_query

    products = [
        {
            key: (str(val) if key == "_id" else val)
            for key, val in p.items()
        }
        for p in product_col.find(query).sort("scraped_at", -1)
    ]
    return products

# ========== Celery Scraping Task ==========

@celery_app.task()
def scrape_and_store(product_url, category, client_name, task_id):
    try:
        print(f"[Task {task_id}] Starting scrape: {product_url}")

        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--lang=en-US,en")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )

        # For debugging, headless=False; change to True for headless mode in production
        driver = uc.Chrome(options=options, headless=False)
        driver.get(product_url)
        time.sleep(3)

        # CAPTCHA detection handling
        if "captcha" in driver.page_source.lower():
            print("CAPTCHA detected! Solve it manually.")
            input("Press ENTER after solving CAPTCHA...")

        def scroll_to_element(selector, timeout=10):
            try:
                elem = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior:'smooth', block:'center'});", elem
                )
                time.sleep(2)
                return elem
            except Exception:
                return None

        def safe_find_text_by_css(selectors, timeout=5):
            if isinstance(selectors, str):
                selectors = [selectors]
            for selector in selectors:
                try:
                    elem = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    text = elem.text.strip()
                    if text:
                        return text
                except Exception:
                    continue
            return "N/A"

        def extract_colors():
            color_names = set()
            selectors = [
                'ul[data-tl-id*="color"] button span',
                'ul[data-tl-id*="color"] label span',
                'div[data-automation-id="color-picker"] label span',
                '[aria-label*="Color"]',
                'button[aria-checked="true"] span',
                '[itemprop="color"]',
            ]
            for sel in selectors:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                for elem in elems:
                    name = elem.text.strip()
                    if name:
                        color_names.add(name)
                if color_names:
                    break
            if not color_names:
                imgs = driver.find_elements(By.CSS_SELECTOR, 'img[alt*="color"], img[alt*="Color"]')
                for img in imgs:
                    alt = img.get_attribute('alt')
                    if alt:
                        color_names.add(alt.strip())
            return list(color_names) or ["N/A"]

        def extract_sizes():
            size_names = set()
            selectors = [
                'ul[data-tl-id*="size"] button span',
                'ul[data-tl-id*="size"] label span',
                'div[data-automation-id="size-picker"] label',
                '[aria-label*="Size"]',
                'button[aria-checked="true"] span',
            ]
            for sel in selectors:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                for elem in elems:
                    txt = elem.text.strip()
                    if txt and txt.lower() != "select":
                        size_names.add(txt)
                if size_names:
                    break
            return list(size_names) or ["N/A"]

        def extract_price():
            selectors = [
                'span[itemprop="price"]',
                'span[data-automation-id="product-price"]',
                'span.price-characteristic',
                'div[data-testid="price"] span'
            ]
            for sel in selectors:
                try:
                    elem = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                    text = elem.text.strip()
                    if text:
                        try:
                            return float(text.replace('$', '').replace(',', '').strip())
                        except Exception:
                            return text
                except Exception:
                    continue
            return 0.0

        product = {
            "client_name": client_name,
            "category": category,
            "task_id": task_id,
            "title": safe_find_text_by_css(['h1.prod-ProductTitle', 'h1[itemprop="name"]'], timeout=15),
            "price": extract_price(),
            "images": [],
            "about_this_item": [],
            "colors": extract_colors(),
            "sizes": extract_sizes(),
            "product_url": product_url,
            "related_links": [],
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # Extract images
        try:
            scroll_to_element('div[data-testid="media-gallery"]')
            image_urls = []
            thumbs = driver.find_elements(By.CSS_SELECTOR, 'img[data-testid="media-gallery-thumbnail-image"]')
            for thumb in thumbs[:5]:
                try:
                    thumb.click()
                    time.sleep(1)
                    main_img = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="media-gallery"] img')
                    src = main_img.get_attribute("src")
                    if src and src not in image_urls:
                        image_urls.append(src)
                except Exception:
                    continue
            product["images"] = image_urls
        except Exception:
            product["images"] = []

        # Extract "about this item"
        try:
            scroll_to_element('div.dangerous-html.mb3', timeout=15)
            about_elem = WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.dangerous-html.mb3'))
            )
            paragraphs = about_elem.find_elements(By.TAG_NAME, 'p')
            product["about_this_item"] = [p.text.strip() for p in paragraphs if p.text.strip()]
        except Exception:
            product["about_this_item"] = []

        # Extract related links
        try:
            links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/ip/"]')
            product["related_links"] = list({a.get_attribute("href").split("?")[0] for a in links if a.get_attribute("href")})
        except Exception:
            product["related_links"] = []

        product_col.insert_one(product)
        print(f"Saved product to MongoDB for task {task_id}")

        # Append to CSV file - only header if file doesn't exist
        try:
            append_header = not pd.io.common.file_exists("walmart_data.csv")
            df = pd.DataFrame([product])
            df.to_csv("walmart_data.csv", mode='a', header=append_header, index=False)
            print(f"Saved product to CSV")
        except Exception as e:
            print("CSV Error:", e)

        driver.quit()

        task_col.update_one({"_id": ObjectId(task_id)}, {"$set": {"status": "completed"}})
        print(f"[Task {task_id}] Completed successfully.")

    except Exception as e:
        err_trace = traceback.format_exc()
        print(f"Scraping error for task {task_id}:\n{err_trace}")
        task_col.update_one({"_id": ObjectId(task_id)}, {"$set": {"status": "failed", "error": str(e)}})

# Run FastAPI app if executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
