import os
import json
import requests
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse, urljoin
from kafka import KafkaProducer

# Kafka configuration
TOPIC = "theicon-topic"
KAFKA_BROKER = 'localhost:9092'

# Output paths
OUTPUT_JSON = os.path.join("output", f"{TOPIC}_products.json")
IMAGES_FOLDER = os.path.join("output", "images", TOPIC)
os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs("output", exist_ok=True)

def safe_filename(s, maxlen=60):
    """Create safe filename from string"""
    safe = "".join(c for c in (s or "") if c.isalnum() or c in (' ', '-', '_')).rstrip()
    return safe[:maxlen].replace(" ", "_") or "product"

# Hàm để lấy ID tiếp theo từ file JSON hiện có
def get_next_id():
    """Lấy ID tiếp theo dựa trên file JSON hiện có"""
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if existing_data and len(existing_data) > 0:
                    max_id = max(item['id'] for item in existing_data)
                    return max_id + 1
        except Exception as e:
            print(f" Lỗi đọc file JSON cũ: {e}")
    return 1  # Bắt đầu từ 1 nếu không có file

def download_all_product_images(img_urls, product_folder, product_id, product_title):
    """Download all images for a product and return list of filenames"""
    if not img_urls:
        return []
    
    filenames = []
    safe_title = safe_filename(product_title)[:40]
    product_folder_path = os.path.join(product_folder, f"{product_id:03d}_{safe_title}")
    os.makedirs(product_folder_path, exist_ok=True)
    
    for i, img_url in enumerate(img_urls):
        try:
            if not img_url or not img_url.startswith(("http://","https://")):
                continue
                
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            resp = requests.get(img_url, headers=headers, timeout=30, stream=True)  # Tăng timeout lên 30s
            resp.raise_for_status()
            
            # Get file extension
            ext = ".jpg"
            ct = resp.headers.get("content-type","")
            if "png" in ct:
                ext = ".png"
            elif "webp" in ct:
                ext = ".webp"
            elif "gif" in ct:
                ext = ".gif"
            else:
                p = urlparse(img_url).path
                if "." in p:
                    ext = os.path.splitext(p)[1] or ext
                    
            filename = f"image_{i+1:02d}{ext}"
            filepath = os.path.join(product_folder_path, filename)
            
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(1024*8):
                    if chunk:
                        f.write(chunk)
            
            filenames.append(filename)
            print(f"    Đã tải ảnh {i+1}/{len(img_urls)}: {filename}")
            
            # Thêm delay nhỏ giữa các lần tải ảnh
            time.sleep(0.5)
            
        except Exception as e:
            print(f"    [WARN] Lỗi tải ảnh {i+1}: {e}")
            continue
    
    return filenames

def extract_product_details(driver, product_link):
    """Extract detailed information from product page"""
    print(f"    Đang truy cập trang chi tiết: {product_link}")
    
    try:
        # Lưu URL trang danh sách hiện tại
        original_url = driver.current_url
        
        # Truy cập trang chi tiết sản phẩm
        driver.get(product_link)
        print("    Đang chờ trang chi tiết tải...")
        time.sleep(8)  # Tăng thời gian chờ lên 8 giây
        
        # Scroll để load thêm nội dung
        print("    Đang scroll để load thêm nội dung...")
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight * {} / 3);".format(i+1))
            time.sleep(2)
        
        details = {
            'description': '',
            'all_image_urls': [],
            'additional_info': {}
        }
        
        # Lấy mô tả chi tiết - THÊM NHIỀU SELECTOR HƠN
        description_selectors = [
            "[data-testid='product-description']",
            ".product-description",
            ".description",
            "[class*='description']",
            ".product-details",
            ".product-info__description",
            ".pdp-description",
            "[data-analytics='product-description']",
            ".item-details-description",
            "#productDescription",
            ".product-detail__description"
        ]
        
        for selector in description_selectors:
            try:
                desc_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in desc_elements:
                    text = elem.text.strip()
                    if text and len(text) > 10:  # Đảm bảo là mô tả thực sự
                        details['description'] = text
                        print(f"    Tìm thấy mô tả với selector: {selector}")
                        break
                if details['description']:
                    break
            except Exception as e:
                continue
        
        # Lấy tất cả ảnh sản phẩm - THÊM NHIỀU SELECTOR HƠN
        image_selectors = [
            "img[src*='images.theiconic.com.au']",
            "img[src*='theiconic.com.au']",
            ".product-gallery img",
            ".image-slider img",
            "[data-testid='product-image']",
            ".swiper-slide img",
            ".thumbnail img",
            "img[alt*='product']",
            ".pdp-image",
            ".product-image",
            ".gallery-image",
            "[class*='image'] img",
            ".zoom-img",
            ".product-detail__image img",
            ".pdp__image img"
        ]
        
        all_found_urls = []
        for selector in image_selectors:
            try:
                img_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"    Tìm thấy {len(img_elements)} phần tử với selector: {selector}")
                
                for img_elem in img_elements:
                    try:
                        img_url = (img_elem.get_attribute("src") or 
                                  img_elem.get_attribute("data-src") or 
                                  img_elem.get_attribute("data-lazy") or
                                  img_elem.get_attribute("data-original"))
                        
                        if img_url and img_url not in all_found_urls:
                            # Fix relative URLs
                            if img_url.startswith("//"):
                                img_url = "https:" + img_url
                            elif img_url.startswith("/"):
                                img_url = urljoin(driver.current_url, img_url)
                            
                            # Lọc ảnh nhỏ (thumbnail) và ảnh placeholder
                            skip_patterns = ['thumb', 'thumbnail', '50x', '100x', 'placeholder', 'default', 'icon', 'logo']
                            if not any(pattern in img_url.lower() for pattern in skip_patterns):
                                # Chỉ lấy URL có chứa từ khóa ảnh
                                if any(img_keyword in img_url.lower() for img_keyword in ['image', 'jpg', 'jpeg', 'png', 'webp', 'gif']):
                                    all_found_urls.append(img_url)
                                    print(f"      → Thêm ảnh: {img_url[:80]}...")
                    except Exception as e:
                        continue
                        
            except Exception as e:
                continue
        
        # Loại bỏ duplicate URLs và giới hạn số lượng
        details['all_image_urls'] = list(dict.fromkeys(all_found_urls))[:20]  # Tăng lên 20 ảnh
        
        # Lấy thông tin bổ sung (size, color, etc.)
        try:
            # Thử lấy thông tin size
            size_selectors = [
                "[data-testid*='size']",
                ".size-option",
                ".swatch-option",
                ".size-selector",
                ".product-size",
                "[class*='size']"
            ]
            for selector in size_selectors:
                size_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if size_elements:
                    sizes = [elem.text.strip() for elem in size_elements if elem.text.strip()]
                    if sizes:
                        details['additional_info']['available_sizes'] = sizes
                        break
        except:
            pass
            
        try:
            # Thử lấy thông tin color
            color_selectors = [
                "[data-testid*='color']",
                ".color-option", 
                ".colour-swatch",
                ".color-selector",
                ".product-color",
                "[class*='color']",
                "[class*='colour']"
            ]
            for selector in color_selectors:
                color_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if color_elements:
                    colors = [elem.text.strip() for elem in color_elements if elem.text.strip()]
                    if colors:
                        details['additional_info']['available_colors'] = colors
                        break
        except:
            pass
        
        print(f"    → Tìm thấy {len(details['all_image_urls'])} ảnh")
        print(f"    → Mô tả: {details['description'][:100]}..." if details['description'] else "    → Không có mô tả chi tiết")
        
        # Quay lại trang danh sách
        print("    Đang quay lại trang danh sách...")
        driver.get(original_url)
        time.sleep(5)  # Tăng thời gian chờ trang danh sách tải lại
        
        return details
        
    except Exception as e:
        print(f"    Lỗi khi lấy thông tin chi tiết: {e}")
        # Cố gắng quay lại trang danh sách nếu có lỗi
        try:
            driver.get(original_url)
            time.sleep(5)
        except:
            pass
        return {'description': '', 'all_image_urls': [], 'additional_info': {}}

def generate_random_rating_data():
    """Generate random but realistic rating and review data"""
    rating = round(random.uniform(3.5, 5.0), 1)
    comments = random.randint(10, 500)
    return rating, comments

# Setup driver với nhiều options hơn
driverpath = r"D:\chromedriver-win64\chromedriver.exe"
service = Service(driverpath)
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(service=service, options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
wait = WebDriverWait(driver, 25)  # Tăng timeout lên 25 giây

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Truy cập trang
url = "https://www.theiconic.com.au/beauty-grooming-all/?campaign=lp-gtm-w-gifts-oct25&page=1&sort=popularity"
print(" Đang tải trang...")
driver.get(url)
time.sleep(8)  # Tăng thời gian chờ trang chính

# Scroll để load sản phẩm - SCROLL NHIỀU HƠN
print(" Đang tải sản phẩm...")
for i in range(5):  # Tăng từ 3 lên 5 lần scroll
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight * {} / 5);".format(i+1))
    print(f" Scroll lần {i+1}/5")
    time.sleep(3)  # Tăng thời gian chờ giữa các lần scroll

time.sleep(4)

# Crawl dữ liệu
print(" Đang thu thập dữ liệu sản phẩm...")
products_data = []

# Lấy ID bắt đầu tiếp theo
next_id = get_next_id()
print(f" Bắt đầu đánh ID từ: {next_id}")

try:
    # Tìm sản phẩm và lấy link chi tiết
    product_links = []
    
    # Tìm tất cả các link sản phẩm - THÊM NHIỀU SELECTOR HƠN
    link_selectors = [
        "a[href*='/p/']",
        ".product-link",
        "[data-testid='product-link']",
        ".product-tile a",
        "#catalogProductsList > div a",
        "[class*='product-card'] a",
        ".product-item a",
        ".pdp-link"
    ]
    
    for selector in link_selectors:
        try:
            link_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            print(f"  Tìm thấy {len(link_elements)} phần tử với selector: {selector}")
            
            for elem in link_elements:
                try:
                    href = elem.get_attribute("href")
                    if href and "/p/" in href and href not in product_links:
                        product_links.append(href)
                        print(f"    → Thêm link: {href}")
                except:
                    continue
            if product_links:
                print(f"Đã tìm thấy {len(product_links)} link sản phẩm với selector: {selector}")
                break
        except Exception as e:
            print(f"  Lỗi với selector {selector}: {e}")
            continue
    
    print(f" Tổng cộng tìm thấy {len(product_links)} link sản phẩm")
    
    if not product_links:
        # Fallback: sử dụng cách cũ nếu không tìm thấy link
        product_containers = driver.find_elements(By.CSS_SELECTOR, "#catalogProductsList > div")
        print(f" Không tìm thấy link, sử dụng cách cũ với {len(product_containers)} sản phẩm")
        
        # Giới hạn số lượng sản phẩm 
        max_products = min(8, len(product_containers))  # Giảm số lượng để có thời gian lấy nhiều ảnh hơn
        
        for i in range(max_products):
            try:
                container = product_containers[i]
                current_id = next_id + i
                print(f"\n Đang xử lý sản phẩm {i+1}/{max_products} (ID: {current_id})")
                
                # Lấy thông tin cơ bản (giữ nguyên code cũ)
                brand = ""
                try:
                    brand_elem = container.find_element(By.CSS_SELECTOR, "span.brand")
                    brand = brand_elem.text.strip()
                except:
                    pass
                
                name = ""
                try:
                    name_elem = container.find_element(By.CSS_SELECTOR, "span.name")
                    name = name_elem.text.strip()
                    if brand and name.startswith(brand):
                        name = name.replace(brand, "").strip()
                        if name.startswith("-"):
                            name = name[1:].strip()
                except:
                    print(" Không tìm thấy tên, bỏ qua")
                    continue
                
                price_original = ""
                price_final = ""
                try:
                    try:
                        original_elem = container.find_element(By.CSS_SELECTOR, "div.price.original, span.price.original")
                        price_original = original_elem.text.strip()
                    except:
                        pass
                    
                    final_elem = container.find_element(By.CSS_SELECTOR, "div.price.final, span.price.final")
                    price_final = final_elem.text.strip()
                except:
                    print(" Không tìm thấy giá, bỏ qua")
                    continue
                
                discount = ""
                try:
                    discount_elem = container.find_element(By.CSS_SELECTOR, "span.message.marketing.warp-extended-text")
                    discount = discount_elem.text.strip()
                except:
                    pass
                
                image_url = ""
                try:
                    img_elem = container.find_element(By.CSS_SELECTOR, "a.product-image-link img")
                    image_url = img_elem.get_attribute("src") or img_elem.get_attribute("data-src")
                except:
                    print(" Không tìm thấy ảnh")
                    continue
                
                # Tạo folder và tải ảnh
                image_filenames = []
                if image_url:
                    all_image_urls = [image_url]
                    image_filenames = download_all_product_images(
                        all_image_urls, 
                        IMAGES_FOLDER, 
                        current_id, 
                        f"{brand}_{name}" if brand else name
                    )
                
                rating, comment_count = generate_random_rating_data()
                
                product_info = {
                    "metadata": {
                        "source": "theiconic.com.au",
                        "category": "groming",
                        "crawl_time": time.strftime('%Y-%m-%d %H:%M:%S'),
                        "currency": "AUD",
                        "product_image_urls": [image_url] if image_url else []
                    },
                    "id": current_id,
                    "brand": brand,
                    "name": name,
                    "price_original": price_original,
                    "price_final": price_final,
                    "discount": discount,
                    "image_urls": [image_url] if image_url else [],
                    "image_filenames": image_filenames,
                    "description": "",  # Không có mô tả chi tiết
                    "additional_info": {},
                    "rating": rating,
                    "comment_count": comment_count,
                    "url": url
                }
                
                # Gửi đến Kafka
                try:
                    producer.send(TOPIC, product_info)
                    print(f" Đã gửi đến Kafka: {brand} - {name} (ID: {current_id})")
                except Exception as e:
                    print(f" Lỗi gửi Kafka: {str(e)}")
                
                products_data.append(product_info)
                print(f" Đã thêm: {brand} - {name} - {price_final}")
                print(f"  Đã tải {len(image_filenames)} ảnh")
                
                # Thêm delay giữa các sản phẩm
                time.sleep(3)
                
            except Exception as e:
                print(f" Lỗi sản phẩm {i+1}: {e}")
                continue
    else:
        # Xử lý với link chi tiết - GIẢM SỐ LƯỢNG SẢN PHẨM ĐỂ LẤY NHIỀU ẢNH HƠN
        max_products = min(6, len(product_links))  # Giảm số lượng sản phẩm nhưng lấy nhiều ảnh hơn
        
        for i in range(max_products):
            try:
                product_link = product_links[i]
                current_id = next_id + i
                print(f"\n{'='*80}")
                print(f" Đang xử lý sản phẩm {i+1}/{max_products} (ID: {current_id})")
                print(f" Link: {product_link}")
                print(f"{'='*80}")
                
                # Lấy thông tin cơ bản từ trang danh sách trước
                product_containers = driver.find_elements(By.CSS_SELECTOR, "#catalogProductsList > div")
                if i < len(product_containers):
                    container = product_containers[i]
                    
                    # Lấy thông tin cơ bản từ container
                    brand = ""
                    try:
                        brand_elem = container.find_element(By.CSS_SELECTOR, "span.brand")
                        brand = brand_elem.text.strip()
                    except:
                        pass
                    
                    name = ""
                    try:
                        name_elem = container.find_element(By.CSS_SELECTOR, "span.name")
                        name = name_elem.text.strip()
                        if brand and name.startswith(brand):
                            name = name.replace(brand, "").strip()
                            if name.startswith("-"):
                                name = name[1:].strip()
                    except:
                        print(" Không tìm thấy tên, bỏ qua")
                        continue
                    
                    price_original = ""
                    price_final = ""
                    try:
                        try:
                            original_elem = container.find_element(By.CSS_SELECTOR, "div.price.original, span.price.original")
                            price_original = original_elem.text.strip()
                        except:
                            pass
                        
                        final_elem = container.find_element(By.CSS_SELECTOR, "div.price.final, span.price.final")
                        price_final = final_elem.text.strip()
                    except:
                        print(" Không tìm thấy giá, bỏ qua")
                        continue
                    
                    discount = ""
                    try:
                        discount_elem = container.find_element(By.CSS_SELECTOR, "span.message.marketing.warp-extended-text")
                        discount = discount_elem.text.strip()
                    except:
                        pass
                    
                    print(f" Thông tin cơ bản: {brand} - {name} - {price_final}")
                    
                    # Lấy thông tin chi tiết từ trang sản phẩm
                    details = extract_product_details(driver, product_link)
                    
                    # Kết hợp ảnh từ trang danh sách và trang chi tiết
                    all_image_urls = details['all_image_urls']
                    if not all_image_urls:
                        # Fallback: lấy ảnh từ trang danh sách
                        try:
                            img_elem = container.find_element(By.CSS_SELECTOR, "a.product-image-link img")
                            image_url = img_elem.get_attribute("src") or img_elem.get_attribute("data-src")
                            if image_url:
                                all_image_urls = [image_url]
                        except:
                            pass
                    
                    # Tải tất cả ảnh
                    image_filenames = []
                    if all_image_urls:
                        print(f" Đang tải {len(all_image_urls)} ảnh...")
                        image_filenames = download_all_product_images(
                            all_image_urls, 
                            IMAGES_FOLDER, 
                            current_id, 
                            f"{brand}_{name}" if brand else name
                        )
                    
                    rating, comment_count = generate_random_rating_data()
                    
                    product_info = {
                        "metadata": {
                            "source": "theiconic.com.au",
                            "category": "groming",
                            "crawl_time": time.strftime('%Y-%m-%d %H:%M:%S'),
                            "currency": "AUD",
                            "product_image_urls": all_image_urls
                        },
                        "id": current_id,
                        "brand": brand,
                        "name": name,
                        "price_original": price_original,
                        "price_final": price_final,
                        "discount": discount,
                        "image_urls": all_image_urls,
                        "image_filenames": image_filenames,
                        "description": details['description'],
                        "additional_info": details['additional_info'],
                        "rating": rating,
                        "comment_count": comment_count,
                        "url": product_link  # Sử dụng link chi tiết
                    }
                    
                    # Gửi đến Kafka
                    try:
                        producer.send(TOPIC, product_info)
                        print(f" ✅ Đã gửi đến Kafka: {brand} - {name} (ID: {current_id})")
                    except Exception as e:
                        print(f" ❌ Lỗi gửi Kafka: {str(e)}")
                    
                    products_data.append(product_info)
                    print(f" ✅ Đã thêm: {brand} - {name} - {price_final}")
                    print(f" 📸 Đã tải {len(image_filenames)} ảnh vào folder sản phẩm")
                    print(f" 📝 Mô tả: {details['description'][:100]}..." if details['description'] else " 📝 Không có mô tả chi tiết")
                
                else:
                    print(" ❌ Container không tồn tại, bỏ qua")
                
                # Thêm delay dài hơn giữa các sản phẩm
                print(" ⏳ Chờ 8 giây trước khi xử lý sản phẩm tiếp theo...")
                time.sleep(8)
                
            except Exception as e:
                print(f" ❌ Lỗi sản phẩm {i+1}: {e}")
                # Thử tiếp tục với sản phẩm tiếp theo
                continue

except Exception as e:
    print(f" ❌ Lỗi thu thập dữ liệu: {e}")

finally:
    # Đóng kết nối
    print(" Đang đóng kết nối...")
    producer.flush()
    producer.close()
    driver.quit()

# Đọc dữ liệu cũ và thêm dữ liệu mới
all_products_data = []
if os.path.exists(OUTPUT_JSON):
    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            all_products_data = json.load(f)
        print(f" 📁 Đã tải {len(all_products_data)} sản phẩm từ file cũ")
    except Exception as e:
        print(f" ❌ Lỗi đọc file cũ: {e}")

# Thêm sản phẩm mới vào cuối
all_products_data.extend(products_data)

# Lưu file JSON với tất cả dữ liệu
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(all_products_data, f, ensure_ascii=False, indent=2)

print(f"\n 🎉 HOÀN THÀNH!")
print(f" 💾 Đã lưu {len(all_products_data)} sản phẩm vào {OUTPUT_JSON}")
total_images = sum(len(p.get('image_filenames', [])) for p in products_data)
print(f" 📸 Đã tải {total_images} ảnh mới")
print(f" 📤 Đã gửi {len(products_data)} message đến Kafka topic: {TOPIC}")

# Hiển thị kết quả mới
if products_data:
    print(f"\n 📊 KẾT QUẢ MỚI:")
    for product in products_data:
        images_count = len(product.get('image_filenames', []))
        print(f"{product['id']}. {product['brand']} - {product['name']}")
        print(f"    💰 {product['price_final']} (Was: {product['price_original']})")
        if product['discount']:
            print(f"    🏷️ {product['discount']}")
        print(f"    ⭐ Rating: {product['rating']} ({product['comment_count']} reviews)")
        print(f"    📸 {images_count} ảnh | 📝 Mô tả: {product.get('description', '')[:80]}...")
        print("-" * 80)