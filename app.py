import asyncio
import websockets
import json
import os
import requests
import time
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask
from threading import Thread

# .env dosyasını yükle
load_dotenv()

# ============== .ENV DOSYASINDAN OKU ==============
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SOURCE_CHANNEL_ID = os.getenv("SOURCE_CHANNEL_ID", "1011057472888389702")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "1457815031839199267")
# ==================================================

# Hariç tutulacak dosya türleri
EXCLUDED_EXTENSIONS = {
    '.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.m4v',
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.ico',
    '.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma'
}

# Global bot instance
bot_instance = None
bot_running = False

class DiscordBot:
    def __init__(self, token: str):
        self.token = token
        self.bot_name: Optional[str] = None
        self.ws = None
        self.sequence = None
        self.session_id = None
        self.resume_gateway_url = None
        self.heartbeat_interval = None
        self.headers = {
            "Authorization": f"{token}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.api_url = "https://discord.com/api/v10"
        self.processed_files = set()
        
    async def connect(self):
        """Connect to Discord Gateway"""
        print("[*] Discord Gateway'e bağlanıyor...")
        
        try:
            gateway_url = "wss://gateway.discord.gg/?v=10&encoding=json"
            
            async with websockets.connect(gateway_url) as websocket:
                self.ws = websocket
                print("[+] WebSocket bağlantısı kuruldu")
                
                async for message in websocket:
                    await self.handle_message(message)
                    
        except Exception as e:
            print(f"[-] Hata: {e}")
            
    async def handle_message(self, message: str):
        """Handle incoming WebSocket messages"""
        data = json.loads(message)
        op = data.get("op")
        
        if op == 10:
            print("[+] HELLO mesajı alındı")
            self.heartbeat_interval = data["d"]["heartbeat_interval"]
            
            await self.identify()
            asyncio.create_task(self.heartbeat(self.heartbeat_interval / 1000))
            
        elif op == 0:
            event_type = data.get("t")
            self.sequence = data.get("s")
            
            if event_type == "READY":
                user_data = data["d"]["user"]
                self.bot_name = user_data.get("username")
                self.session_id = data["d"]["session_id"]
                self.resume_gateway_url = data["d"]["resume_gateway_url"]
                
                print(f"\n[+] === BOT BAŞARILI İLE BAĞLANDI ===")
                print(f"[+] Bot Adı: {self.bot_name}")
                print(f"[+] ========================\n")
                
        elif op == 1:
            await self.send_heartbeat()
            
        elif op == 11:
            pass
            
    async def identify(self):
        """Send IDENTIFY message to Discord"""
        identify_payload = {
            "op": 2,
            "d": {
                "token": self.token,
                "intents": 513,
                "properties": {
                    "os": "Windows",
                    "browser": "CrustyBot",
                    "device": "CrustyBot"
                }
            }
        }
        
        await self.ws.send(json.dumps(identify_payload))
        print("[*] IDENTIFY mesajı gönderildi")
        
    async def heartbeat(self, interval: float):
        """Send heartbeat to keep connection alive"""
        while True:
            try:
                await asyncio.sleep(interval)
                await self.send_heartbeat()
            except:
                break
                
    async def send_heartbeat(self):
        """Send heartbeat packet"""
        heartbeat_payload = {
            "op": 1,
            "d": self.sequence
        }
        
        try:
            await self.ws.send(json.dumps(heartbeat_payload))
        except:
            pass
    
    def is_valid_file(self, filename: str) -> bool:
        """Video ve resim hariç dosya kontrolü"""
        file_ext = os.path.splitext(filename)[1].lower()
        return file_ext not in EXCLUDED_EXTENSIONS and file_ext != ""
    
    def get_all_messages_from_channel(self, channel_id: str, channel_name: str):
        """Kanaldan TÜM mesajları geriye doğru taraması"""
        message_count = 0
        page = 1
        last_message_id = None
        found_files_total = 0
        
        try:
            while True:
                if last_message_id:
                    url = f"{self.api_url}/channels/{channel_id}/messages?limit=100&before={last_message_id}"
                else:
                    url = f"{self.api_url}/channels/{channel_id}/messages?limit=100"
                
                print(f"    [*] Sayfa {page} taranıyor...")
                response = requests.get(url, headers=self.headers, timeout=30)
                
                if response.status_code != 200:
                    if response.status_code == 429:
                        retry_after = response.json().get("retry_after", 1)
                        print(f"    [!] Rate limit - {retry_after} saniye bekleniyor...")
                        time.sleep(retry_after)
                        continue
                    print(f"    [-] API Hatası: {response.status_code}")
                    break
                
                messages = response.json()
                
                if not messages:
                    print(f"    [*] Daha fazla mesaj yok")
                    break
                
                message_count += len(messages)
                page_files = []
                
                for message in messages:
                    attachments = message.get("attachments", [])
                    
                    for attachment in attachments:
                        filename = attachment.get("filename", "")
                        
                        if self.is_valid_file(filename):
                            file_id = f"{channel_id}_{filename}"
                            if file_id not in self.processed_files:
                                file_info = {
                                    "url": attachment.get("url"),
                                    "name": filename,
                                    "channel_id": channel_id,
                                    "channel_name": channel_name,
                                    "size": attachment.get("size")
                                }
                                page_files.append(file_info)
                                self.processed_files.add(file_id)
                                found_files_total += 1
                                print(f"        [+] Bulundu: {filename} ({attachment.get('size', 0) // 1024} KB)")
                
                if page_files:
                    print(f"\n    [*] Bu sayfadaki {len(page_files)} dosya yükleniyor...")
                    yield page_files
                    print(f"")
                else:
                    print(f"    [*] Bu sayfada dosya yok")
                
                if messages:
                    last_message_id = messages[-1].get("id")
                else:
                    break
                
                page += 1
                # Rate limit önlemek için
                time.sleep(0.5)
            
            print(f"    [+] {message_count} mesaj tarandı, {found_files_total} dosya bulundu")
                        
        except Exception as e:
            print(f"    [-] Hata: {e}")
    
    def upload_file_to_channel(self, channel_id: str, file_url: str, filename: str):
        """Discord'a dosya yükle"""
        try:
            print(f"    [*] İndiriliyor: {filename}")
            file_response = requests.get(file_url, timeout=30)
            
            if file_response.status_code != 200:
                print(f"    [-] İndirme hatası: {file_response.status_code}")
                return False
            
            print(f"    [*] Discord'a yükleniyor...")
            files = {'file': (filename, file_response.content)}
            data = {'content': f'📦 **{filename}**'}
            
            url = f"{self.api_url}/channels/{channel_id}/messages"
            response = requests.post(url, headers=self.headers, files=files, data=data, timeout=30)
            
            if response.status_code == 200:
                print(f"    [+] Yüklendi: {filename}")
                return True
            elif response.status_code == 429:
                retry_after = response.json().get("retry_after", 5)
                print(f"    [!] Rate limit - {retry_after} saniye bekleniyor...")
                time.sleep(retry_after)
                # Tekrar dene
                response = requests.post(url, headers=self.headers, files=files, data=data, timeout=30)
                if response.status_code == 200:
                    print(f"    [+] Yüklendi: {filename}")
                    return True
                else:
                    print(f"    [-] Yükleme hatası: {response.status_code} - {response.text}")
                    return False
            else:
                print(f"    [-] Yükleme hatası: {response.status_code}")
                print(f"    [-] Hata: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            print(f"    [-] Timeout hatası")
            return False
        except Exception as e:
            print(f"    [-] Hata: {e}")
            return False
    
    def sync_build_files(self, target_channel_id: str, interval: int = 10):
        """Belirli bir kanaldan hedef kanala dosya senkronize et"""
        
        global bot_running
        bot_running = True
        
        print(f"\n{'='*70}")
        print(f"[*] SİNCRONİZASYON BAŞLANIYOR")
        print(f"[*] Kanal: {SOURCE_CHANNEL_ID} → {target_channel_id}")
        print(f"{'='*70}\n")
        
        scan_channel_id = SOURCE_CHANNEL_ID
        print(f"[*] Kanal taranıyor: {scan_channel_id}\n")
        
        page_generator = self.get_all_messages_from_channel(scan_channel_id, "scan_channel")
        
        total_uploaded = 0
        total_failed = 0
        page_count = 0
        
        try:
            for page_files in page_generator:
                page_count += 1
                print(f"\n[*] SAYFA {page_count} YÜKLENIYOR ({len(page_files)} dosya):")
                print(f"{'='*70}\n")
                
                uploaded_count = 0
                failed_count = 0
                
                for idx, file_info in enumerate(page_files, 1):
                    print(f"[{idx}/{len(page_files)}] {file_info['name']}")
                    
                    success = self.upload_file_to_channel(
                        target_channel_id,
                        file_info['url'],
                        file_info['name']
                    )
                    
                    if success:
                        uploaded_count += 1
                        total_uploaded += 1
                    else:
                        failed_count += 1
                        total_failed += 1
                    
                    if idx < len(page_files):
                        print(f"    [*] {interval} saniye bekleniyor...\n")
                        time.sleep(interval)
                
                print(f"\n[+] Sayfa {page_count} Tamamlandı!")
                print(f"    Başarılı: {uploaded_count}/{len(page_files)}")
                print(f"    Başarısız: {failed_count}/{len(page_files)}")
                print(f"\n[*] Bir sonraki sayfaya geçiliyor...\n")
                print(f"{'='*70}\n")
        
        except Exception as e:
            print(f"[-] Hata: {e}")
        
        print(f"\n{'='*70}")
        print(f"[+] TÜM SAYFALAR TAMAMLANDI!")
        print(f"[+] Toplam Başarılı: {total_uploaded}")
        print(f"[+] Toplam Başarısız: {total_failed}")
        print(f"[+] İşlenen Sayfa: {page_count}")
        print(f"{'='*70}\n")
        
        bot_running = False

# ============== FLASK WEB SUNUCUSU ==============
app = Flask(__name__)

@app.route('/')
def status():
    return "Bot Aktif"

# ==================================================

# Discord Bot'u arka planda çalıştır
def run_bot():
    """Discord bot'u async loop'ta çalıştır"""
    if not BOT_TOKEN:
        print("[-] HATA: Token ayarlanmamış!")
        print("[*] .env dosyasında BOT_TOKEN ayarlayın")
        return
    
    print(f"[+] Bot başlatılıyor...")
    global bot_instance
    bot_instance = DiscordBot(BOT_TOKEN)
    
    # Event loop oluştur ve bot'u çalıştır
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(bot_instance.connect())
    except KeyboardInterrupt:
        print("\n[-] Bot kapatıldı")
    finally:
        loop.close()

def start_sync():
    """Senkronizasyonu başlat"""
    if bot_instance:
        bot_instance.sync_build_files(TARGET_CHANNEL_ID, 10)

if __name__ == "__main__":
    # Bot'u ayrı thread'te başlat
    bot_thread = Thread(target=run_bot, daemon=True)
    bot_thread.start()
    
    # Kısa gecikmeden sonra senkronizasyonu başlat
    time.sleep(2)
    sync_thread = Thread(target=start_sync, daemon=True)
    sync_thread.start()
    
    # Flask web sunucusunu başlat
    print("\n[+] Flask web sunucusu başlatılıyor: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
