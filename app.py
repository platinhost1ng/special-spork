import asyncio
import websockets
import json
import sys
import os
import requests
import time
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

# .env dosyasını yükle
load_dotenv()

# ============== .ENV DOSYASINDAN OKU ==============
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SOURCE_CHANNEL_ID = os.getenv("SOURCE_CHANNEL_ID", "1011057472888389702")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "1457815031839199267")
# ==================================================

# Hariç tutulacak dosya türleri (video ve resim)
EXCLUDED_EXTENSIONS = {
    '.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.m4v',
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.ico',
    '.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma'
}

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
            "Authorization": f"Bot {token}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.api_url = "https://discord.com/api/v10"
        self.processed_files = set()  # Duplicate önlemek için
        
    async def connect(self):
        """Connect to Discord Gateway"""
        print("[*] Discord Gateway'e bağlanıyor...")
        
        try:
            # Discord Gateway URL
            gateway_url = "wss://gateway.discord.gg/?v=10&encoding=json"
            
            async with websockets.connect(gateway_url) as websocket:
                self.ws = websocket
                print("[+] WebSocket bağlantısı kuruldu")
                
                # Listen for messages
                async for message in websocket:
                    await self.handle_message(message)
                    
        except Exception as e:
            print(f"[-] Hata: {e}")
            
    async def handle_message(self, message: str):
        """Handle incoming WebSocket messages"""
        data = json.loads(message)
        op = data.get("op")
        
        if op == 10:  # HELLO
            print("[+] HELLO mesajı alındı")
            self.heartbeat_interval = data["d"]["heartbeat_interval"]
            print(f"[*] Heartbeat intervali: {self.heartbeat_interval}ms")
            
            # Identify bot
            await self.identify()
            
            # Start heartbeat
            asyncio.create_task(self.heartbeat(self.heartbeat_interval / 1000))
            
        elif op == 0:  # DISPATCH
            event_type = data.get("t")
            self.sequence = data.get("s")
            
            if event_type == "READY":
                user_data = data["d"]["user"]
                self.bot_name = user_data.get("username")
                self.session_id = data["d"]["session_id"]
                self.resume_gateway_url = data["d"]["resume_gateway_url"]
                
                print(f"\n[+] === BOT BAŞARILI İLE BAĞLANDI ===")
                print(f"[+] Bot Adı: {self.bot_name}")
                print(f"[+] User ID: {user_data.get('id')}")
                print(f"[+] Bot mı?: {user_data.get('bot')}")
                print(f"[+] ========================\n")
                
            elif event_type == "MESSAGE_CREATE":
                message_data = data["d"]
                author = message_data.get("author", {}).get("username")
                content = message_data.get("content")
                # Dosya bilgisini göster
                attachments = message_data.get("attachments", [])
                if attachments:
                    print(f"[MESSAGE] {author}: {len(attachments)} dosya - {content}")
                
        elif op == 1:  # HEARTBEAT request
            await self.send_heartbeat()
            
        elif op == 11:  # HEARTBEAT_ACK
            pass  # Silent
            
    async def identify(self):
        """Send IDENTIFY message to Discord"""
        identify_payload = {
            "op": 2,
            "d": {
                "token": self.token,
                "intents": 513,  # GUILDS | GUILD_MESSAGES
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
    
    def get_all_channels_from_guild(self, guild_id: str):
        """Sunucudaki tüm kanalları al"""
        print(f"\n[*] Sunucu {guild_id}'daki kanallar taranıyor...")
        
        try:
            url = f"{self.api_url}/guilds/{guild_id}/channels"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                channels = response.json()
                text_channels = [ch for ch in channels if ch.get("type") == 0]  # type 0 = text channel
                print(f"[+] {len(text_channels)} metin kanalı bulundu")
                return text_channels
            else:
                print(f"[-] Hata {response.status_code}: {response.text}")
                return []
                
        except Exception as e:
            print(f"[-] Hata: {e}")
            return []
    
    def get_all_messages_from_channel(self, channel_id: str, channel_name: str):
        """Kanaldan TÜM mesajları geriye doğru taraması (en eskileri bulana kadar)"""
        message_count = 0
        page = 1
        last_message_id = None
        
        try:
            while True:
                # Geriye doğru tarama (before parametresi ile)
                if last_message_id:
                    url = f"{self.api_url}/channels/{channel_id}/messages?limit=100&before={last_message_id}"
                else:
                    url = f"{self.api_url}/channels/{channel_id}/messages?limit=100"
                
                print(f"    [*] Sayfa {page} taranıyor...")
                response = requests.get(url, headers=self.headers)
                
                if response.status_code != 200:
                    if response.status_code == 429:  # Rate limit
                        retry_after = response.json().get("retry_after", 1)
                        print(f"    [!] Rate limit - {retry_after} saniye bekleniyor...")
                        time.sleep(retry_after)
                        continue
                    break
                
                messages = response.json()
                
                if not messages:
                    break
                
                message_count += len(messages)
                page_files = []
                
                # Bu sayfadaki dosyaları bul
                for message in messages:
                    attachments = message.get("attachments", [])
                    
                    for attachment in attachments:
                        filename = attachment.get("filename", "")
                        
                        # Sadece izin verilen dosyalar
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
                                print(f"        [+] Bulundu: {filename} ({attachment.get('size', 0) // 1024} KB)")
                
                # Bu sayfada bulunan dosyaları hemen gönder
                if page_files:
                    print(f"\n    [*] Bu sayfadaki {len(page_files)} dosya yükleniyor...")
                    yield page_files
                    print(f"")
                
                # Son mesajın ID'sini al (sonraki sayfada before için)
                if messages:
                    last_message_id = messages[-1].get("id")
                else:
                    break
            
            print(f"    [+] {message_count} mesaj tarandı")
                        
        except Exception as e:
            print(f"    [-] Hata: {e}")
    
    def find_all_files(self, guild_id: str):
        """Sunucudaki tüm dosyaları bul (tüm kanalları tara)"""
        print(f"\n[*] {guild_id} sunucusunda TÜÜN dosyalar aranıyor...\n")
        
        channels = self.get_all_channels_from_guild(guild_id)
        all_files = []
        
        for idx, channel in enumerate(channels, 1):
            channel_id = channel.get("id")
            channel_name = channel.get("name")
            
            print(f"\n[{idx}/{len(channels)}] Kanal taranıyor: #{channel_name} ({channel_id})")
            
            files = self.get_all_messages_from_channel(channel_id, channel_name)
            all_files.extend(files)
            
            # Rate limit önlemek için
            time.sleep(0.5)
        
        if all_files:
            print(f"\n[+] === TOPLAM {len(all_files)} DOSYA BULUNDU ===\n")
        else:
            print(f"\n[-] Dosya bulunamadı\n")
        
        return all_files
    
    def upload_file_to_channel(self, channel_id: str, file_url: str, filename: str):
        """Discord'a dosya yükle"""
        try:
            # Dosyayı indir
            print(f"    [*] İndiriliyor: {filename}")
            file_response = requests.get(file_url)
            
            if file_response.status_code != 200:
                print(f"    [-] İndirme hatası")
                return False
            
            # Discord'a yükle
            files = {'file': (filename, file_response.content)}
            data = {'content': f'📦 **{filename}**'}
            
            url = f"{self.api_url}/channels/{channel_id}/messages"
            response = requests.post(url, headers=self.headers, files=files, data=data)
            
            if response.status_code == 200:
                print(f"    [+] Yüklendi: {filename}")
                return True
            else:
                print(f"    [-] Yükleme hatası: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"    [-] Hata: {e}")
            return False
    
    def sync_build_files(self, source_guild_id: str, target_channel_id: str, interval: int = 10):
        """Belirli bir kanaldan hedef kanala dosya senkronize et (sayfa sayfa)"""
        
        print(f"\n{'='*70}")
        print(f"[*] SİNCRONİZASYON BAŞLANIYOR")
        print(f"[*] Kanal: 1011057472888389702 → {target_channel_id}")
        print(f"{'='*70}\n")
        
        # Sadece belirtilen kanalı tara
        scan_channel_id = "1011057472888389702"  # Taranacak kanal
        
        print(f"[*] Kanal taranıyor: {scan_channel_id}\n")
        
        # Generator kullanarak sayfa sayfa işle
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
                    
                    # Her dosya arasında interval
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

async def main():
    print("\n" + "=" * 70)
    print("   DISCORD BOT - DOSYA SENKRONIZASYON SİSTEMİ")
    print("=" * 70 + "\n")
    
    # .env dosyasından oku
    token = BOT_TOKEN
    source_channel_id = SOURCE_CHANNEL_ID
    target_channel_id = TARGET_CHANNEL_ID
    interval = 10  # 10 saniye aralık
    
    # Token kontrolü
    if not token:
        print("[-] HATA: Token ayarlanmamış!")
        print("[*] Kök klasöre .env dosyası oluşturun şu içerikle:\n")
        print("BOT_TOKEN=YOUR_TOKEN_HERE")
        print("SOURCE_CHANNEL_ID=1011057472888389702")
        print("TARGET_CHANNEL_ID=1457815031839199267")
        return
    
    print(f"[+] .env dosyası yüklendi!")
    print(f"[+] Token: {token[:30]}...")
    print(f"[+] Kaynak Kanal: {source_channel_id}")
    print(f"[+] Hedef Kanal: {target_channel_id}")
    print(f"[+] Aralık: {interval} saniye\n")
    
    bot = DiscordBot(token)
    
    # Direk senkronize et
    print("[*] Senkronizasyon başlatılıyor...\n")
    bot.sync_build_files("", target_channel_id, interval)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[-] Bot kapatıldı")
