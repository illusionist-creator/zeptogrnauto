#!/usr/bin/env python3
"""
Zepto Google Drive PDF Processor with LlamaParse to Google Sheets
Processes PDFs from Google Drive with LlamaParse and appends data to Google Sheets
Uses the extraction logic from ZeptoAgent with OAuth2 authentication
"""

import os
import json
import time
import logging
import tempfile
import ssl
import socket
from typing import List, Dict, Any

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Add LlamaParse import
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False
    print("LlamaParse not available. Install with: pip install llama-cloud-services")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('zepto_drive_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ZeptoDriveProcessor:
    def __init__(self, credentials_path: str):
        """
        Initialize the PDF processor
        
        Args:
            credentials_path: Path to the Google credentials JSON file
        """
        self.credentials_path = credentials_path
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes - updated to use the correct format
        self.drive_scopes = ['https://www.googleapis.com/auth/drive.readonly']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
    def test_network_connection(self) -> bool:
        """Test network and SSL connectivity to Google APIs"""
        try:
            # Test basic internet connectivity
            socket.create_connection(("www.google.com", 443), timeout=10)
            print("✅ Basic network connectivity test passed")
            
            # Test SSL handshake
            context = ssl.create_default_context()
            with socket.create_connection(("www.google.com", 443), timeout=10) as sock:
                with context.wrap_socket(sock, server_hostname="www.google.com") as ssock:
                    print("✅ SSL handshake successful")
            
            return True
            
        except Exception as e:
            print(f"❌ Network connectivity test failed: {str(e)}")
            logger.error(f"[NETWORK] Connection test failed: {str(e)}")
            return False
    
    def authenticate(self):
        """Authenticate with Google Drive and Google Sheets APIs"""
        if not self.test_network_connection():
            return False
            
        try:
            # Load credentials from JSON file
            with open(self.credentials_path, 'r') as f:
                creds_data = json.load(f)
            
            # Check if it's a service account or OAuth2 credentials
            if 'type' in creds_data and creds_data['type'] == 'service_account':
                print("🔑 Using service account authentication")
                # Service account authentication
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=self.drive_scopes + self.sheets_scopes
                )
                
                # Build services with retry mechanism
                self.drive_service = self._build_service_with_retry('drive', 'v3', credentials)
                self.sheets_service = self._build_service_with_retry('sheets', 'v4', credentials)
                
            else:
                print("🔑 Using OAuth2 authentication")
                # OAuth2 authentication - use combined scopes
                combined_scopes = self.drive_scopes + self.sheets_scopes
                creds = self._oauth2_authenticate(combined_scopes, 'combined')
                
                # Build services with retry mechanism
                self.drive_service = self._build_service_with_retry('drive', 'v3', creds)
                self.sheets_service = self._build_service_with_retry('sheets', 'v4', creds)
            
            if self.drive_service and self.sheets_service:
                logger.info("[SUCCESS] Successfully authenticated with Google Drive and Sheets")
                print("✅ Authentication successful")
                return True
            else:
                logger.error("[ERROR] Failed to build one or more services")
                print("❌ Failed to build one or more services")
                return False
            
        except Exception as e:
            logger.error(f"[ERROR] Authentication failed: {str(e)}")
            print(f"❌ Authentication failed: {str(e)}")
            return False
    
    def _build_service_with_retry(self, service_name: str, version: str, credentials, max_retries: int = 3) -> Any:
        """Build service with retry mechanism for network issues"""
        for attempt in range(1, max_retries + 1):
            try:
                service = build(service_name, version, credentials=credentials)
                print(f"✅ Successfully built {service_name} service")
                return service
            except Exception as e:
                if attempt < max_retries:
                    print(f"⚠️ Failed to build {service_name} service (attempt {attempt}/{max_retries}): {str(e)}")
                    time.sleep(2)  # Wait before retry
                else:
                    print(f"❌ Failed to build {service_name} service after {max_retries} attempts: {str(e)}")
                    logger.error(f"[ERROR] Failed to build {service_name} service: {str(e)}")
                    return None
    
    def _oauth2_authenticate(self, scopes: List[str], service_name: str) -> Credentials:
        """Handle OAuth2 authentication flow"""
        creds = None
        token_file = f'token_{service_name}.json'
        
        # Check for existing token
        if os.path.exists(token_file):
            try:
                creds = Credentials.from_authorized_user_file(token_file, scopes)
                print("✅ Loaded existing OAuth2 token")
            except Exception as e:
                print(f"⚠️ Error loading token file: {str(e)}")
                creds = None
        
        # If no valid credentials, run OAuth flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    print("✅ Refreshed OAuth2 token")
                except Exception as e:
                    print(f"⚠️ Token refresh failed: {str(e)}")
                    creds = None
            
            if not creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, scopes)
                    creds = flow.run_local_server(port=0)
                    print("✅ Completed OAuth2 authentication flow")
                except Exception as e:
                    print(f"❌ OAuth flow failed: {str(e)}")
                    raise
            
            # Save credentials for next run
            try:
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
                print("✅ Saved OAuth2 token for future use")
            except Exception as e:
                print(f"⚠️ Failed to save token: {str(e)}")
        
        return creds
    
    def list_drive_files(self, folder_id: str) -> List[Dict]:
        """List all PDF files in a Google Drive folder"""
        try:
            # Query to find all PDF files in the specified folder
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
            
            files = []
            page_token = None

            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc",
                    pageToken=page_token,
                    pageSize=100
                ).execute()
                
                files.extend(results.get('files', []))
                page_token = results.get('nextPageToken', None)
                
                if page_token is None:
                    break

            print(f"📂 Found {len(files)} PDF files in folder")
            logger.info(f"[DRIVE] Found {len(files)} PDF files in folder {folder_id}")
            
            # Print file names for debugging
            for file in files:
                print(f"   - {file['name']}")
                logger.info(f"[DRIVE] Found file: {file['name']} (ID: {file['id']})")
            
            return files

        except Exception as e:
            print(f"❌ Failed to list files: {str(e)}")
            logger.error(f"[ERROR] Failed to list files in folder {folder_id}: {str(e)}")
            return []
    
    def download_from_drive(self, file_id: str, file_name: str) -> bytes:
        """Download a file from Google Drive"""
        try:
            print(f"⬇️ Downloading: {file_name}")
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = request.execute()
            print(f"✅ Downloaded: {file_name}")
            return file_data
        except Exception as e:
            print(f"❌ Failed to download {file_name}: {str(e)}")
            logger.error(f"[ERROR] Failed to download file {file_name}: {str(e)}")
            return b""
    
    def append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        """Append data to a Google Sheet with retry mechanism"""
        max_retries = 3
        wait_time = 2
        
        for attempt in range(1, max_retries + 1):
            try:
                body = {
                    'values': values
                }
                
                result = self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id, 
                    range=range_name,
                    valueInputOption='USER_ENTERED', 
                    body=body
                ).execute()
                
                updated_cells = result.get('updates', {}).get('updatedCells', 0)
                print(f"💾 Appended {updated_cells} cells to Google Sheet")
                logger.info(f"[SHEETS] Appended {updated_cells} cells to Google Sheet")
                return True
                
            except Exception as e:
                if attempt < max_retries:
                    print(f"⚠️ Failed to append to Google Sheet (attempt {attempt}/{max_retries}): {str(e)}")
                    logger.warning(f"[SHEETS] Attempt {attempt} failed: {str(e)}")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Failed to append to Google Sheet after {max_retries} attempts: {str(e)}")
                    logger.error(f"[ERROR] Failed to append to Google Sheet: {str(e)}")
                    return False
        return False
    
    def get_sheet_headers(self, spreadsheet_id: str, range_name: str) -> List[str]:
        """Get existing headers from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                majorDimension="ROWS"
            ).execute()
            
            values = result.get('values', [])
            if values and len(values) > 0:
                return values[0]  # Return header row
            return []
            
        except Exception as e:
            print(f"ℹ️ No existing headers found or error: {str(e)}")
            logger.error(f"[ERROR] Failed to get sheet headers: {str(e)}")
            return []
    
    def get_value(self, data, possible_keys, default=""):
        """Return the first found key value from dict."""
        for key in possible_keys:
            if key in data:
                return data[key]
        return default
    
    def safe_extract(self, agent, file_path: str, retries: int = 3, wait_time: int = 2):
        """Retry-safe extraction to handle server disconnections"""
        for attempt in range(1, retries + 1):
            try:
                print(f"🔍 Extracting data (attempt {attempt}/{retries})...")
                result = agent.extract(file_path)
                print("✅ Extraction successful")
                return result
            except Exception as e:
                print(f"⚠️ Attempt {attempt} failed: {e}")
                logger.error(f"⚠️ Attempt {attempt} failed for {file_path}: {e}")
                time.sleep(wait_time)
        raise Exception(f"❌ Extraction failed after {retries} attempts for {file_path}")
    
    def process_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        """
        Process extracted data using ZeptoAgent logic
        Returns a list of dictionaries (rows) for Google Sheets
        """
        rows = []
        
        # Extract items from the document
        items = []
        if "items" in extracted_data:
            items = extracted_data["items"]
            for item in items:
                item["po_number"] = self.get_value(extracted_data, ["po_number", "purchase_order_number", "PO No"])
                item["vendor_invoice_number"] = self.get_value(extracted_data, ["vendor_invoice_number", "invoice_number", "inv_no", "Invoice No"])
                item["supplier"] = self.get_value(extracted_data, ["supplier", "vendor", "Supplier Name"])
                item["shipping_address"] = self.get_value(extracted_data, ["shipping_address", "receiver_address", "Shipping Address"])
                item["grn_date"] = self.get_value(extracted_data, ["grn_date", "delivered_on", "GRN Date"])
                item["source_file"] = file_info['name']
                item["processed_date"] = time.strftime("%Y-%m-%d %H:%M:%S")
                item["drive_file_id"] = file_info['id']

        elif "product_items" in extracted_data:
            items = extracted_data["product_items"]
            for item in items:
                item["po_number"] = self.get_value(extracted_data, ["purchase_order_number", "po_number", "PO No"])
                item["vendor_invoice_number"] = self.get_value(extracted_data, ["supplier_bill_number", "vendor_invoice_number", "invoice_number"])
                item["supplier"] = self.get_value(extracted_data, ["supplier", "vendor", "Supplier Name"])
                item["shipping_address"] = self.get_value(extracted_data, ["Shipping Address", "receiver_address", "shipping_address"])
                item["grn_date"] = self.get_value(extracted_data, ["delivered_on", "grn_date"])
                item["source_file"] = file_info['name']
                item["processed_date"] = time.strftime("%Y-%m-%d %H:%M:%S")
                item["drive_file_id"] = file_info['id']

        else:
            print(f"⚠ Skipping (no recognizable items key): {file_info['name']}")
            return rows
        
        # Clean empty fields
        for item in items:
            cleaned_item = {k: v for k, v in item.items() if v not in ["", None]}
            rows.append(cleaned_item)
        
        return rows
    
    def process_pdfs(self, drive_folder_id: str, api_key: str, agent_name: str, 
                    spreadsheet_id: str, sheet_range: str = "Sheet1") -> Dict:
        """
        Process PDFs from Google Drive with LlamaParse and save to Google Sheets
        Using ZeptoAgent extraction logic
        
        Args:
            drive_folder_id: Google Drive folder ID containing PDFs
            api_key: LlamaParse API key
            agent_name: LlamaParse agent name
            spreadsheet_id: Google Sheets ID to save results to
            sheet_range: Sheet range to update (default: "Sheet1")
        """
        stats = {
            'total_pdfs': 0,
            'processed_pdfs': 0,
            'failed_pdfs': 0,
            'rows_added': 0
        }
        
        if not LLAMA_AVAILABLE:
            print("❌ LlamaParse not available. Install with: pip install llama-cloud-services")
            logger.error("[ERROR] LlamaParse not available. Install with: pip install llama-cloud-services")
            return stats
        
        try:
            # Set up LlamaParse
            print("🔑 Setting up LlamaParse...")
            os.environ["LLAMA_CLOUD_API_KEY"] = api_key
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=agent_name)
            
            if agent is None:
                print(f"❌ Could not find agent '{agent_name}'. Check dashboard.")
                logger.error(f"[ERROR] Could not find agent '{agent_name}'. Check dashboard.")
                return stats
            
            print("✅ LlamaParse agent found")
            
            # Get PDF files from Drive
            print(f"📂 Searching for PDFs in folder ID: {drive_folder_id}")
            pdf_files = self.list_drive_files(drive_folder_id)
            stats['total_pdfs'] = len(pdf_files)
            
            if not pdf_files:
                print("❌ No PDF files found in the specified folder")
                logger.info("[INFO] No PDF files found in the specified folder")
                return stats
            
            print(f"📊 Found {len(pdf_files)} PDF files to process")
            
            all_rows = []
            
            # Get existing headers from Google Sheet to maintain consistency
            print("📋 Checking existing sheet headers...")
            existing_headers = self.get_sheet_headers(spreadsheet_id, sheet_range)
            
            for i, file in enumerate(pdf_files, 1):
                try:
                    print(f"\n📄 Processing PDF {i}/{len(pdf_files)}: {file['name']}")
                    print(f"📊 Progress: {i}/{len(pdf_files)} files processed")
                    logger.info(f"[LLAMA] Processing PDF {i}/{len(pdf_files)}: {file['name']}")
                    
                    # Download PDF from Drive
                    pdf_data = self.download_from_drive(file['id'], file['name'])
                    
                    if not pdf_data:
                        print(f"❌ Failed to download PDF: {file['name']}")
                        logger.error(f"[ERROR] Failed to download PDF: {file['name']}")
                        stats['failed_pdfs'] += 1
                        continue
                    
                    # Save to temporary file for processing
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                        temp_file.write(pdf_data)
                        temp_path = temp_file.name
                    
                    # Extract data with LlamaParse
                    result = self.safe_extract(agent, temp_path)
                    extracted_data = result.data
                    
                    # Clean up temp file
                    os.unlink(temp_path)
                    
                    # Process extracted data using ZeptoAgent logic
                    rows = self.process_extracted_data(extracted_data, file)
                    all_rows.extend(rows)
                    stats['processed_pdfs'] += 1
                    
                    print(f"✅ Successfully processed: {file['name']}")
                    print(f"📈 Extracted {len(rows)} rows from this PDF")
                    logger.info(f"[LLAMA] Successfully processed: {file['name']}")
                    
                except Exception as e:
                    print(f"❌ Error processing {file['name']}: {e}")
                    logger.error(f"[ERROR] Failed to process PDF {file['name']}: {str(e)}")
                    stats['failed_pdfs'] += 1
            
            # Prepare data for Google Sheets
            if all_rows:
                print(f"📊 Preparing {len(all_rows)} rows for Google Sheets...")
                
                # Get all unique keys to create comprehensive headers
                all_keys = set()
                for row in all_rows:
                    all_keys.update(row.keys())
                
                # Use existing headers if available, otherwise create new ones
                if existing_headers:
                    headers = existing_headers
                    # Add any missing headers
                    for key in all_keys:
                        if key not in headers:
                            headers.append(key)
                else:
                    headers = list(all_keys)
                
                # Convert to list of lists for Sheets API
                values = []
                if not existing_headers:  # First run - include headers
                    values.append(headers)
                
                for row in all_rows:
                    row_values = [row.get(h, "") for h in headers]
                    values.append(row_values)
                
                # Append to Google Sheet
                print("💾 Saving data to Google Sheets...")
                success = self.append_to_google_sheet(spreadsheet_id, sheet_range, values)
                
                if success:
                    stats['rows_added'] = len(all_rows)
                    print(f"\n✅ Successfully appended {len(all_rows)} rows to Google Sheet")
                    logger.info(f"[SHEETS] Successfully updated Google Sheet with {len(all_rows)} rows")
                else:
                    print("\n❌ Failed to update Google Sheet")
                    logger.error("[ERROR] Failed to update Google Sheet")
            
            return stats
            
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            logger.error(f"[ERROR] LlamaParse processing failed: {str(e)}")
            return stats

def main():
    """Run the PDF processing from Google Drive to Google Sheets"""
    
    print("=== Zepto Google Drive PDF Processor with LlamaParse ===")
    print("Processing PDFs from Google Drive and saving to Google Sheets")
    print("Using ZeptoAgent extraction logic with OAuth2 authentication")
    print()
    
    # Configuration - MODIFY THESE VALUES
    CONFIG = {
        'credentials_path': 'C:\\Users\\Lucifer\\Desktop\\New folder\\TBD\\GRN\\Zepto Automation\\credentials.json',
        'drive_folder_id': '18LRA2eMtHVPXQ2lQa5tuaYk9CAYNVJsW',  # Google Drive folder with PDFs
        'llama_api_key': 'llx-Tu2NvniPNGbp3VVCfJLkHvppt8JvLSoOVqoPhqmzFzmaGcTa',
        'llama_agent': 'Zepto Agent',
        'spreadsheet_id': '1YgLZfg7g07_koytHmEXEdy_BxU5sje3T1Ugnav0MIGI',
        'sheet_range': 'zeptogrn'
    }
    
    # Validate configuration
    if not os.path.exists(CONFIG['credentials_path']):
        print(f"[ERROR] Credentials file not found: {CONFIG['credentials_path']}")
        print()
        print("SETUP INSTRUCTIONS:")
        print("1. Go to https://console.cloud.google.com")
        print("2. Create a new project or select existing one")
        print("3. Enable Google Drive API and Google Sheets API")
        print("4. Go to 'Credentials' > 'Create Credentials' > 'OAuth client ID'")
        print("5. Choose 'Desktop application' as application type")
        print("6. Download the JSON file and save it as 'credentials.json'")
        print()
        print("Required packages:")
        print("pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        print("pip install llama-cloud-services")
        return
    
    # Initialize processor
    processor = ZeptoDriveProcessor(
        credentials_path=CONFIG['credentials_path']
    )
    
    # Authenticate
    print("🔐 Authenticating with Google APIs...")
    if not processor.authenticate():
        print("❌ Authentication failed")
        return
    
    # Process PDFs
    print("🚀 Starting PDF processing...")
    stats = processor.process_pdfs(
        drive_folder_id=CONFIG['drive_folder_id'],
        api_key=CONFIG['llama_api_key'],
        agent_name=CONFIG['llama_agent'],
        spreadsheet_id=CONFIG['spreadsheet_id'],
        sheet_range=CONFIG['sheet_range']
    )
    
    # Print final results
    print("\n" + "="*50)
    print("📊 PROCESSING COMPLETE - FINAL STATISTICS")
    print("="*50)
    print(f"Total PDFs found: {stats['total_pdfs']}")
    print(f"Successfully processed: {stats['processed_pdfs']}")
    print(f"Failed to process: {stats['failed_pdfs']}")
    print(f"Rows added to Google Sheets: {stats['rows_added']}")
    print("="*50)
    
    if stats['failed_pdfs'] > 0:
        print("❌ Some PDFs failed to process. Check the log file for details.")
    elif stats['processed_pdfs'] > 0:
        print("✅ All PDFs processed successfully!")
    else:
        print("ℹ️ No PDFs were processed.")

if __name__ == "__main__":
    main()